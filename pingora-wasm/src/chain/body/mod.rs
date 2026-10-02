// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Body and trailer filters
//!
//! The request body, response body, and response trailer filters are in this module, along with the
//! pass over the chain that the two body filters share.

mod direction;
mod held;
mod request;
mod response;
mod retry;
mod trailers;

pub(crate) use direction::BodyDirection;
use held::BodyHold;
pub(crate) use held::HeldBodies;
pub(crate) use retry::RequestBodyState;

use super::wait::CalloutWaitOutcome;
use super::{ResponseProgress, WasmCtx};
use crate::stream_state::{BodyBuffer, PluginResponse};
use bytes::Bytes;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_proxy::Session;
use proxy_wasm_host::Buffer;
use std::mem;

/// Outcome of running a chain's body callbacks on one chunk.
pub(super) enum BodyOutcome {
    /// The bytes to pass on, which are empty if a plugin is holding them back.
    Released(Bytes),
    /// The plugin at this position sent its own response.
    Respond(usize, Box<PluginResponse>),
}

/// Outcome of [WasmCtx::run_body_callbacks_from].
enum BodyCallbacksOutcome {
    /// The pass is over. Either every plugin ran, or one held its bytes or sent its own response.
    Finished(BodyOutcome),
    /// The plugin at this step paused with a callout pending.
    WaitsForCallout(usize),
}

/// Convert the output of a body pass into what the filter leaves in `body`.
///
/// Pingora treats `None` from `request_body_filter` as the end of the request body, so empty
/// output only becomes `None` at the end of the stream.
pub(super) fn filter_output(output: Bytes, end_of_stream: bool) -> Option<Bytes> {
    if output.is_empty() && end_of_stream {
        None
    } else {
        Some(output)
    }
}

impl WasmCtx {
    /// Return `true` if the body filter for `direction` has nothing to do for this request.
    ///
    /// That is the case when no plugin in the chain runs on that body, once a plugin has sent its
    /// own response, for a subrequest, and after an upgrade.
    pub(super) fn skips_body<DS: DownstreamSession>(
        &self,
        session: &Session<DS>,
        direction: BodyDirection,
    ) -> bool {
        let runs = match direction {
            BodyDirection::Request => self.chain.phases.request_body,
            BodyDirection::Response => self.chain.phases.response_body,
        };
        !runs
            || self.response_progress == ResponseProgress::FromPlugin
            || session.subrequest_ctx.is_some()
            || session.was_upgraded()
    }

    /// Run the body callback of each plugin on `chunk`, waiting for callouts along the way.
    ///
    /// Plugins run in the order given by `direction`. When a plugin pauses with a callout pending,
    /// the pass waits for it. Once the plugin continues, the bytes it was holding are passed to
    /// the plugins after it.
    pub(super) async fn run_body_callbacks<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        direction: BodyDirection,
        chunk: Bytes,
        end_of_stream: bool,
    ) -> Result<BodyOutcome> {
        let count = self.records.len();
        let mut first_step = 0;
        let mut current = chunk;
        loop {
            let outcome = self.run_body_callbacks_from(
                session,
                direction,
                first_step,
                current,
                end_of_stream,
            )?;
            let step = match outcome {
                BodyCallbacksOutcome::Finished(outcome) => return Ok(outcome),
                BodyCallbacksOutcome::WaitsForCallout(step) => step,
            };
            let position = direction.position_at_step(step, count);
            let phase = direction.paused_phase();
            let outcome = self.wait_for_callouts(session, position, phase).await?;
            let released_by_plugin = match outcome {
                CalloutWaitOutcome::Continued | CalloutWaitOutcome::PluginSkipped => true,
                CalloutWaitOutcome::StillPaused => {
                    let held = self.hold_body_or_skip_plugin(direction, position, end_of_stream)?;
                    held == BodyHold::EndedBySkip
                }
                CalloutWaitOutcome::Respond(response) => {
                    return Ok(BodyOutcome::Respond(position, response))
                }
            };
            if !released_by_plugin {
                return Ok(BodyOutcome::Released(Bytes::new()));
            }
            current = self.prepend_held_bytes(direction, position, Bytes::new());
            first_step = step + 1;
        }
    }

    /// Run the body callbacks on `chunk`, starting at `first_step` of the pass.
    ///
    /// Each plugin gets the output of the one before it, preceded by any bytes it was already
    /// holding. The pass stops early when a plugin pauses, which leaves its bytes held, when a
    /// plugin sends its own response, and when there is nothing left to pass on before the end of
    /// the stream.
    ///
    /// A skipped plugin is not run. The bytes held for it, including what it was given in a call
    /// that failed, are passed on to the next plugin as they are. If a failing plugin is not
    /// skipped, those bytes stay held and the error is returned.
    fn run_body_callbacks_from<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        direction: BodyDirection,
        first_step: usize,
        chunk: Bytes,
        end_of_stream: bool,
    ) -> Result<BodyCallbacksOutcome> {
        let runtime = self.chain.runtime.clone();
        let count = self.records.len();
        let mut current = chunk;
        for step in first_step..count {
            let position = direction.position_at_step(step, count);
            let Some(record) = self.records[position] else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            if !direction.runs(&pool.phases) {
                continue;
            }
            if self.is_skipped(position) {
                current = self.prepend_held_bytes(direction, position, current);
                continue;
            }
            if current.is_empty() && !end_of_stream {
                break;
            }
            let callback = direction.callback();
            let Some(mut locked) =
                self.lock_slot_or_skip_plugin(pool, position, &record, callback)?
            else {
                current = self.prepend_held_bytes(direction, position, current);
                continue;
            };
            let loaded = locked.loaded()?;
            let held = self.held.take(direction, position);
            let buffer = BodyBuffer::new(held, mem::take(&mut current));
            let size = u32::try_from(buffer.len()).unwrap_or(u32::MAX);
            self.stream().body_buffer = buffer;
            self.request_in(session.req_header_mut());
            let action = self.run_for_context(loaded, record.context, |scope| match direction {
                BodyDirection::Request => {
                    scope.on_request_body(record.context, size, end_of_stream)
                }
                BodyDirection::Response => {
                    scope.on_response_body(record.context, size, end_of_stream)
                }
            });
            self.request_out(position, session.req_header_mut());
            let buffer = mem::take(&mut self.stream().body_buffer);
            if buffer.was_written_by_guest() {
                self.record_body_change(direction, position);
            }
            let sent = self.stream().plugin_response.take();
            let action = match action {
                Ok(action) => action,
                Err(e) => {
                    self.held.put(direction, position, buffer.into_vec());
                    self.guest_call_failed(position, locked, direction.callback(), e)?;
                    current = self.prepend_held_bytes(direction, position, Bytes::new());
                    continue;
                }
            };
            drop(locked);
            let paused =
                sent.is_none() && self.plugin_stays_paused(action, direction.stream_type());
            self.start_callouts(position, paused);
            if let Some(response) = sent {
                self.held.put(direction, position, buffer.into_vec());
                let outcome = BodyOutcome::Respond(position, Box::new(response));
                return Ok(BodyCallbacksOutcome::Finished(outcome));
            }
            if !paused {
                current = buffer.into_bytes();
                continue;
            }
            self.held.put(direction, position, buffer.into_vec());
            if self.waits_for_callout(position) {
                return Ok(BodyCallbacksOutcome::WaitsForCallout(step));
            }
            match self.hold_body_or_skip_plugin(direction, position, end_of_stream)? {
                BodyHold::Continues => break,
                BodyHold::EndedBySkip => {
                    current = self.prepend_held_bytes(direction, position, Bytes::new());
                }
            }
        }
        let outcome = BodyOutcome::Released(current);
        Ok(BodyCallbacksOutcome::Finished(outcome))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        body_chunk, body_plugin, start_request, Wat, GET, HOLD, HOLD_THEN_TRAP, MARK_AND_HOLD,
        MARK_A_REQUEST, MARK_A_RESPONSE, MARK_B_REQUEST, MARK_B_RESPONSE, PAUSE, POST, UPGRADE,
    };
    use crate::{FailPolicy, WasmPluginConf, ERR_PLUGIN_FAILED};
    use crate::{ERR_REQUEST_BODY_TOO_LARGE, ERR_RESPONSE_BODY_TOO_LARGE};
    use pingora_http::ResponseHeader;

    fn both(request: &'static str, response: &'static str) -> Wat {
        Wat {
            request_body: Some(request),
            response_body: Some(response),
            ..Wat::default()
        }
    }

    fn hold_with_a_limit_of_4() -> Vec<WasmPluginConf> {
        let mut conf = body_plugin("a", both(HOLD, HOLD));
        conf.request_body_limit = 4;
        conf.response_body_limit = 4;
        vec![conf]
    }

    #[tokio::test]
    async fn held_chunk_leaves_empty_bytes() {
        let plugins = vec![body_plugin("a", Wat::request_body(HOLD))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        let mut body = body_chunk("abc");

        let result = ctx
            .request_body_filter(&mut session, &mut body, false)
            .await;

        assert!(result.is_ok());
        assert_eq!(body, Some(Bytes::new()));
    }

    #[tokio::test]
    async fn request_body_runs_plugins_in_chain_order() {
        let plugins = vec![
            body_plugin("a", Wat::request_body(MARK_A_REQUEST)),
            body_plugin("b", Wat::request_body(MARK_B_REQUEST)),
        ];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        let mut body = body_chunk("x");

        ctx.request_body_filter(&mut session, &mut body, false)
            .await
            .unwrap();

        assert_eq!(body, body_chunk("bax"));
    }

    #[tokio::test]
    async fn response_body_runs_plugins_in_reverse_order() {
        let plugins = vec![
            body_plugin("a", Wat::response_body(MARK_A_RESPONSE)),
            body_plugin("b", Wat::response_body(MARK_B_RESPONSE)),
        ];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        let mut body = body_chunk("x");

        ctx.response_body_filter(&mut session, &mut body, false)
            .await
            .unwrap();

        assert_eq!(body, body_chunk("abx"));
    }

    #[tokio::test]
    async fn next_plugin_runs_once_held_bytes_are_released() {
        let plugins = vec![
            body_plugin("hold", Wat::request_body(HOLD)),
            body_plugin("mark", Wat::request_body(MARK_B_REQUEST)),
        ];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        let mut first = body_chunk("x");
        let mut last = body_chunk("y");
        ctx.request_body_filter(&mut session, &mut first, false)
            .await
            .unwrap();

        ctx.request_body_filter(&mut session, &mut last, true)
            .await
            .unwrap();

        assert_eq!(first, Some(Bytes::new()));
        assert_eq!(last, body_chunk("bxy"));
    }

    #[tokio::test]
    async fn empty_chunk_runs_no_plugin() {
        let plugins = vec![body_plugin("a", Wat::request_body(MARK_AND_HOLD))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        let mut chunks = [body_chunk("x"), body_chunk(""), body_chunk("y")];

        for (index, body) in chunks.iter_mut().enumerate() {
            ctx.request_body_filter(&mut session, body, index == 2)
                .await
                .unwrap();
        }

        assert_eq!(chunks, [body_chunk(""), body_chunk(""), body_chunk("aaxy")]);
    }

    #[tokio::test]
    async fn plugin_with_request_body_disabled_does_not_run() {
        let mut conf = body_plugin("a", Wat::request_body(MARK_A_REQUEST));
        conf.request_body = false;
        let (_runtime, mut ctx, mut session, _client) = start_request(vec![conf], POST).await;
        let mut body = body_chunk("x");

        ctx.request_body_filter(&mut session, &mut body, true)
            .await
            .unwrap();

        assert_eq!(body, body_chunk("x"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn chain_with_body_phases_disabled_takes_no_slot_lock() {
        let mut conf = body_plugin("a", both(MARK_A_REQUEST, MARK_A_RESPONSE));
        conf.request_body = false;
        conf.response_body = false;
        let (runtime, mut ctx, mut session, _client) = start_request(vec![conf], POST).await;
        let (lock, unlock) = std::sync::mpsc::channel::<()>();
        let (locked, is_locked) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let _slot = runtime.inner.pools[0].lock_slot(0);
            locked.send(()).unwrap();
            let _ = unlock.recv();
        });
        is_locked.recv().unwrap();
        let mut sent = body_chunk("x");
        let mut returned = body_chunk("y");

        ctx.request_body_filter(&mut session, &mut sent, true)
            .await
            .unwrap();
        ctx.response_body_filter(&mut session, &mut returned, true)
            .await
            .unwrap();

        drop(lock);
        holder.join().unwrap();
        assert_eq!([sent, returned], [body_chunk("x"), body_chunk("y")]);
    }

    #[tokio::test]
    async fn pause_at_end_of_stream_fails() {
        let plugins = vec![body_plugin("a", Wat::response_body(PAUSE))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;

        let err = ctx
            .response_body_filter(&mut session, &mut body_chunk("x"), true)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("paused on the last body chunk"));
    }

    #[tokio::test]
    async fn held_body_over_limit_fails() {
        let cases = [
            (
                BodyDirection::Request,
                ERR_REQUEST_BODY_TOO_LARGE,
                "wasm plugin a: 6 held request body bytes exceed request_body_limit 4",
            ),
            (
                BodyDirection::Response,
                ERR_RESPONSE_BODY_TOO_LARGE,
                "wasm plugin a: 6 held response body bytes exceed response_body_limit 4",
            ),
        ];

        let policies = [FailPolicy::Closed, FailPolicy::Open];
        let cases = cases
            .iter()
            .flat_map(|case| policies.map(|policy| (case, policy)));

        for ((direction, error_type, message), policy) in cases {
            let mut plugins = hold_with_a_limit_of_4();
            plugins[0].fail_policy = policy;
            let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
            let mut body = body_chunk("abcdef");

            let result = match direction {
                BodyDirection::Request => {
                    ctx.request_body_filter(&mut session, &mut body, false)
                        .await
                }
                BodyDirection::Response => {
                    ctx.response_body_filter(&mut session, &mut body, false)
                        .await
                }
            };

            let err = result.unwrap_err();
            assert_eq!(err.etype(), error_type, "{policy}");
            assert!(err.to_string().contains(message), "{err}");
            assert_eq!(ctx.skipped_plugins().count(), 0, "{policy}");
        }
    }

    #[tokio::test]
    async fn limit_does_not_apply_to_released_body() {
        let (_runtime, mut ctx, mut session, _client) =
            start_request(hold_with_a_limit_of_4(), POST).await;
        let mut last = body_chunk("cdef");
        ctx.request_body_filter(&mut session, &mut body_chunk("ab"), false)
            .await
            .unwrap();

        ctx.request_body_filter(&mut session, &mut last, true)
            .await
            .unwrap();

        assert_eq!(last, body_chunk("abcdef"));
    }

    #[tokio::test]
    async fn none_ends_request_body() {
        let plugins = vec![body_plugin("a", Wat::request_body(HOLD))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        ctx.request_body_filter(&mut session, &mut body_chunk("held"), false)
            .await
            .unwrap();
        let mut body = None;

        ctx.request_body_filter(&mut session, &mut body, false)
            .await
            .unwrap();

        assert_eq!(body, body_chunk("held"));
    }

    #[tokio::test]
    async fn none_does_not_end_response_body() {
        let plugins = vec![body_plugin("a", Wat::response_body(HOLD))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        ctx.response_body_filter(&mut session, &mut body_chunk("held"), false)
            .await
            .unwrap();
        let mut body = None;

        ctx.response_body_filter(&mut session, &mut body, false)
            .await
            .unwrap();

        assert_eq!(body, None);
        assert_eq!(ctx.held.take(BodyDirection::Response, 0), b"held");
    }

    #[tokio::test]
    async fn request_without_body_runs_no_plugin() {
        let plugins = vec![body_plugin("a", Wat::request_body(MARK_A_REQUEST))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, GET).await;
        let mut body = None;

        ctx.request_body_filter(&mut session, &mut body, true)
            .await
            .unwrap();

        assert_eq!(body, None);
    }

    #[tokio::test]
    async fn upgraded_connection_runs_no_plugin() {
        let plugins = vec![body_plugin("a", both(MARK_A_REQUEST, MARK_A_RESPONSE))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, UPGRADE).await;
        ctx.request_body.expect_body();
        let mut switching = ResponseHeader::build(101, None).unwrap();
        switching.insert_header("connection", "upgrade").unwrap();
        switching.insert_header("upgrade", "websocket").unwrap();
        session
            .write_response_header(Box::new(switching), false)
            .await
            .unwrap();
        let mut sent = body_chunk("frame");
        let mut returned = body_chunk("frame");

        ctx.request_body_filter(&mut session, &mut sent, false)
            .await
            .unwrap();
        ctx.response_body_filter(&mut session, &mut returned, false)
            .await
            .unwrap();

        assert!(session.was_upgraded());
        assert_eq!([sent, returned], [body_chunk("frame"), body_chunk("frame")]);
    }

    #[tokio::test]
    async fn trap_rebuilds_guest_and_keeps_held_bytes() {
        let plugins = vec![body_plugin("a", Wat::request_body(HOLD_THEN_TRAP))];
        let (runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        ctx.request_body_filter(&mut session, &mut body_chunk("ab"), false)
            .await
            .unwrap();

        let err = ctx
            .request_body_filter(&mut session, &mut body_chunk("cd"), true)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("proxy_on_request_body failed"));
        assert_eq!(ctx.held.take(BodyDirection::Request, 0), b"abcd");
        assert_eq!(session.req_header().raw_path(), b"/original");
        let slot = runtime.inner.pools[0].lock_slot(0);
        assert!(slot.as_ref().unwrap().guest.is_serving());
    }

    #[tokio::test]
    async fn body_fails_after_guest_is_replaced() {
        let plugins = vec![body_plugin("a", Wat::request_body(HOLD))];
        let (runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        runtime.inner.pools[0].replace_slot(0);

        let err = ctx
            .request_body_filter(&mut session, &mut body_chunk("ab"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        let message = "guest in slot 0 lost before proxy_on_request_body";
        assert!(err.to_string().contains(message), "{err}");
    }
}
