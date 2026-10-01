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

//! The body phases and the trailer phase, and the pass over the plugins that the body phases use.

mod direction;
mod held;
mod request;
mod response;
mod retry;
mod trailers;

pub(crate) use direction::BodyDirection;
pub(crate) use held::HeldBodies;
pub(crate) use retry::RequestBodyState;

use super::slot::LockedSlot;
use super::wait::CalloutWaitOutcome;
use super::{ResponseProgress, WasmCtx};
use crate::stream_state::{BodyBuffer, PluginResponse};
use bytes::Bytes;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::{Error, Result};
use pingora_proxy::Session;
use proxy_wasm_host::Buffer;
use std::mem;

/// The result of the body callbacks of a chain on one chunk.
pub(super) enum BodyOutcome {
    /// The bytes that the last plugin returned.
    Released(Bytes),
    /// The plugin at this position sent its own response.
    Respond(usize, Box<PluginResponse>),
}

/// The result of [WasmCtx::run_body_callbacks_from].
enum BodyCallbacksOutcome {
    /// Every plugin ran, or a plugin held its bytes or sent its own response.
    Finished(BodyOutcome),
    /// The plugin at this step paused and has a callout to wait for.
    WaitsForCallout(usize),
}

/// Return what a body filter leaves in `body` for Pingora.
///
/// Pingora ends the body on `None`, so an empty chunk stays `Some` until the end of the stream.
pub(super) fn filter_output(output: Bytes, end_of_stream: bool) -> Option<Bytes> {
    if output.is_empty() && end_of_stream {
        None
    } else {
        Some(output)
    }
}

impl WasmCtx {
    /// Return `true` when a body phase does not run for this request.
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

    /// Run the body callback of each plugin on `chunk`, in the order of `direction`, and wait
    /// for the callouts of a plugin that pauses to wait for them.
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
            match self.wait_for_callouts(session, position, phase).await? {
                CalloutWaitOutcome::Continued => {
                    current = Bytes::from(self.held.take(direction, position));
                    first_step = step + 1;
                }
                CalloutWaitOutcome::StillPaused => {
                    self.check_body_can_be_held(direction, position, end_of_stream)?;
                    return Ok(BodyOutcome::Released(Bytes::new()));
                }
                CalloutWaitOutcome::Respond(response) => {
                    return Ok(BodyOutcome::Respond(position, response))
                }
            }
        }
    }

    /// Return an error when the plugin at `position` cannot hold the bytes that it paused on.
    fn check_body_can_be_held(
        &self,
        direction: BodyDirection,
        position: usize,
        end_of_stream: bool,
    ) -> Result<()> {
        if end_of_stream {
            return Err(self.plugin_error(position, "paused a body at its end"));
        }
        let pool = self.pool_at(position);
        let size = self.held.len(direction, position);
        if size > direction.limit(&pool.phases) {
            return Error::e_explain(
                direction.too_large(),
                format!(
                    "wasm plugin {} holds {size} body bytes, more than its limit",
                    pool.name
                ),
            );
        }
        Ok(())
    }

    /// Run the body callback of each plugin on `chunk`, from `first_step` of the pass.
    ///
    /// Each plugin receives what the plugin before it returned. When a plugin pauses, it holds the
    /// bytes, and the plugins after it do not run.
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
            if current.is_empty() && !end_of_stream {
                break;
            }
            let mut locked = LockedSlot::of_request(pool, &record)?;
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
            self.request_out(session.req_header_mut());
            let buffer = mem::take(&mut self.stream().body_buffer);
            let sent = self.stream().plugin_response.take();
            let action = match action {
                Ok(action) => action,
                Err(e) => {
                    self.held.put(direction, position, buffer.into_vec());
                    return Err(locked.guest_failure(direction.failure(), e));
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
            self.check_body_can_be_held(direction, position, end_of_stream)?;
            break;
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
    use crate::{WasmPluginConf, ERR_PLUGIN_FAILED};
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
    async fn a_held_chunk_leaves_empty_bytes() {
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
    async fn the_request_body_runs_the_plugins_in_chain_order() {
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
    async fn the_response_body_runs_the_plugins_in_reverse_order() {
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
    async fn a_plugin_that_holds_gives_the_next_plugin_nothing_until_it_continues() {
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
    async fn a_chunk_of_zero_bytes_runs_no_plugin() {
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
    async fn a_plugin_with_its_body_setting_off_does_not_run() {
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
    async fn a_chain_with_its_body_settings_off_takes_no_slot_lock() {
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
    async fn a_pause_at_the_end_of_the_stream_fails() {
        let plugins = vec![body_plugin("a", Wat::response_body(PAUSE))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;

        let err = ctx
            .response_body_filter(&mut session, &mut body_chunk("x"), true)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("paused a body at its end"));
    }

    #[tokio::test]
    async fn a_held_request_body_over_its_limit_fails() {
        let (_runtime, mut ctx, mut session, _client) =
            start_request(hold_with_a_limit_of_4(), POST).await;

        let err = ctx
            .request_body_filter(&mut session, &mut body_chunk("abcdef"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_REQUEST_BODY_TOO_LARGE);
    }

    #[tokio::test]
    async fn a_held_response_body_over_its_limit_fails() {
        let (_runtime, mut ctx, mut session, _client) =
            start_request(hold_with_a_limit_of_4(), POST).await;

        let err = ctx
            .response_body_filter(&mut session, &mut body_chunk("abcdef"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_RESPONSE_BODY_TOO_LARGE);
    }

    #[tokio::test]
    async fn a_plugin_releases_a_body_over_its_limit() {
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
    async fn none_ends_a_request_body() {
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
    async fn none_does_not_end_a_response_body() {
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
    async fn a_request_with_no_body_runs_no_plugin() {
        let plugins = vec![body_plugin("a", Wat::request_body(MARK_A_REQUEST))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, GET).await;
        let mut body = None;

        ctx.request_body_filter(&mut session, &mut body, true)
            .await
            .unwrap();

        assert_eq!(body, None);
    }

    #[tokio::test]
    async fn an_upgraded_connection_runs_no_plugin() {
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
    async fn a_trap_rebuilds_the_guest_and_keeps_the_held_bytes() {
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
        assert!(err.to_string().contains("failed in on_request_body"));
        assert_eq!(ctx.held.take(BodyDirection::Request, 0), b"abcd");
        assert_eq!(session.req_header().raw_path(), b"/original");
        let slot = runtime.inner.pools[0].lock_slot(0);
        assert!(slot.as_ref().unwrap().guest.is_serving());
    }

    #[tokio::test]
    async fn a_body_on_a_replaced_guest_fails() {
        let plugins = vec![body_plugin("a", Wat::request_body(HOLD))];
        let (runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        runtime.inner.pools[0].replace_slot(0);

        let err = ctx
            .request_body_filter(&mut session, &mut body_chunk("ab"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("lost the guest of this request"));
    }
}
