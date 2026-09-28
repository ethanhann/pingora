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

//! The pass over the plugins that both body phases use, and the body bytes that plugins hold.

use super::failure::Locked;
use super::{Exchange, WasmCtx};
use crate::plugin_unavailable;
use crate::runtime::pool::PhaseConf;
use crate::stream::{BodyBuffer, PluginResponse};
use crate::{ERR_REQUEST_BODY_TOO_LARGE, ERR_RESPONSE_BODY_TOO_LARGE};
use bytes::Bytes;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::{Error, ErrorType, Result};
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::Action;
use proxy_wasm_host::Buffer;
use std::mem;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Direction {
    Request,
    Response,
}

impl Direction {
    fn runs(self, conf: &PhaseConf) -> bool {
        match self {
            Direction::Request => conf.request,
            Direction::Response => conf.response,
        }
    }

    fn limit(self, conf: &PhaseConf) -> usize {
        match self {
            Direction::Request => conf.request_limit,
            Direction::Response => conf.response_limit,
        }
    }

    fn too_large(self) -> ErrorType {
        match self {
            Direction::Request => ERR_REQUEST_BODY_TOO_LARGE,
            Direction::Response => ERR_RESPONSE_BODY_TOO_LARGE,
        }
    }

    fn failure(self) -> &'static str {
        match self {
            Direction::Request => "failed in on_request_body",
            Direction::Response => "failed in on_response_body",
        }
    }
}

/// The body bytes that the plugins hold, by position in the chain.
///
/// A list stays empty until a plugin holds bytes.
#[derive(Debug, Default)]
pub(super) struct Held {
    request: Vec<Vec<u8>>,
    response: Vec<Vec<u8>>,
}

impl Held {
    fn list(&mut self, direction: Direction) -> &mut Vec<Vec<u8>> {
        match direction {
            Direction::Request => &mut self.request,
            Direction::Response => &mut self.response,
        }
    }

    pub(super) fn take(&mut self, direction: Direction, position: usize) -> Vec<u8> {
        match self.list(direction).get_mut(position) {
            Some(bytes) => mem::take(bytes),
            None => Vec::new(),
        }
    }

    pub(super) fn put(&mut self, direction: Direction, position: usize, bytes: Vec<u8>) {
        let list = self.list(direction);
        if list.len() <= position {
            if bytes.is_empty() {
                return;
            }
            list.resize_with(position + 1, Vec::new);
        }
        list[position] = bytes;
    }

    /// Take the response bytes that the plugins hold, in chain order.
    pub(super) fn take_response(&mut self) -> Vec<Vec<u8>> {
        mem::take(&mut self.response)
    }

    pub(super) fn request_len(&self) -> usize {
        self.request.iter().map(Vec::len).sum()
    }

    pub(super) fn response_len(&self) -> usize {
        self.response.iter().map(Vec::len).sum()
    }
}

/// The result of [WasmCtx::body_pass].
pub(super) enum BodyOutcome {
    /// The bytes that the last plugin returned.
    Released(Bytes),
    /// The plugin at this position sent its own response.
    Respond(usize, Box<PluginResponse>),
}

/// Return what a body filter leaves in `body` for Pingora.
///
/// Pingora ends the body on `None`, so an empty chunk stays `Some` until the end of the stream.
pub(super) fn released(output: Bytes, end_of_stream: bool) -> Option<Bytes> {
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
        direction: Direction,
    ) -> bool {
        let runs = match direction {
            Direction::Request => self.chain.phases.request_body,
            Direction::Response => self.chain.phases.response_body,
        };
        !runs
            || self.exchange == Exchange::Responded
            || session.subrequest_ctx.is_some()
            || session.was_upgraded()
    }

    /// Run the body callback of each plugin on `chunk`, in the order of `direction`.
    ///
    /// Each plugin receives what the plugin before it returned. When a plugin pauses, it holds the
    /// bytes, and the plugins after it do not run.
    pub(super) fn body_pass<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        direction: Direction,
        chunk: Bytes,
        end_of_stream: bool,
    ) -> Result<BodyOutcome> {
        let runtime = self.chain.runtime.clone();
        let count = self.records.len();
        let mut current = chunk;
        for step in 0..count {
            let position = match direction {
                Direction::Request => step,
                Direction::Response => count - 1 - step,
            };
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
            let mut locked = Locked::of(pool, &record)?;
            let guest = &mut locked.loaded()?.guest;
            let held = self.held.take(direction, position);
            let buffer = BodyBuffer::new(held, mem::take(&mut current));
            let size = u32::try_from(buffer.len()).unwrap_or(u32::MAX);
            self.stream().body_buffer = buffer;
            self.request_in(session.req_header_mut());
            let action = self.run(guest, |scope| match direction {
                Direction::Request => scope.on_request_body(record.context, size, end_of_stream),
                Direction::Response => scope.on_response_body(record.context, size, end_of_stream),
            });
            self.request_out(session.req_header_mut());
            let buffer = mem::take(&mut self.stream().body_buffer);
            let sent = self.stream().plugin_response.take();
            let action = match action {
                Ok(action) => action,
                Err(e) => {
                    self.held.put(direction, position, buffer.into_vec());
                    return Err(locked.failed(direction.failure(), e));
                }
            };
            if let Some(response) = sent {
                self.held.put(direction, position, buffer.into_vec());
                return Ok(BodyOutcome::Respond(position, Box::new(response)));
            }
            if action == Action::Continue {
                current = buffer.into_bytes();
                continue;
            }
            let held = buffer.into_vec();
            let size = held.len();
            self.held.put(direction, position, held);
            if end_of_stream {
                return Err(plugin_unavailable(&pool.name, "paused a body at its end"));
            }
            if size > direction.limit(&pool.phases) {
                return Error::e_explain(
                    direction.too_large(),
                    format!(
                        "wasm plugin {} holds {size} body bytes, more than its limit",
                        pool.name
                    ),
                );
            }
            break;
        }
        Ok(BodyOutcome::Released(current))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        body_plugin, chunk, started, Wat, GET, HOLD, HOLD_THEN_TRAP, MARK_AND_HOLD, MARK_A_REQUEST,
        MARK_A_RESPONSE, MARK_B_REQUEST, MARK_B_RESPONSE, PAUSE, POST, UPGRADE,
    };
    use crate::{WasmPluginConf, ERR_PLUGIN_FAILED};
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
        let (_runtime, mut ctx, mut session, _client) = started(plugins, POST).await;
        let mut body = chunk("abc");

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
        let (_runtime, mut ctx, mut session, _client) = started(plugins, POST).await;
        let mut body = chunk("x");

        ctx.request_body_filter(&mut session, &mut body, false)
            .await
            .unwrap();

        assert_eq!(body, chunk("bax"));
    }

    #[tokio::test]
    async fn the_response_body_runs_the_plugins_in_reverse_order() {
        let plugins = vec![
            body_plugin("a", Wat::response_body(MARK_A_RESPONSE)),
            body_plugin("b", Wat::response_body(MARK_B_RESPONSE)),
        ];
        let (_runtime, mut ctx, mut session, _client) = started(plugins, POST).await;
        let mut body = chunk("x");

        ctx.response_body_filter(&mut session, &mut body, false)
            .await
            .unwrap();

        assert_eq!(body, chunk("abx"));
    }

    #[tokio::test]
    async fn a_plugin_that_holds_gives_the_next_plugin_nothing_until_it_continues() {
        let plugins = vec![
            body_plugin("hold", Wat::request_body(HOLD)),
            body_plugin("mark", Wat::request_body(MARK_B_REQUEST)),
        ];
        let (_runtime, mut ctx, mut session, _client) = started(plugins, POST).await;
        let mut first = chunk("x");
        let mut last = chunk("y");
        ctx.request_body_filter(&mut session, &mut first, false)
            .await
            .unwrap();

        ctx.request_body_filter(&mut session, &mut last, true)
            .await
            .unwrap();

        assert_eq!(first, Some(Bytes::new()));
        assert_eq!(last, chunk("bxy"));
    }

    #[tokio::test]
    async fn a_chunk_of_zero_bytes_runs_no_plugin() {
        let plugins = vec![body_plugin("a", Wat::request_body(MARK_AND_HOLD))];
        let (_runtime, mut ctx, mut session, _client) = started(plugins, POST).await;
        let mut chunks = [chunk("x"), chunk(""), chunk("y")];

        for (index, body) in chunks.iter_mut().enumerate() {
            ctx.request_body_filter(&mut session, body, index == 2)
                .await
                .unwrap();
        }

        assert_eq!(chunks, [chunk(""), chunk(""), chunk("aaxy")]);
    }

    #[tokio::test]
    async fn a_plugin_with_its_body_setting_off_does_not_run() {
        let mut conf = body_plugin("a", Wat::request_body(MARK_A_REQUEST));
        conf.request_body = false;
        let (_runtime, mut ctx, mut session, _client) = started(vec![conf], POST).await;
        let mut body = chunk("x");

        ctx.request_body_filter(&mut session, &mut body, true)
            .await
            .unwrap();

        assert_eq!(body, chunk("x"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_chain_with_its_body_settings_off_takes_no_slot_lock() {
        let mut conf = body_plugin("a", both(MARK_A_REQUEST, MARK_A_RESPONSE));
        conf.request_body = false;
        conf.response_body = false;
        let (runtime, mut ctx, mut session, _client) = started(vec![conf], POST).await;
        let (lock, unlock) = std::sync::mpsc::channel::<()>();
        let (locked, is_locked) = std::sync::mpsc::channel();
        let holder = std::thread::spawn(move || {
            let _slot = runtime.inner.pools[0].lock_slot(0);
            locked.send(()).unwrap();
            let _ = unlock.recv();
        });
        is_locked.recv().unwrap();
        let mut sent = chunk("x");
        let mut returned = chunk("y");

        ctx.request_body_filter(&mut session, &mut sent, true)
            .await
            .unwrap();
        ctx.response_body_filter(&mut session, &mut returned, true)
            .await
            .unwrap();

        drop(lock);
        holder.join().unwrap();
        assert_eq!([sent, returned], [chunk("x"), chunk("y")]);
    }

    #[tokio::test]
    async fn a_pause_at_the_end_of_the_stream_fails() {
        let plugins = vec![body_plugin("a", Wat::response_body(PAUSE))];
        let (_runtime, mut ctx, mut session, _client) = started(plugins, POST).await;

        let err = ctx
            .response_body_filter(&mut session, &mut chunk("x"), true)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("paused a body at its end"));
    }

    #[tokio::test]
    async fn a_held_request_body_over_its_limit_fails() {
        let (_runtime, mut ctx, mut session, _client) =
            started(hold_with_a_limit_of_4(), POST).await;

        let err = ctx
            .request_body_filter(&mut session, &mut chunk("abcdef"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_REQUEST_BODY_TOO_LARGE);
    }

    #[tokio::test]
    async fn a_held_response_body_over_its_limit_fails() {
        let (_runtime, mut ctx, mut session, _client) =
            started(hold_with_a_limit_of_4(), POST).await;

        let err = ctx
            .response_body_filter(&mut session, &mut chunk("abcdef"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_RESPONSE_BODY_TOO_LARGE);
    }

    #[tokio::test]
    async fn a_plugin_releases_a_body_over_its_limit() {
        let (_runtime, mut ctx, mut session, _client) =
            started(hold_with_a_limit_of_4(), POST).await;
        let mut last = chunk("cdef");
        ctx.request_body_filter(&mut session, &mut chunk("ab"), false)
            .await
            .unwrap();

        ctx.request_body_filter(&mut session, &mut last, true)
            .await
            .unwrap();

        assert_eq!(last, chunk("abcdef"));
    }

    #[tokio::test]
    async fn none_ends_a_request_body() {
        let plugins = vec![body_plugin("a", Wat::request_body(HOLD))];
        let (_runtime, mut ctx, mut session, _client) = started(plugins, POST).await;
        ctx.request_body_filter(&mut session, &mut chunk("held"), false)
            .await
            .unwrap();
        let mut body = None;

        ctx.request_body_filter(&mut session, &mut body, false)
            .await
            .unwrap();

        assert_eq!(body, chunk("held"));
    }

    #[tokio::test]
    async fn none_does_not_end_a_response_body() {
        let plugins = vec![body_plugin("a", Wat::response_body(HOLD))];
        let (_runtime, mut ctx, mut session, _client) = started(plugins, POST).await;
        ctx.response_body_filter(&mut session, &mut chunk("held"), false)
            .await
            .unwrap();
        let mut body = None;

        ctx.response_body_filter(&mut session, &mut body, false)
            .await
            .unwrap();

        assert_eq!(body, None);
        assert_eq!(ctx.held.take(Direction::Response, 0), b"held");
    }

    #[tokio::test]
    async fn a_request_with_no_body_runs_no_plugin() {
        let plugins = vec![body_plugin("a", Wat::request_body(MARK_A_REQUEST))];
        let (_runtime, mut ctx, mut session, _client) = started(plugins, GET).await;
        let mut body = None;

        ctx.request_body_filter(&mut session, &mut body, true)
            .await
            .unwrap();

        assert_eq!(body, None);
    }

    #[tokio::test]
    async fn an_upgraded_connection_runs_no_plugin() {
        let plugins = vec![body_plugin("a", both(MARK_A_REQUEST, MARK_A_RESPONSE))];
        let (_runtime, mut ctx, mut session, _client) = started(plugins, UPGRADE).await;
        ctx.request_body.expect();
        let mut switching = ResponseHeader::build(101, None).unwrap();
        switching.insert_header("connection", "upgrade").unwrap();
        switching.insert_header("upgrade", "websocket").unwrap();
        session
            .write_response_header(Box::new(switching), false)
            .await
            .unwrap();
        let mut sent = chunk("frame");
        let mut returned = chunk("frame");

        ctx.request_body_filter(&mut session, &mut sent, false)
            .await
            .unwrap();
        ctx.response_body_filter(&mut session, &mut returned, false)
            .await
            .unwrap();

        assert!(session.was_upgraded());
        assert_eq!([sent, returned], [chunk("frame"), chunk("frame")]);
    }

    #[tokio::test]
    async fn a_trap_rebuilds_the_guest_and_keeps_the_held_bytes() {
        let plugins = vec![body_plugin("a", Wat::request_body(HOLD_THEN_TRAP))];
        let (runtime, mut ctx, mut session, _client) = started(plugins, POST).await;
        ctx.request_body_filter(&mut session, &mut chunk("ab"), false)
            .await
            .unwrap();

        let err = ctx
            .request_body_filter(&mut session, &mut chunk("cd"), true)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("failed in on_request_body"));
        assert_eq!(ctx.held.take(Direction::Request, 0), b"abcd");
        assert_eq!(session.req_header().raw_path(), b"/original");
        let slot = runtime.inner.pools[0].lock_slot(0);
        assert!(slot.as_ref().unwrap().guest.is_serving());
    }

    #[tokio::test]
    async fn a_body_on_a_replaced_guest_fails() {
        let plugins = vec![body_plugin("a", Wat::request_body(HOLD))];
        let (runtime, mut ctx, mut session, _client) = started(plugins, POST).await;
        runtime.inner.pools[0].replace_slot(0);

        let err = ctx
            .request_body_filter(&mut session, &mut chunk("ab"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("lost the guest of this request"));
    }
}
