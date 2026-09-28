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

use crate::chain::slot::LockedSlot;
use crate::chain::wait::{CalloutWaitOutcome, PausedPhase};
use crate::chain::{ResponseProgress, WasmCtx};
use crate::stream::ResponseTrailers;
use bytes::{Bytes, BytesMut};
use log::{error, warn};
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::StreamType;
use proxy_wasm_host::HeaderMap;
use std::mem;

const PAUSED_THE_TRAILERS: &str = "paused the trailers";

impl WasmCtx {
    /// Run `proxy_on_response_trailers` of each plugin, in reverse chain order.
    ///
    /// Call it from `response_trailer_filter` with the arguments of that filter, and return what it
    /// returns. Only plugins with [response_trailers](crate::WasmPluginConf::response_trailers)
    /// turned on run. Plugins can read and change the trailers, and can read the request headers.
    ///
    /// Call it when you run plugins on response bodies, too. A response with trailers ends with the
    /// trailers and not with a last body chunk, so a plugin that paused the body still holds bytes
    /// here. This phase returns those bytes, and Pingora writes them to the downstream in place of
    /// the trailers.
    ///
    /// A plugin can pause the trailers while it waits for a callout, and this phase waits with
    /// it.
    ///
    /// # Errors
    ///
    /// An error of type [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) when a plugin traps, fails,
    /// or sends its own response, or when a plugin pauses and has no callout to wait for.
    /// Pingora logs an error from `response_trailer_filter` and sends the trailers, so end the
    /// response in your filter if the trailers must not go out.
    ///
    /// When a plugin holds body bytes, this phase logs the failure and returns the bytes, so that
    /// the downstream receives the whole body.
    pub async fn response_trailer_filter<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        trailers: &mut http::HeaderMap,
    ) -> Result<Option<Bytes>> {
        if session.subrequest_ctx.is_some()
            || self.response_progress == ResponseProgress::FromPlugin
        {
            return Ok(None);
        }
        let mut passed = self.refuse_after_cancelled_wait();
        if passed.is_ok() && self.chain.phases.response_trailers {
            self.chain.runtime.start_ticker()?;
            passed = self.run_trailer_callbacks(session, trailers).await;
        }
        match (passed, self.release_held()) {
            (Err(e), None) => Err(e),
            (Err(e), held) => {
                error!("{e}");
                Ok(held)
            }
            (Ok(()), held) => Ok(held),
        }
    }

    /// Run the trailer callback of each plugin, in reverse chain order, and wait for the
    /// callouts of a plugin that pauses to wait for them.
    async fn run_trailer_callbacks<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        trailers: &mut http::HeaderMap,
    ) -> Result<()> {
        let mut remaining = self.records.len();
        while let Some(position) = self.run_trailer_callbacks_of(session, trailers, remaining)? {
            let phase = PausedPhase::ResponseTrailers(&mut *trailers);
            match self.wait_for_callouts(session, position, phase).await? {
                CalloutWaitOutcome::Continued => remaining = position,
                CalloutWaitOutcome::StillPaused => {
                    return Err(self.plugin_error(position, PAUSED_THE_TRAILERS))
                }
                CalloutWaitOutcome::Respond(_) => return Err(self.late_response_error(position)),
            }
        }
        Ok(())
    }

    /// Run the trailer callback of the first `remaining` plugins of the chain, in reverse
    /// order.
    ///
    /// Return the position of a plugin that paused and has a callout to wait for.
    fn run_trailer_callbacks_of<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        trailers: &mut http::HeaderMap,
        remaining: usize,
    ) -> Result<Option<usize>> {
        let runtime = self.chain.runtime.clone();
        for position in (0..remaining).rev() {
            let Some(record) = self.records[position] else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            if !pool.phases.trailers {
                continue;
            }
            let mut locked = LockedSlot::of_request(pool, &record)?;
            let loaded = locked.loaded()?;
            let map = ResponseTrailers::new(mem::take(trailers));
            let count = u32::try_from(map.len()).unwrap_or(u32::MAX);
            self.stream().trailers = Some(map);
            self.request_in(session.req_header_mut());
            let action = self.run_for_context(loaded, record.context, |scope| {
                scope.on_response_trailers(record.context, count)
            });
            self.request_out(session.req_header_mut());
            if let Some(map) = self.stream().trailers.take() {
                *trailers = map.trailers;
            }
            let sent = self.stream().plugin_response.take();
            let action = match action {
                Ok(action) => action,
                Err(e) => return Err(locked.guest_failure("failed in on_response_trailers", e)),
            };
            drop(locked);
            let paused =
                sent.is_none() && self.plugin_stays_paused(action, StreamType::HttpResponse);
            self.start_callouts(position, paused);
            if sent.is_some() {
                return Err(self.late_response_error(position));
            }
            if !paused {
                continue;
            }
            if self.waits_for_callout(position) {
                return Ok(Some(position));
            }
            return Err(self.plugin_error(position, PAUSED_THE_TRAILERS));
        }
        Ok(None)
    }

    /// Take the response body bytes that the plugins hold, in the order of the stream.
    ///
    /// The plugins run in reverse chain order, so the first plugin of the chain holds the earliest
    /// bytes.
    fn release_held(&mut self) -> Option<Bytes> {
        let held = self.held.take_response();
        let size: usize = held.iter().map(Vec::len).sum();
        if size == 0 {
            return None;
        }
        let mut all = BytesMut::with_capacity(size);
        for (position, bytes) in held.iter().enumerate() {
            if bytes.is_empty() {
                continue;
            }
            warn!(
                "wasm plugin {} held {} body bytes, sent in place of the response trailers",
                self.pool_at(position).name,
                bytes.len()
            );
            all.extend_from_slice(bytes);
        }
        Some(all.freeze())
    }
}

#[cfg(test)]
mod tests {
    use crate::chain::body::BodyDirection;
    use crate::test_support::{
        body_chunk, body_plugin, start_request, Wat, HOLD, PAUSE, POST, SET_TRAILER, TEAPOT, TRAP,
    };
    use crate::ERR_PLUGIN_FAILED;

    fn trailers() -> http::HeaderMap {
        let mut trailers = http::HeaderMap::new();
        trailers.insert("grpc-status", "0".parse().unwrap());
        trailers
    }

    #[tokio::test]
    async fn a_plugin_changes_a_trailer() {
        let plugins = vec![body_plugin("a", Wat::response_trailers(SET_TRAILER))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        let mut trailers = trailers();

        let body = ctx
            .response_trailer_filter(&mut session, &mut trailers)
            .await
            .unwrap();

        assert_eq!(body, None);
        assert_eq!(trailers["x-trailer"], "set");
        assert_eq!(trailers["grpc-status"], "0");
    }

    #[tokio::test]
    async fn a_pause_a_plugin_response_or_a_trap_fails() {
        let cases = [
            (PAUSE, "paused the trailers"),
            (TEAPOT, "sent a response after the response header"),
            (TRAP, "failed in on_response_trailers"),
        ];

        for (body, message) in cases {
            let plugins = vec![body_plugin("a", Wat::response_trailers(body))];
            let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
            let mut trailers = trailers();

            let err = ctx
                .response_trailer_filter(&mut session, &mut trailers)
                .await
                .unwrap_err();

            assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
            assert!(err.to_string().contains(message), "{err}");
            assert_eq!(trailers["grpc-status"], "0");
        }
    }

    #[tokio::test]
    async fn held_bytes_are_released_once_in_place_of_the_trailers() {
        let plugins = vec![body_plugin("a", Wat::response_body(HOLD))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        ctx.response_body_filter(&mut session, &mut body_chunk("held"), false)
            .await
            .unwrap();

        let first = ctx
            .response_trailer_filter(&mut session, &mut trailers())
            .await
            .unwrap();

        let second = ctx
            .response_trailer_filter(&mut session, &mut trailers())
            .await
            .unwrap();
        assert_eq!(first, body_chunk("held"));
        assert_eq!(second, None);
    }

    #[tokio::test]
    async fn held_bytes_are_joined_in_the_order_of_the_stream() {
        let plugins = vec![
            body_plugin("a", Wat::default()),
            body_plugin("b", Wat::default()),
        ];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        ctx.held.put(BodyDirection::Response, 1, b"late".to_vec());
        ctx.held.put(BodyDirection::Response, 0, b"early ".to_vec());

        let body = ctx
            .response_trailer_filter(&mut session, &mut trailers())
            .await
            .unwrap();

        assert_eq!(body, body_chunk("early late"));
    }

    #[tokio::test]
    async fn a_failure_releases_the_held_bytes_in_place_of_the_trailers() {
        let plugins = vec![
            body_plugin("hold", Wat::response_body(HOLD)),
            body_plugin("trap", Wat::response_trailers(TRAP)),
        ];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        ctx.response_body_filter(&mut session, &mut body_chunk("held"), false)
            .await
            .unwrap();

        let released = ctx
            .response_trailer_filter(&mut session, &mut trailers())
            .await;

        assert_eq!(released.unwrap(), body_chunk("held"));
    }
}
