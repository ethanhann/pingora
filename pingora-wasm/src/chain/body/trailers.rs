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

use crate::chain::failure::FilterFailure;
use crate::chain::wait::{CalloutWaitOutcome, PausedPhase};
use crate::chain::{ResponseProgress, WasmCtx};
use crate::stream_state::ResponseTrailers;
use bytes::{Bytes, BytesMut};
use log::{error, warn};
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::StreamType;
use proxy_wasm_host::abi::v0_2_1::Callback;
use proxy_wasm_host::HeaderMap;
use std::mem;

fn trailer_pause_failure() -> FilterFailure {
    let what = "paused on response trailers with no callout pending";
    FilterFailure::paused(Callback::ResponseTrailers, what)
}

impl WasmCtx {
    /// Run `proxy_on_response_trailers` for each plugin, in reverse chain order.
    ///
    /// Call this from your `response_trailer_filter`, pass its arguments through, and return its
    /// result. Only plugins that have
    /// [response_trailers](crate::WasmPluginConf::response_trailers) enabled and export the
    /// callback are run. Plugins can read and change the trailers, and can read the request
    /// headers. This filter does nothing for a subrequest and once a plugin has sent its own
    /// response.
    ///
    /// Call this even if you only run plugins on response bodies. A response with trailers ends
    /// with the trailers, not with a last body chunk, so a plugin that paused on the body is
    /// still holding bytes at this point. Those bytes are returned, and Pingora writes them to
    /// the downstream in place of the trailers.
    ///
    /// A plugin may pause while waiting for a callout, in which case this filter waits with it.
    ///
    /// # Errors
    ///
    /// For a plugin with [FailPolicy::Closed](crate::FailPolicy::Closed), returns
    /// [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) if the plugin traps or otherwise fails,
    /// pauses with no callout pending, waits for callouts longer than its
    /// [callout_wait_limit](crate::WasmPluginConf::callout_wait_limit), or lost the guest holding
    /// this request. A plugin with [FailPolicy::Open](crate::FailPolicy::Open) is skipped
    /// instead. A change it made to the response body no longer prevents that, because the
    /// trailers end the response. A change to a request body that is still being sent upstream
    /// does. See [fail_policy](crate::WasmPluginConf::fail_policy) for the full rule.
    ///
    /// Under both policies, the same error is returned if a plugin sends its own response, or if
    /// an earlier filter of this request was cancelled while a plugin was waiting for a callout.
    ///
    /// Pingora only logs an error from `response_trailer_filter` and still sends the trailers, so
    /// end the response in your filter if they must not go out.
    ///
    /// If a plugin was holding body bytes, the failure is logged and the bytes are returned
    /// instead of the error, so the downstream still gets the whole body.
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
        self.failures.response_body_ended = true;
        let mut passed = self.refuse_after_cancelled_wait();
        if passed.is_ok() && self.chain.phases.response_trailers {
            self.chain.runtime.start_threads()?;
            passed = self.run_trailer_callbacks(session, trailers).await;
        }
        match (passed, self.release_held()) {
            (Err(e), None) => Err(e),
            (Err(e), held) => {
                error!("response trailer filter failed, held body bytes sent in place of the trailers: {e}");
                Ok(held)
            }
            (Ok(()), held) => Ok(held),
        }
    }

    /// Run the trailer callback of each plugin in reverse chain order, waiting for callouts along
    /// the way.
    ///
    /// # Errors
    ///
    /// Returns [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) if a plugin sends its own response.
    /// The same error is returned if a callback fails, or if a plugin is still paused with no
    /// callout left to wait for, unless the plugin's fail policy lets it be skipped.
    async fn run_trailer_callbacks<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        trailers: &mut http::HeaderMap,
    ) -> Result<()> {
        let mut remaining = self.records.len();
        while let Some(position) =
            self.run_trailer_callbacks_before(session, trailers, remaining)?
        {
            let phase = PausedPhase::ResponseTrailers(&mut *trailers);
            match self.wait_for_callouts(session, position, phase).await? {
                CalloutWaitOutcome::Continued | CalloutWaitOutcome::PluginSkipped => {}
                CalloutWaitOutcome::StillPaused => {
                    self.skip_plugin_or_fail_request(position, trailer_pause_failure())?;
                }
                CalloutWaitOutcome::Respond(_) => {
                    return Err(self.late_response_error(position, Callback::ResponseTrailers))
                }
            }
            remaining = position;
        }
        Ok(())
    }

    /// Run the trailer callbacks of the plugins at positions below `remaining`, in reverse order.
    ///
    /// Returns the position of a plugin that paused with a callout pending, or `None` once the
    /// pass is over.
    fn run_trailer_callbacks_before<DS: DownstreamSession>(
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
            if !pool.phases.trailers || self.is_skipped(position) {
                continue;
            }
            let callback = Callback::ResponseTrailers;
            let Some(mut locked) =
                self.lock_slot_or_skip_plugin(pool, position, &record, callback)?
            else {
                continue;
            };
            let loaded = locked.loaded()?;
            let map = ResponseTrailers::new(mem::take(trailers));
            let count = u32::try_from(map.len()).unwrap_or(u32::MAX);
            self.stream().trailers = Some(map);
            self.request_in(session.req_header_mut());
            let action = self.run_for_context(loaded, record.context, |scope| {
                scope.on_response_trailers(record.context, count)
            });
            self.request_out(position, session.req_header_mut());
            if let Some(map) = self.stream().trailers.take() {
                *trailers = map.trailers;
            }
            let sent = self.stream().plugin_response.take();
            let action = match action {
                Ok(action) => action,
                Err(e) => {
                    self.guest_call_failed(position, locked, Callback::ResponseTrailers, e)?;
                    continue;
                }
            };
            drop(locked);
            let paused =
                sent.is_none() && self.plugin_stays_paused(action, StreamType::HttpResponse);
            self.start_callouts(position, paused);
            if sent.is_some() {
                return Err(self.late_response_error(position, Callback::ResponseTrailers));
            }
            if !paused {
                continue;
            }
            if self.waits_for_callout(position) {
                return Ok(Some(position));
            }
            self.skip_plugin_or_fail_request(position, trailer_pause_failure())?;
        }
        Ok(None)
    }

    /// Take the response body bytes still held by plugins and join them in stream order.
    ///
    /// Response bodies run in reverse chain order, so the first plugin in the chain holds the
    /// earliest bytes. A warning is logged for each plugin that held any. Returns `None` if
    /// nothing was held.
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
                "wasm plugin {}: still held {} body bytes at the response trailers, sent in place of the trailers",
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
    async fn plugin_changes_trailer() {
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
    async fn trailer_pause_response_or_trap_fails() {
        let cases = [
            (PAUSE, "paused on response trailers with no callout pending"),
            (TEAPOT, "response rejected, sent after the response header"),
            (TRAP, "proxy_on_response_trailers failed"),
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
    async fn held_bytes_are_released_once_in_place_of_trailers() {
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
    async fn held_bytes_are_joined_in_stream_order() {
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
    async fn failure_still_releases_held_bytes() {
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
