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

use super::retry::RequestBodyProgress;
use super::{filter_output, BodyDirection, BodyOutcome};
use crate::chain::WasmCtx;
use crate::ERR_PLUGIN_FAILED;
use bytes::Bytes;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::{Error, Result};
use pingora_proxy::Session;
use std::mem;

impl WasmCtx {
    /// Run `proxy_on_request_body` for each plugin, in chain order.
    ///
    /// Call this from your `request_body_filter` and pass its arguments through. Only plugins
    /// that have [request_body](crate::WasmPluginConf::request_body) enabled and export the
    /// callback are run. By default no plugin runs on request bodies and this filter does
    /// nothing.
    ///
    /// A plugin may send its own response, e.g. to deny a request after inspecting the body.
    /// This filter writes it to the downstream and then returns an error with the response
    /// status to stop the request. It returns an
    /// [ERR_REQUEST_BODY_TOO_LARGE](crate::ERR_REQUEST_BODY_TOO_LARGE) error if a plugin holds
    /// more bytes than its [request_body_limit](crate::WasmPluginConf::request_body_limit).
    ///
    /// Pingora has already sent the request header upstream by the time it reads the body, so a
    /// plugin that changes the body length must remove `content-length` in
    /// `proxy_on_request_headers`. This filter also returns an error if a plugin runs on the
    /// body and [WasmCtx::upstream_attempt] was not called from `upstream_peer`.
    pub async fn request_body_filter<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
    ) -> Result<()> {
        if self.skips_body(session, BodyDirection::Request)
            || self.request_body.progress == RequestBodyProgress::Absent
        {
            return Ok(());
        }
        self.refuse_after_cancelled_wait()?;
        if self.request_body.attempts == 0 {
            return Error::e_explain(
                ERR_PLUGIN_FAILED,
                "WasmCtx::upstream_attempt was not called from upstream_peer, required when a wasm plugin runs on the request body",
            );
        }
        // An H2 downstream that sends request trailers ends its body with `None` and never sets
        // the end flag
        let end_of_stream = end_of_stream || body.is_none();
        // A retry gets what the plugins produced the first time, without running them again
        let replay = mem::take(&mut self.request_body.replay_due)
            && self.request_body.progress != RequestBodyProgress::Waiting;
        if let (true, Some(kept)) = (replay, self.request_body.kept.as_ref()) {
            *body = filter_output(Bytes::copy_from_slice(kept), end_of_stream);
            return Ok(());
        }
        let empty = body.as_ref().is_none_or(Bytes::is_empty);
        if self.request_body.progress == RequestBodyProgress::Ended {
            if empty {
                return Ok(());
            }
            return Error::e_explain(
                ERR_PLUGIN_FAILED,
                "request body chunk received after the end of the body",
            );
        }
        if empty && !end_of_stream {
            return Ok(());
        }
        self.request_body.progress = RequestBodyProgress::Streaming;
        self.chain.runtime.start_threads()?;
        self.stream().request_facts.request_body_bytes = session.body_bytes_read();
        let chunk = body.take().unwrap_or_default();
        let outcome = self
            .run_body_callbacks(session, BodyDirection::Request, chunk, end_of_stream)
            .await?;
        match outcome {
            BodyOutcome::Released(output) => {
                if end_of_stream {
                    self.request_body.progress = RequestBodyProgress::Ended;
                }
                self.keep_for_retry(session, &output);
                *body = filter_output(output, end_of_stream);
                Ok(())
            }
            BodyOutcome::Respond(position, response) => Err(self
                .respond_to_request_body(session, position, *response)
                .await),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        body_chunk, body_plugin, read_downstream, read_downstream_after_marker, session,
        start_request, Wat, FORBIDDEN, MARKER_RESPONSE, MARK_A_REQUEST, POST, TEAPOT, TRAP,
    };
    use crate::{WasmPluginConf, WasmRuntime};
    use pingora_error::ErrorType;
    use pingora_http::ResponseHeader;

    fn mark() -> Vec<WasmPluginConf> {
        vec![body_plugin("a", Wat::request_body(MARK_A_REQUEST))]
    }

    fn teapot() -> Vec<WasmPluginConf> {
        vec![body_plugin("a", Wat::request_body(TEAPOT))]
    }

    /// Run the request body filter on each chunk and collect what it leaves in `body`.
    async fn filter(
        ctx: &mut WasmCtx,
        session: &mut Session,
        chunks: &[(&'static str, bool)],
    ) -> Vec<Option<Bytes>> {
        let mut outputs = Vec::new();
        for (bytes, end_of_stream) in chunks {
            let mut body = body_chunk(bytes);
            ctx.request_body_filter(session, &mut body, *end_of_stream)
                .await
                .unwrap();
            outputs.push(body);
        }
        outputs
    }

    #[tokio::test]
    async fn body_without_upstream_attempt_fails() {
        let runtime = WasmRuntime::new(mark()).unwrap();
        let mut ctx = runtime.chain(&["a"]).unwrap().new_ctx();
        let (mut session, _client) = session(POST).await;
        ctx.request_filter(&mut session).await.unwrap();
        let mut body = body_chunk("x");

        let err = ctx
            .request_body_filter(&mut session, &mut body, true)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("upstream_attempt"), "{err}");
        assert_eq!(body, body_chunk("x"));
    }

    #[tokio::test]
    async fn replay_sends_first_attempt_output() {
        let (_runtime, mut ctx, mut session, _client) = start_request(mark(), POST).await;
        filter(&mut ctx, &mut session, &[("x", false), ("y", true)]).await;
        ctx.upstream_attempt();

        let replay = filter(&mut ctx, &mut session, &[("xy", true)]).await;

        assert_eq!(replay, [body_chunk("axay")]);
    }

    #[tokio::test]
    async fn live_chunks_follow_mid_body_replay() {
        let (_runtime, mut ctx, mut session, _client) = start_request(mark(), POST).await;
        filter(&mut ctx, &mut session, &[("x", false)]).await;
        ctx.upstream_attempt();

        let outputs = filter(&mut ctx, &mut session, &[("x", false), ("y", true)]).await;

        assert_eq!(outputs, [body_chunk("ax"), body_chunk("ay")]);
    }

    #[tokio::test]
    async fn retry_before_any_body_runs_plugins() {
        let (_runtime, mut ctx, mut session, _client) = start_request(mark(), POST).await;
        ctx.upstream_attempt();

        let outputs = filter(&mut ctx, &mut session, &[("x", true)]).await;

        assert_eq!(outputs, [body_chunk("ax")]);
    }

    #[tokio::test]
    async fn chunk_after_end_of_body_fails() {
        let (_runtime, mut ctx, mut session, _client) = start_request(mark(), POST).await;
        filter(&mut ctx, &mut session, &[("x", true)]).await;

        let err = ctx
            .request_body_filter(&mut session, &mut body_chunk("x"), true)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("after the end of the body"));
    }

    #[tokio::test]
    async fn plugin_sends_its_own_response() {
        let (_runtime, mut ctx, mut session, mut client) = start_request(teapot(), POST).await;

        let err = ctx
            .request_body_filter(&mut session, &mut body_chunk("attack"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(418));
        assert!(ctx.plugin_responded());
        let response = read_downstream(&mut client).await;
        assert!(response.starts_with("HTTP/1.1 418"), "{response}");
        assert!(response.ends_with("\r\n\r\nteapot"), "{response}");
        assert!(response.contains("\r\nConnection: close\r\n"), "{response}");
    }

    #[tokio::test]
    async fn plugins_ahead_of_sender_see_response_and_cannot_replace_it() {
        let plugins = vec![
            body_plugin("first", Wat::response_headers(TRAP)),
            body_plugin("second", Wat::response_headers(FORBIDDEN)),
            body_plugin("sender", Wat::request_body(TEAPOT)),
            body_plugin("last", Wat::response_headers(TRAP)),
        ];
        let (runtime, mut ctx, mut session, mut client) = start_request(plugins, POST).await;

        let err = ctx
            .request_body_filter(&mut session, &mut body_chunk("attack"), false)
            .await
            .unwrap_err();

        assert_eq!(
            err.etype(),
            &ERR_PLUGIN_FAILED,
            "first plugin should have run and trapped"
        );
        assert!(
            err.to_string()
                .contains("wasm plugin first: proxy_on_response_headers failed"),
            "{err}"
        );
        assert_eq!(read_downstream(&mut client).await, "");
        let last = runtime.inner.pools[3].lock_slot(0);
        assert!(last.as_ref().unwrap().guest.is_serving());
    }

    #[tokio::test]
    async fn second_plugin_response_is_dropped() {
        let plugins = vec![
            body_plugin("second", Wat::response_headers(FORBIDDEN)),
            body_plugin("sender", Wat::request_body(TEAPOT)),
        ];
        let (_runtime, mut ctx, mut session, mut client) = start_request(plugins, POST).await;

        let err = ctx
            .request_body_filter(&mut session, &mut body_chunk("attack"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(418));
        let response = read_downstream(&mut client).await;
        assert!(response.starts_with("HTTP/1.1 418"), "{response}");
        assert!(!response.contains("403"), "{response}");
    }

    #[tokio::test]
    async fn plugin_response_after_upstream_response_fails() {
        let (_runtime, mut ctx, mut session, mut client) = start_request(teapot(), POST).await;
        let mut upstream = ResponseHeader::build(200, None).unwrap();
        ctx.response_filter(&mut session, &mut upstream)
            .await
            .unwrap();

        let err = ctx
            .request_body_filter(&mut session, &mut body_chunk("attack"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(!ctx.plugin_responded());
        let written = read_downstream_after_marker(&mut session, &mut client).await;
        assert!(written.starts_with(MARKER_RESPONSE), "{written}");
    }
}
