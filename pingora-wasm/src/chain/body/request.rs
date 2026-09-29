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
    /// Run `proxy_on_request_body` of each plugin, in chain order.
    ///
    /// Call it from `request_body_filter` with the arguments of that filter. Only plugins with
    /// [request_body](crate::WasmPluginConf::request_body) turned on run. Plugins can read and
    /// replace the body, and can read the request headers.
    ///
    /// A plugin can pause to wait for more of the body. This phase then holds the bytes for the
    /// plugin and leaves an empty chunk for the upstream. On the next chunk, the plugin reads the
    /// bytes it paused on and the new bytes together. A plugin can hold up to
    /// [request_body_limit](crate::WasmPluginConf::request_body_limit) bytes.
    ///
    /// A plugin can also pause a chunk while it waits for a callout, and this phase waits with
    /// it. When the plugin continues, the bytes that it holds go to the next plugin.
    ///
    /// Pingora sends the request header to the upstream before it reads the body. A plugin that
    /// changes the length of the body must remove `content-length` in `proxy_on_request_headers`.
    ///
    /// A plugin can send its own response, for example to deny a request after it read the body.
    /// The plugins before it in the chain run `proxy_on_response_headers` on that response, and
    /// this phase writes it to the downstream. The phase then returns an error with the status of
    /// the response to stop the request, and [WasmCtx::plugin_responded] returns `true`.
    ///
    /// By default no plugin runs on request bodies, and this phase does nothing. It also does
    /// nothing for a request with no body, for a subrequest, and after an upgrade.
    ///
    /// # Errors
    ///
    /// An error of type [ERR_PLUGIN_FAILED] when a plugin traps or fails, when a plugin pauses the
    /// last chunk of a body and does not continue, or when [WasmCtx::upstream_attempt] did not
    /// run. An error of type [ERR_REQUEST_BODY_TOO_LARGE](crate::ERR_REQUEST_BODY_TOO_LARGE) when
    /// a plugin holds more bytes than its limit.
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
                "a wasm plugin reads the request body and upstream_peer did not call WasmCtx::upstream_attempt",
            );
        }
        // With request trailers, an H2 downstream ends its body with `None` and no end flag
        let end_of_stream = end_of_stream || body.is_none();
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
                "request body bytes arrived after the end of the body",
            );
        }
        if empty && !end_of_stream {
            return Ok(());
        }
        self.request_body.progress = RequestBodyProgress::Streaming;
        self.chain.runtime.start_ticker()?;
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

    /// Run the request body phase on each chunk, and return what it leaves for Pingora.
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
    async fn a_body_call_with_no_upstream_attempt_fails() {
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
    async fn a_replay_receives_the_output_of_the_first_attempt() {
        let (_runtime, mut ctx, mut session, _client) = start_request(mark(), POST).await;
        filter(&mut ctx, &mut session, &[("x", false), ("y", true)]).await;
        ctx.upstream_attempt();

        let replay = filter(&mut ctx, &mut session, &[("xy", true)]).await;

        assert_eq!(replay, [body_chunk("axay")]);
    }

    #[tokio::test]
    async fn live_chunks_follow_a_replay_in_the_middle_of_a_body() {
        let (_runtime, mut ctx, mut session, _client) = start_request(mark(), POST).await;
        filter(&mut ctx, &mut session, &[("x", false)]).await;
        ctx.upstream_attempt();

        let outputs = filter(&mut ctx, &mut session, &[("x", false), ("y", true)]).await;

        assert_eq!(outputs, [body_chunk("ax"), body_chunk("ay")]);
    }

    #[tokio::test]
    async fn a_second_attempt_with_no_bytes_before_it_runs_the_plugins() {
        let (_runtime, mut ctx, mut session, _client) = start_request(mark(), POST).await;
        ctx.upstream_attempt();

        let outputs = filter(&mut ctx, &mut session, &[("x", true)]).await;

        assert_eq!(outputs, [body_chunk("ax")]);
    }

    #[tokio::test]
    async fn bytes_after_the_end_of_the_body_fail() {
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
    async fn a_plugin_sends_its_own_response() {
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
    async fn the_plugins_before_the_sender_see_its_response_and_cannot_replace_it() {
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
            "the first plugin ran and trapped"
        );
        assert!(
            err.to_string().contains("wasm plugin first failed"),
            "{err}"
        );
        assert_eq!(read_downstream(&mut client).await, "");
        let last = runtime.inner.pools[3].lock_slot(0);
        assert!(last.as_ref().unwrap().guest.is_serving());
    }

    #[tokio::test]
    async fn a_second_plugin_response_is_dropped() {
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
    async fn a_plugin_response_after_the_upstream_response_fails() {
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
