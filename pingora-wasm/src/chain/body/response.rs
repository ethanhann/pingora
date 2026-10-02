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

use super::{filter_output, BodyDirection, BodyOutcome};
use crate::chain::WasmCtx;
use bytes::Bytes;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::Callback;

impl WasmCtx {
    /// Run `proxy_on_response_body` for each plugin, in reverse chain order.
    ///
    /// Call this from your `response_body_filter`, pass its arguments through, and return
    /// `Ok(None)` from the filter. Only plugins that have
    /// [response_body](crate::WasmPluginConf::response_body) enabled and export the callback are
    /// run. A plugin may pause on a chunk to wait for a callout, in which case this filter waits
    /// with it, up to its [callout_wait_limit](crate::WasmPluginConf::callout_wait_limit).
    ///
    /// A plugin that changes the body length must remove `content-length` in
    /// `proxy_on_response_headers`. If a plugin changes the body of a range response,
    /// `content-range` will no longer match it. This filter returns an
    /// [ERR_RESPONSE_BODY_TOO_LARGE](crate::ERR_RESPONSE_BODY_TOO_LARGE) error if a plugin holds
    /// more bytes than its [response_body_limit](crate::WasmPluginConf::response_body_limit).
    ///
    /// Plugins cannot add trailers to a response. A plugin built with the Rust SDK panics if it
    /// writes a trailer in `proxy_on_response_body`.
    pub async fn response_body_filter<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        body: &mut Option<Bytes>,
        end_of_stream: bool,
    ) -> Result<()> {
        if self.skips_body(session, BodyDirection::Response) {
            return Ok(());
        }
        self.refuse_after_cancelled_wait()?;
        if !end_of_stream && body.as_ref().is_none_or(Bytes::is_empty) {
            return Ok(());
        }
        self.chain.runtime.start_threads()?;
        let chunk = body.take().unwrap_or_default();
        let outcome = self
            .run_body_callbacks(session, BodyDirection::Response, chunk, end_of_stream)
            .await?;
        match outcome {
            BodyOutcome::Released(output) => {
                self.failures.response_body_ended |= end_of_stream;
                *body = filter_output(output, end_of_stream);
                Ok(())
            }
            BodyOutcome::Respond(position, _) => {
                Err(self.late_response_error(position, Callback::ResponseBody))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::{
        body_chunk, body_plugin, read_downstream_after_marker, start_request, Wat,
        HOLD_THEN_REPLACE, MARKER_RESPONSE, POST, TEAPOT,
    };
    use crate::ERR_PLUGIN_FAILED;

    #[tokio::test]
    async fn plugin_reads_held_chunks_as_one_body() {
        let plugins = vec![body_plugin("a", Wat::response_body(HOLD_THEN_REPLACE))];
        let (_runtime, mut ctx, mut session, _client) = start_request(plugins, POST).await;
        let mut chunks = [body_chunk("aaa"), body_chunk("bbb"), body_chunk("cc")];

        for (index, body) in chunks.iter_mut().enumerate() {
            ctx.response_body_filter(&mut session, body, index == 2)
                .await
                .unwrap();
        }

        assert_eq!(
            chunks,
            [body_chunk(""), body_chunk(""), body_chunk("replaced")]
        );
    }

    #[tokio::test]
    async fn plugin_response_from_response_body_fails() {
        let plugins = vec![body_plugin("a", Wat::response_body(TEAPOT))];
        let (_runtime, mut ctx, mut session, mut client) = start_request(plugins, POST).await;

        let err = ctx
            .response_body_filter(&mut session, &mut body_chunk("origin"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err
            .to_string()
            .contains("response rejected, sent after the response header"));
        assert!(!ctx.plugin_responded());
        let written = read_downstream_after_marker(&mut session, &mut client).await;
        assert!(written.starts_with(MARKER_RESPONSE), "{written}");
    }
}
