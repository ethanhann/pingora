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

impl WasmCtx {
    /// Run `proxy_on_response_body` of each plugin, in reverse chain order.
    ///
    /// Call it from `response_body_filter` with the arguments of that filter, and return `Ok(None)`
    /// from the filter. Only plugins with [response_body](crate::WasmPluginConf::response_body)
    /// turned on run. Plugins can read and replace the body, and can read the request headers.
    ///
    /// A plugin can pause to wait for more of the body. This phase then holds the bytes for the
    /// plugin and leaves an empty chunk for the downstream. On the next chunk, the plugin reads the
    /// bytes it paused on and the new bytes together. A plugin can hold up to
    /// [response_body_limit](crate::WasmPluginConf::response_body_limit) bytes.
    ///
    /// A plugin can also pause a chunk while it waits for a callout, and this phase waits with
    /// it. When the plugin continues, the bytes that it holds go to the next plugin.
    ///
    /// A plugin that changes the length of the body must remove `content-length` in
    /// `proxy_on_response_headers`. A plugin that changes the body of a range response leaves a
    /// `content-range` that does not match the body.
    ///
    /// A plugin cannot add trailers to a response. A plugin built with the Rust SDK panics when it
    /// writes a trailer in `proxy_on_response_body`, because the write returns `BadArgument`.
    ///
    /// By default no plugin runs on response bodies, and this phase does nothing. It also does
    /// nothing for a subrequest and after an upgrade.
    ///
    /// # Errors
    ///
    /// An error of type [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) when a plugin traps or fails,
    /// when a plugin pauses the last chunk of a body and does not continue, or when a plugin sends
    /// its own response. An error of type
    /// [ERR_RESPONSE_BODY_TOO_LARGE](crate::ERR_RESPONSE_BODY_TOO_LARGE) when a plugin holds more
    /// bytes than its limit. Pingora sent the response header before this phase,
    /// so after an error the downstream receives a response that ends early.
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
        self.chain.runtime.start_ticker()?;
        let chunk = body.take().unwrap_or_default();
        let outcome = self
            .run_body_callbacks(session, BodyDirection::Response, chunk, end_of_stream)
            .await?;
        match outcome {
            BodyOutcome::Released(output) => {
                *body = filter_output(output, end_of_stream);
                Ok(())
            }
            BodyOutcome::Respond(position, _) => Err(self.late_response_error(position)),
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
    async fn a_plugin_reads_the_chunks_it_held_as_one_body() {
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
    async fn a_plugin_response_from_the_response_body_fails() {
        let plugins = vec![body_plugin("a", Wat::response_body(TEAPOT))];
        let (_runtime, mut ctx, mut session, mut client) = start_request(plugins, POST).await;

        let err = ctx
            .response_body_filter(&mut session, &mut body_chunk("origin"), false)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err
            .to_string()
            .contains("sent a response after the response header"));
        assert!(!ctx.plugin_responded());
        let written = read_downstream_after_marker(&mut session, &mut client).await;
        assert!(written.starts_with(MARKER_RESPONSE), "{written}");
    }
}
