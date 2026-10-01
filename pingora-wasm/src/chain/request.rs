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

use super::ctx::PluginRecord;
use super::slot::LockedSlot;
use super::wait::{CalloutWaitOutcome, PausedPhase};
use super::{RequestOutcome, ResponseProgress, WasmCtx};
use crate::properties::built_in::{RequestStart, TlsFacts};
use crate::stream::PluginResponse;
use http::uri::Scheme;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::{Action, StreamType};
use proxy_wasm_host::abi::v0_2_1::StreamKind;
use std::time::{Instant, SystemTime};

const PAUSED_A_REQUEST: &str = "paused a request";

impl WasmCtx {
    /// Run `proxy_on_request_headers` of each plugin, in chain order.
    ///
    /// Call it from `request_filter`, after the checks your proxy runs itself. Plugins can read
    /// and change the request headers.
    ///
    /// A plugin can pause the request while it waits for the response to a callout. This phase
    /// then waits too, and passes the response to the `proxy_on_http_call_response` of the
    /// plugin, where the plugin can change the request headers before it continues. The plugins
    /// after it in the chain run once it continues.
    ///
    /// When a plugin sends its own response, the later plugins do not run and the earlier
    /// plugins see the response headers. The response comes back as [RequestOutcome::Respond].
    ///
    /// Subrequests do not run the plugins.
    ///
    /// # Errors
    ///
    /// An error of type [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) when a plugin traps or
    /// fails, or when a plugin pauses the request and has no callout to wait for.
    pub async fn request_filter<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
    ) -> Result<RequestOutcome> {
        if session.subrequest_ctx.is_some() {
            return Ok(RequestOutcome::Continue);
        }
        self.refuse_after_cancelled_wait()?;
        self.chain.runtime.start_threads()?;
        let end_of_stream = session.is_body_empty();
        if !end_of_stream {
            self.request_body.expect_body();
        }
        self.scheme = scheme_of(session);
        self.record_request_facts(session);
        for position in 0..self.chain.plugins.len() {
            let action = self.run_request_headers_at(session, position, end_of_stream)?;
            let sent = self.stream().plugin_response.take();
            let paused =
                sent.is_none() && self.plugin_stays_paused(action, StreamType::HttpRequest);
            self.start_callouts(position, paused);
            if let Some(response) = sent {
                return self.respond_to_request_headers(session, position, response);
            }
            if !paused {
                continue;
            }
            if !self.waits_for_callout(position) {
                return Err(self.plugin_error(position, PAUSED_A_REQUEST));
            }
            let wait_outcome = self
                .wait_for_callouts(session, position, PausedPhase::RequestHeaders)
                .await?;
            match wait_outcome {
                CalloutWaitOutcome::Continued => {}
                CalloutWaitOutcome::StillPaused => {
                    return Err(self.plugin_error(position, PAUSED_A_REQUEST))
                }
                CalloutWaitOutcome::Respond(response) => {
                    return self.respond_to_request_headers(session, position, *response)
                }
            }
        }
        Ok(RequestOutcome::Continue)
    }

    /// Create the context of the plugin at `position`, and run its `proxy_on_request_headers`.
    fn run_request_headers_at<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        end_of_stream: bool,
    ) -> Result<Action> {
        let runtime = self.chain.runtime.clone();
        let pool = &runtime.pools[self.chain.plugins[position]];
        let mut locked = LockedSlot::for_new_request(pool)?;
        let slot = locked.slot;
        let loaded = locked.loaded()?;
        let root = loaded.root;
        let guest = loaded.guest.id();
        let created = self.run(&mut loaded.guest, |scope| {
            scope.on_context_create(Some(root))
        });
        let context = match created {
            Ok(context) => context,
            Err(e) => return Err(locked.guest_failure("could not create a context", e)),
        };
        pool.opened(slot);
        self.records[position] = Some(PluginRecord {
            slot,
            guest,
            context,
        });
        self.request_in(session.req_header_mut());
        let count = self.request_count();
        let action = self.run_for_context(loaded, context, |scope| {
            scope.expect_stream_kind(context, StreamKind::Http)?;
            scope.on_request_headers(context, count, end_of_stream)
        });
        self.request_out(session.req_header_mut());
        match action {
            Ok(action) => Ok(action),
            Err(e) => Err(locked.guest_failure("failed in on_request_headers", e)),
        }
    }

    /// Return the response that the plugin at `position` sent to the request headers, after the
    /// plugins before it ran on its header.
    fn respond_to_request_headers<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        response: PluginResponse,
    ) -> Result<RequestOutcome> {
        let mut header = response.header;
        let empty = response.body.is_empty();
        self.pass_plugin_response(session, position, &mut header, empty)?;
        self.response_progress = ResponseProgress::FromPlugin;
        Ok(RequestOutcome::Respond(Box::new(header), response.body))
    }
}

impl WasmCtx {
    /// Record the facts of the request that the headers do not have, for the properties.
    fn record_request_facts<DS: DownstreamSession>(&mut self, session: &Session<DS>) {
        let facts = &mut self.stream().request_facts;
        facts.client_address = session.client_addr().and_then(|a| a.as_inet()).copied();
        facts.server_address = session.server_addr().and_then(|a| a.as_inet()).copied();
        let tls = session.digest().and_then(|d| d.ssl_digest.as_deref());
        facts.tls = tls.map(TlsFacts::new);
        facts.start = Some(RequestStart {
            wall_time: SystemTime::now(),
            monotonic_time: Instant::now(),
        });
    }
}

fn scheme_of<DS: DownstreamSession>(session: &Session<DS>) -> Scheme {
    match session.req_header().uri.scheme() {
        Some(scheme) => scheme.clone(),
        None if session.digest().is_some_and(|d| d.ssl_digest.is_some()) => Scheme::HTTPS,
        None => Scheme::HTTP,
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::{
        crate_log_lines_with, one_plugin, record_crate_logs, session, wat_plugin, GET, TEAPOT,
    };
    use crate::WasmRuntime;
    use crate::{RequestOutcome, ERR_PLUGIN_FAILED};

    #[tokio::test]
    async fn a_plugin_response_is_returned_to_the_caller() {
        let (_runtime, mut ctx) = one_plugin(wat_plugin("respond-unit", TEAPOT));
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await.unwrap();

        let RequestOutcome::Respond(header, body) = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(header.status, 418);
        assert_eq!(&body[..], b"teapot");
        assert!(ctx.plugin_responded());
    }

    #[tokio::test]
    async fn a_guest_error_restores_the_session_header() {
        let (_runtime, mut ctx) = one_plugin(wat_plugin("bad-unit", "i32.const 7"));
        let (mut session, _client) = session(GET).await;

        let err = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert_eq!(session.req_header().raw_path(), b"/original");
        assert_eq!(session.req_header().headers["host"], "example.test");
    }

    #[tokio::test]
    async fn a_pause_without_a_plugin_response_fails() {
        let (_runtime, mut ctx) = one_plugin(wat_plugin("pause-unit", "i32.const 1"));
        let (mut session, _client) = session(GET).await;

        let err = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("paused a request"));
    }

    #[tokio::test]
    async fn a_trap_logs_one_line_for_the_guest_that_was_replaced() {
        record_crate_logs();
        let mut conf = wat_plugin("trap-log-unit", "unreachable");
        conf.name = "replaced-once".to_string();
        let runtime = WasmRuntime::new(vec![conf]).unwrap();
        let mut ctx = runtime.chain(&["replaced-once"]).unwrap().new_ctx();
        let (mut session, _client) = session(GET).await;

        let trapped = ctx.request_filter(&mut session).await;

        assert!(trapped.is_err());
        let mut lines = crate_log_lines_with("replaced-once");
        lines.retain(|line| line.contains("guest"));
        assert_eq!(lines.len(), 1, "{lines:?}");
        let want = "wasm plugin replaced-once replaced the guest of slot 0 after a failure: ";
        assert!(lines[0].starts_with(want), "{lines:?}");
    }

    #[tokio::test]
    async fn a_trap_replaces_the_guest_and_resets_its_counts() {
        let (runtime, mut ctx) = one_plugin(wat_plugin("trap-unit", "unreachable"));
        let (mut session, _client) = session(GET).await;

        let err = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert_eq!(runtime.open_contexts(), 0);
        let slot = runtime.inner.pools[0].lock_slot(0);
        assert!(slot.as_ref().unwrap().guest.is_serving());
    }
}
