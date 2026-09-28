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
use super::failure::Locked;
use super::{Exchange, RequestOutcome, WasmCtx};
use crate::plugin_unavailable;
use http::uri::Scheme;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::Action;
use proxy_wasm_host::abi::v0_2_1::StreamKind;

impl WasmCtx {
    /// Run `proxy_on_request_headers` of each plugin, in chain order.
    ///
    /// Call it from `request_filter`, after the checks your proxy runs itself. Plugins can read
    /// and change the request headers.
    ///
    /// When a plugin sends its own response, the later plugins do not run and the earlier
    /// plugins see the response headers. The response comes back as [RequestOutcome::Respond].
    ///
    /// Subrequests do not run the plugins.
    ///
    /// # Errors
    ///
    /// An error of type [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) when a plugin traps,
    /// fails, or pauses the request. Pausing a request is not supported.
    pub async fn request_filter<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
    ) -> Result<RequestOutcome> {
        if session.subrequest_ctx.is_some() {
            return Ok(RequestOutcome::Continue);
        }
        let runtime = self.chain.runtime.clone();
        runtime.start_ticker()?;
        let end_of_stream = session.is_body_empty();
        if !end_of_stream {
            self.request_body.expect();
        }
        self.scheme = scheme_of(session);
        let plugins = self.chain.plugins.clone();
        for (position, &plugin) in plugins.iter().enumerate() {
            let pool = &runtime.pools[plugin];
            let mut locked = Locked::pick(pool)?;
            let slot = locked.slot;
            let loaded = locked.loaded()?;
            let root = loaded.root;
            let guest = loaded.guest.id();
            let created = self.run(&mut loaded.guest, |scope| {
                scope.on_context_create(Some(root))
            });
            let context = match created {
                Ok(context) => context,
                Err(e) => return Err(locked.failed("could not create a context", e)),
            };
            pool.opened(slot);
            self.records[position] = Some(PluginRecord {
                slot,
                guest,
                context,
            });
            self.request_in(session.req_header_mut());
            let count = self.request_count();
            let action = self.run(&mut loaded.guest, |scope| {
                scope.expect_stream_kind(context, StreamKind::Http)?;
                scope.on_request_headers(context, count, end_of_stream)
            });
            self.request_out(session.req_header_mut());
            let action = match action {
                Ok(action) => action,
                Err(e) => return Err(locked.failed("failed in on_request_headers", e)),
            };
            drop(locked);
            if let Some(response) = self.stream().plugin_response.take() {
                let mut header = response.header;
                let empty = response.body.is_empty();
                self.pass_plugin_response(session, position, &mut header, empty)?;
                self.exchange = Exchange::Responded;
                return Ok(RequestOutcome::Respond(Box::new(header), response.body));
            }
            if action == Action::Pause {
                return Err(plugin_unavailable(&pool.name, "paused a request"));
            }
        }
        Ok(RequestOutcome::Continue)
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
    use crate::test_support::{one_plugin, session, wat_plugin, GET, TEAPOT};
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
