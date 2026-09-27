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
use super::{RequestOutcome, WasmCtx};
use crate::{plugin_failure, plugin_unavailable};
use http::uri::Scheme;
use http::Method;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::Action;
use proxy_wasm_host::abi::v0_2_1::StreamKind;

impl WasmCtx {
    /// Runs `on_request_headers` of each plugin, in order.
    ///
    /// Call it from `request_filter`. A [RequestOutcome::Respond] means a plugin answered the
    /// request: write it, for example with [write_plugin_response](crate::write_plugin_response),
    /// and return `Ok(true)`.
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
        self.scheme = scheme_of(session);
        let plugins = self.chain.plugins.clone();
        for (position, &plugin) in plugins.iter().enumerate() {
            let pool = &runtime.pools[plugin];
            let (slot, mut guard) = pool.pick()?;
            let Some(loaded) = guard.as_mut() else {
                return Err(plugin_unavailable(&pool.name, "has no guest"));
            };
            let root = loaded.root;
            let guest = loaded.guest.id();
            let created = self.run(&mut loaded.guest, |scope| {
                scope.on_context_create(Some(root))
            });
            let context = match created {
                Ok(context) => context,
                Err(e) => {
                    pool.check(slot, guard, &e);
                    return Err(plugin_failure(&pool.name, "could not create a context", e));
                }
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
                Err(e) => {
                    pool.check(slot, guard, &e);
                    return Err(plugin_failure(
                        &pool.name,
                        "failed in on_request_headers",
                        e,
                    ));
                }
            };
            drop(guard);
            if let Some(response) = self.stream().plugin_response.take() {
                let mut header = response.header;
                let end_of_stream =
                    response.body.is_empty() || session.req_header().method == Method::HEAD;
                self.response_pass(session, &mut header, (0..=position).rev(), end_of_stream)?;
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
    use crate::test_support::{one_plugin, session, wat_plugin, GET};
    use pingora_error::ErrorType;

    #[tokio::test]
    async fn a_guest_error_restores_the_session_header() {
        let (_runtime, mut ctx) = one_plugin(wat_plugin("bad-unit", "i32.const 7"));
        let (mut session, _client) = session(GET).await;

        let err = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(503));
        assert_eq!(session.req_header().raw_path(), b"/original");
        assert_eq!(session.req_header().headers["host"], "example.test");
    }

    #[tokio::test]
    async fn a_trap_replaces_the_guest_and_resets_its_counts() {
        let (runtime, mut ctx) = one_plugin(wat_plugin("trap-unit", "unreachable"));
        let (mut session, _client) = session(GET).await;

        let err = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(503));
        assert_eq!(runtime.open_contexts(), 0);
        let slot = runtime.inner.pools[0].lock_slot(0);
        assert!(slot.as_ref().unwrap().guest.is_serving());
    }
}
