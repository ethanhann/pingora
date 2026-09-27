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

use crate::ctx::{PluginRecord, WasmCtx};
use crate::pool::{failure, unavailable, GuestPool, SlotGuard};
use crate::runtime::RuntimeInner;
use bytes::Bytes;
use http::header::CONTENT_LENGTH;
use http::{Method, StatusCode};
use log::{error, warn};
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_http::ResponseHeader;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::Action;
use proxy_wasm_host::abi::v0_2_1::{CallScope, ContextId, GuestError, StreamKind, StreamState};
use std::fmt;
use std::sync::Arc;

/// An ordered list of plugins from one [WasmRuntime](crate::WasmRuntime).
///
/// The request phase runs the plugins in order, and the response phase runs them in reverse.
#[derive(Clone)]
pub struct WasmChain {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) plugins: Arc<[usize]>,
}

/// What a proxy does after the request phase of a chain.
#[derive(Debug)]
#[non_exhaustive]
pub enum RequestOutcome {
    /// Every plugin let the request continue.
    Continue,
    /// A plugin answered the request. Write this response and end the request.
    Respond(Box<ResponseHeader>, Bytes),
}

impl WasmChain {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, plugins: Vec<usize>) -> Self {
        WasmChain {
            runtime,
            plugins: plugins.into(),
        }
    }

    /// The state of one request in this chain.
    pub fn new_ctx(&self) -> WasmCtx {
        WasmCtx::new(self.clone())
    }

    pub(crate) fn plugin_names(&self) -> Vec<&str> {
        self.plugins
            .iter()
            .map(|index| self.runtime.pools[*index].name.as_str())
            .collect()
    }
}

impl fmt::Debug for WasmChain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmChain")
            .field("plugins", &self.plugin_names())
            .finish()
    }
}

impl WasmCtx {
    /// Runs `on_request_headers` of each plugin, in order.
    ///
    /// Call it from `request_filter`. A [RequestOutcome::Respond] means a plugin answered the
    /// request: write it, for example with [write_local_response](crate::write_local_response),
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
                return Err(unavailable(&pool.name, "has no guest"));
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
                    return Err(failure(&pool.name, "could not create a context", e));
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
                    return Err(failure(&pool.name, "failed in on_request_headers", e));
                }
            };
            drop(guard);
            if let Some(local) = self.stream().local.take() {
                let mut header = local.header;
                let end_of_stream =
                    local.body.is_empty() || session.req_header().method == Method::HEAD;
                self.response_pass(session, &mut header, (0..=position).rev(), end_of_stream)?;
                return Ok(RequestOutcome::Respond(Box::new(header), local.body));
            }
            if action == Action::Pause {
                return Err(unavailable(&pool.name, "paused a request"));
            }
        }
        Ok(RequestOutcome::Continue)
    }

    /// Runs `on_response_headers` of each plugin that saw the request, in reverse order.
    ///
    /// Call it from `response_filter`.
    pub async fn response_filter<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        resp: &mut ResponseHeader,
    ) -> Result<()> {
        if session.subrequest_ctx.is_some() || skips_response(resp.status) {
            return Ok(());
        }
        self.chain.runtime.start_ticker()?;
        let end_of_stream = response_ends(&session.req_header().method, resp);
        self.response_pass(session, resp, (0..self.records.len()).rev(), end_of_stream)
    }

    /// Runs `on_done`, `on_log`, and `on_delete` of each plugin that saw the request, in
    /// reverse order.
    ///
    /// Call it from `logging`.
    pub async fn logging<DS: DownstreamSession>(&mut self, session: &mut Session<DS>) {
        let runtime = self.chain.runtime.clone();
        let mut response = session.response_written().cloned();
        for position in (0..self.records.len()).rev() {
            let Some(record) = self.records[position].take() else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            let Some(mut guard) = pool.lock(record.slot, record.guest) else {
                continue;
            };
            let Some(loaded) = guard.as_mut() else {
                continue;
            };
            self.request_in(session.req_header_mut());
            if let Some(header) = response.as_mut() {
                self.response_in(header);
            }
            let result = self.run(&mut loaded.guest, |scope| {
                finish(scope, record.context, true)
            });
            if let Some(header) = response.as_mut() {
                self.response_out(header);
            }
            self.request_out(session.req_header_mut());
            finished(pool, record.slot, guard, result);
        }
    }

    fn response_pass<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        resp: &mut ResponseHeader,
        positions: impl Iterator<Item = usize>,
        end_of_stream: bool,
    ) -> Result<()> {
        let runtime = self.chain.runtime.clone();
        for position in positions {
            let Some(record) = self.records[position] else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            let Some(mut guard) = pool.lock(record.slot, record.guest) else {
                return Err(unavailable(&pool.name, "lost the guest of this request"));
            };
            let Some(loaded) = guard.as_mut() else {
                return Err(unavailable(&pool.name, "has no guest"));
            };
            self.request_in(session.req_header_mut());
            self.response_in(resp);
            let count = self.response_count();
            let action = self.run(&mut loaded.guest, |scope| {
                scope.on_response_headers(record.context, count, end_of_stream)
            });
            self.response_out(resp);
            self.request_out(session.req_header_mut());
            self.stream().local = None;
            match action {
                Ok(Action::Pause) => return Err(unavailable(&pool.name, "paused a response")),
                Ok(_) => {}
                Err(e) => {
                    pool.check(record.slot, guard, &e);
                    return Err(failure(&pool.name, "failed in on_response_headers", e));
                }
            }
        }
        Ok(())
    }
}

/// Ends a context: `on_done`, then `on_log` when `log` is set, then `on_delete`.
///
/// Answers `false` when the guest holds the context.
pub(crate) fn finish<H: StreamState>(
    scope: &mut CallScope<'_, H>,
    context: ContextId,
    log: bool,
) -> std::result::Result<bool, GuestError> {
    if !scope.on_done(context)? {
        return Ok(false);
    }
    if log {
        scope.on_log(context)?;
    }
    scope.on_delete(context)?;
    Ok(true)
}

pub(crate) fn finished(
    pool: &GuestPool,
    slot: usize,
    guard: SlotGuard<'_>,
    result: std::result::Result<bool, GuestError>,
) {
    match result {
        Ok(true) => pool.deleted(slot),
        Ok(false) => {
            warn!(
                "wasm plugin {} holds a context after the request ended",
                pool.name
            );
            pool.held(slot);
        }
        Err(e) => {
            error!("wasm plugin {} failed to end a context: {e}", pool.name);
            pool.check(slot, guard, &e);
        }
    }
}

fn scheme_of<DS: DownstreamSession>(session: &Session<DS>) -> &'static str {
    match session.req_header().uri.scheme_str() {
        Some("https") => "https",
        Some("http") => "http",
        _ if session.digest().is_some_and(|d| d.ssl_digest.is_some()) => "https",
        _ => "http",
    }
}

fn skips_response(status: StatusCode) -> bool {
    status.is_informational() && status != StatusCode::SWITCHING_PROTOCOLS
}

fn response_ends(method: &Method, resp: &ResponseHeader) -> bool {
    *method == Method::HEAD
        || resp.status == StatusCode::NO_CONTENT
        || resp.status == StatusCode::NOT_MODIFIED
        || resp
            .headers
            .get(CONTENT_LENGTH)
            .is_some_and(|len| len.as_bytes() == b"0")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{fixture, plugin, session, wat_guest, Wat};
    use crate::{WasmPluginConf, WasmRuntime};
    use pingora_error::ErrorType;

    const GET: &[u8] = b"GET /original HTTP/1.1\r\nHost: example.test\r\n\r\n";

    fn runtime(conf: WasmPluginConf) -> (WasmRuntime, WasmCtx) {
        let runtime = WasmRuntime::new(vec![conf]).unwrap();
        let ctx = runtime.chain(&["a"]).unwrap().new_ctx();
        (runtime, ctx)
    }

    fn add() -> WasmPluginConf {
        plugin("a", fixture("add-request-header"), 1)
    }

    fn guest(label: &str, request_headers: &'static str) -> WasmPluginConf {
        let wat = Wat {
            request_headers,
            ..Wat::default()
        };
        plugin("a", wat_guest(label, wat), 1)
    }

    #[tokio::test]
    async fn a_response_on_a_replaced_guest_answers_503() {
        let (runtime, mut ctx) = runtime(add());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        runtime.inner.pools[0].replace_slot(0);
        let mut resp = ResponseHeader::build(200, None).unwrap();

        let err = ctx
            .response_filter(&mut session, &mut resp)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(503));
        assert!(err.to_string().contains("lost the guest of this request"));
    }

    #[tokio::test]
    async fn logging_skips_a_replaced_guest() {
        let (runtime, mut ctx) = runtime(add());
        let (mut other_session, _other_client) = session(GET).await;
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        runtime.inner.pools[0].replace_slot(0);
        let mut other = runtime.chain(&["a"]).unwrap().new_ctx();
        other.request_filter(&mut other_session).await.unwrap();
        let stale = ctx.records[0].unwrap().context;
        let live = other.records[0].unwrap().context;

        ctx.logging(&mut session).await;

        assert_eq!(stale, live);
        assert_eq!(runtime.open_contexts(), 1);
        let slot = runtime.inner.pools[0].lock_slot(0);
        assert!(slot.as_ref().unwrap().guest.context_state(live).is_some());
    }

    #[tokio::test]
    async fn a_ctx_finishes_on_its_runtime_after_a_swap() {
        let (old, mut ctx) = runtime(add());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        let weak = Arc::downgrade(&old.inner);
        let (new, _) = runtime(add());
        drop(old);

        ctx.logging(&mut session).await;

        let old = weak.upgrade().expect("the request keeps its runtime");
        assert_eq!(old.pools[0].open_contexts(), 0);
        assert_eq!(new.open_contexts(), 0);
    }

    #[tokio::test]
    async fn the_last_ctx_releases_an_old_runtime() {
        let (old, mut ctx) = runtime(add());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        ctx.logging(&mut session).await;
        let weak = Arc::downgrade(&old.inner);
        drop(old);

        drop(ctx);

        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn a_guest_error_restores_the_session_header() {
        let (_runtime, mut ctx) = runtime(guest("bad-unit", "i32.const 7"));
        let (mut session, _client) = session(GET).await;

        let err = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(503));
        assert_eq!(session.req_header().raw_path(), b"/original");
        assert_eq!(session.req_header().headers["host"], "example.test");
    }

    #[tokio::test]
    async fn logging_closes_a_context_after_a_guest_error() {
        let (runtime, mut ctx) = runtime(guest("bad-log", "i32.const 7"));
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap_err();
        let open = runtime.open_contexts();

        ctx.logging(&mut session).await;

        assert_eq!(open, 1);
        assert_eq!(runtime.open_contexts(), 0);
    }

    #[tokio::test]
    async fn a_trap_replaces_the_guest_and_resets_its_counts() {
        let (runtime, mut ctx) = runtime(guest("trap-unit", "unreachable"));
        let (mut session, _client) = session(GET).await;

        let err = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(503));
        assert_eq!(runtime.open_contexts(), 0);
        let slot = runtime.inner.pools[0].lock_slot(0);
        assert!(slot.as_ref().unwrap().guest.is_serving());
    }

    fn response(status: u16, length: Option<&str>) -> ResponseHeader {
        let mut resp = ResponseHeader::build(status, None).unwrap();
        if let Some(length) = length {
            resp.insert_header(CONTENT_LENGTH, length).unwrap();
        }
        resp
    }

    #[test]
    fn skips_response_for_informational_other_than_101() {
        let statuses = [100, 101, 103, 199, 200, 404];

        let skipped: Vec<_> = statuses
            .iter()
            .map(|s| skips_response(StatusCode::from_u16(*s).unwrap()))
            .collect();

        assert_eq!(skipped, [true, false, true, true, false, false]);
    }

    #[test]
    fn response_ends_follows_the_rule() {
        let cases = [
            (Method::GET, response(200, None), false),
            (Method::GET, response(200, Some("10")), false),
            (Method::GET, response(200, Some("0")), true),
            (Method::GET, response(204, None), true),
            (Method::GET, response(304, None), true),
            (Method::HEAD, response(200, Some("10")), true),
        ];

        let ends: Vec<_> = cases
            .iter()
            .map(|(method, resp, _)| response_ends(method, resp))
            .collect();

        let want: Vec<_> = cases.iter().map(|c| c.2).collect();
        assert_eq!(ends, want);
    }
}
