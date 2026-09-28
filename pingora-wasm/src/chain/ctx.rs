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

use super::body::Held;
use super::failure::Locked;
use super::logging::{finish, finished};
use super::request_body::RequestBody;
use super::WasmChain;
use crate::stream::{PingoraStream, RequestHeaders, ResponseHeaders};
use http::uri::Scheme;
use http::{Method, StatusCode};
use pingora_http::{RequestHeader, ResponseHeader};
use proxy_wasm_host::abi::v0_2_1::{CallScope, ContextId, Guest, GuestId};
use proxy_wasm_host::HeaderMap;
use std::fmt;
use std::mem;

/// The location of the context of one plugin for one request: the slot, the guest, and the
/// context id.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PluginRecord {
    pub(crate) slot: usize,
    pub(crate) guest: GuestId,
    pub(crate) context: ContextId,
}

/// The progress of one request through the plugins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Exchange {
    /// The plugins did not run on a response header.
    Request,
    /// The plugins ran on the upstream response header.
    Response,
    /// A plugin sent its own response.
    Responded,
}

/// The state of one request in the plugins of one chain.
///
/// Create it with [WasmChain::new_ctx] and keep it in the `CTX` of your proxy. It holds a
/// reference to its chain and runtime, so a request finishes on the runtime it started on.
///
/// Call [WasmCtx::logging] for every `WasmCtx` you create, so that each plugin sees the end of
/// its request. If the request task ends before `logging`, dropping the `WasmCtx` ends each open
/// context without `proxy_on_log`.
pub struct WasmCtx {
    pub(crate) chain: WasmChain,
    pub(crate) records: Vec<Option<PluginRecord>>,
    pub(crate) scheme: Scheme,
    pub(super) exchange: Exchange,
    pub(super) request_body: RequestBody,
    pub(super) held: Held,
    stream: PingoraStream,
    spare_request: Option<RequestHeader>,
    spare_response: Option<ResponseHeader>,
}

impl fmt::Debug for WasmCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmCtx")
            .field("plugins", &self.chain.plugin_names())
            .field("records", &self.records)
            .field("scheme", &self.scheme)
            .finish()
    }
}

impl WasmCtx {
    pub(crate) fn new(chain: WasmChain) -> Self {
        let records = vec![None; chain.plugins.len()];
        WasmCtx {
            exchange: Exchange::Request,
            request_body: RequestBody::new(),
            held: Held::default(),
            chain,
            records,
            scheme: Scheme::HTTP,
            stream: PingoraStream::default(),
            spare_request: None,
            spare_response: None,
        }
    }

    /// Run `body` on the guest while the guest holds the stream state.
    pub(crate) fn run<R>(
        &mut self,
        guest: &mut Guest,
        body: impl FnOnce(&mut CallScope<'_, PingoraStream>) -> R,
    ) -> R {
        let (result, stream) = guest.with(mem::take(&mut self.stream), body);
        self.stream = stream;
        result
    }

    /// Move the request header from the session into the stream state for a callback, and put
    /// a placeholder in the session.
    pub(crate) fn request_in(&mut self, header: &mut RequestHeader) {
        let spare = self
            .spare_request
            .take()
            .unwrap_or_else(placeholder_request);
        let request = mem::replace(header, spare);
        self.stream.request = Some(RequestHeaders::new(request, self.scheme.clone()));
    }

    /// Move the request header back into the session.
    pub(crate) fn request_out(&mut self, header: &mut RequestHeader) {
        if let Some(request) = self.stream.request.take() {
            self.spare_request = Some(mem::replace(header, request.header));
        }
    }

    pub(crate) fn response_in(&mut self, header: &mut ResponseHeader) {
        let spare = self
            .spare_response
            .take()
            .unwrap_or_else(placeholder_response);
        let response = mem::replace(header, spare);
        self.stream.response = Some(ResponseHeaders::new(response));
    }

    pub(crate) fn response_out(&mut self, header: &mut ResponseHeader) {
        if let Some(response) = self.stream.response.take() {
            self.spare_response = Some(mem::replace(header, response.header));
        }
    }

    pub(crate) fn stream(&mut self) -> &mut PingoraStream {
        &mut self.stream
    }

    pub(crate) fn request_count(&self) -> u32 {
        let count = self.stream.request.as_ref().map_or(0, |r| r.len());
        u32::try_from(count).unwrap_or(u32::MAX)
    }

    pub(crate) fn response_count(&self) -> u32 {
        let count = self.stream.response.as_ref().map_or(0, |r| r.len());
        u32::try_from(count).unwrap_or(u32::MAX)
    }
}

impl Drop for WasmCtx {
    fn drop(&mut self) {
        let runtime = self.chain.runtime.clone();
        for position in (0..self.records.len()).rev() {
            let Some(record) = self.records[position].take() else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            let Ok(mut locked) = Locked::of(pool, &record) else {
                continue;
            };
            let Ok(loaded) = locked.loaded() else {
                continue;
            };
            let result = self.run(&mut loaded.guest, |scope| {
                finish(scope, record.context, false)
            });
            finished(locked, result);
        }
    }
}

fn placeholder_request() -> RequestHeader {
    RequestHeader::build(Method::GET, b"/", Some(0)).expect("a static request line is valid")
}

fn placeholder_response() -> ResponseHeader {
    ResponseHeader::build(StatusCode::OK, Some(0)).expect("a static status is valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        add_request_header, fixture, one_plugin, plugin, session, wat_guest, Wat, GET,
    };
    use crate::WasmRuntime;
    use std::sync::Arc;

    fn open_context(runtime: &WasmRuntime, ctx: &mut WasmCtx) {
        let pool = &runtime.inner.pools[0];
        let (slot, mut guard) = pool.pick().unwrap();
        let loaded = guard.as_mut().unwrap();
        let root = loaded.root;
        let guest = loaded.guest.id();
        let context = ctx
            .run(&mut loaded.guest, |scope| {
                scope.on_context_create(Some(root))
            })
            .unwrap();
        pool.opened(slot);
        ctx.records[0] = Some(PluginRecord {
            slot,
            guest,
            context,
        });
    }

    #[test]
    fn dropping_a_ctx_ends_its_open_context() {
        let held = Wat {
            done: "i32.const 0",
            ..Wat::default()
        };
        let cases = [
            (fixture("add-request-header"), 0),
            (wat_guest("held-unit", held), 1),
        ];

        for (path, held) in cases {
            let runtime = WasmRuntime::new(vec![plugin("a", path, 1)]).unwrap();
            let mut ctx = runtime.chain(&["a"]).unwrap().new_ctx();
            open_context(&runtime, &mut ctx);

            drop(ctx);

            assert_eq!(runtime.open_contexts(), 0);
            assert_eq!(runtime.held_contexts(), held);
        }
    }

    #[test]
    fn a_swap_returns_the_session_header() {
        let runtime =
            WasmRuntime::new(vec![plugin("a", fixture("add-request-header"), 1)]).unwrap();
        let mut ctx = runtime.chain(&["a"]).unwrap().new_ctx();
        let mut session_header =
            pingora_http::RequestHeader::build("POST", b"/original", None).unwrap();

        ctx.request_in(&mut session_header);
        let during = session_header.raw_path().to_vec();
        ctx.request_out(&mut session_header);

        assert_eq!(during, b"/");
        assert_eq!(session_header.method, http::Method::POST);
        assert_eq!(session_header.raw_path(), b"/original");
    }

    #[tokio::test]
    async fn a_ctx_finishes_on_its_runtime_after_a_swap() {
        let (old, mut ctx) = one_plugin(add_request_header());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        let weak = Arc::downgrade(&old.inner);
        let (new, _) = one_plugin(add_request_header());
        drop(old);

        ctx.logging(&mut session).await;

        let old = weak.upgrade().expect("the request keeps its runtime");
        assert_eq!(old.pools[0].open_contexts(), 0);
        assert_eq!(new.open_contexts(), 0);
    }

    #[tokio::test]
    async fn the_last_ctx_releases_an_old_runtime() {
        let (old, mut ctx) = one_plugin(add_request_header());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        ctx.logging(&mut session).await;
        let weak = Arc::downgrade(&old.inner);
        drop(old);

        drop(ctx);

        assert!(weak.upgrade().is_none());
    }
}
