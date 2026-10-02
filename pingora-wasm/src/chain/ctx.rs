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

use super::body::{HeldBodies, RequestBodyState};
use super::logging::finish;
use super::slot::LockedSlot;
use super::WasmChain;
use crate::callout::RequestCallouts;
use crate::plugin_unavailable;
use crate::properties::WasmPropertyValue;
use crate::runtime::pool::{GuestPool, Loaded};
use crate::stream_state::{PingoraStream, RequestHeaders, ResponseHeaders};
use http::uri::Scheme;
use http::{Method, StatusCode};
use pingora_core::upstreams::peer::{HttpPeer, Peer};
use pingora_error::Error;
use pingora_http::{RequestHeader, ResponseHeader};
use proxy_wasm_host::abi::v0_2_1::{CallScope, ContextId, Guest, GuestId};
use proxy_wasm_host::HeaderMap;
use std::fmt;
use std::mem;

/// Where one plugin's context for a request is, given as its slot, guest, and context id.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PluginRecord {
    pub(crate) slot: usize,
    pub(crate) guest: GuestId,
    pub(crate) context: ContextId,
}

/// Whether plugins have run on a response header yet, and where that response came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResponseProgress {
    /// No response header has been run through the plugins.
    NotStarted,
    /// The plugins ran on the upstream response header.
    FromUpstream,
    /// A plugin sent its own response.
    FromPlugin,
}

/// Per-request state for the plugins of one chain.
///
/// Create it with [WasmChain::new_ctx] and keep it in your proxy's `CTX`. It holds a reference to
/// its chain and runtime, so a request finishes on the runtime it started on.
///
/// Call [WasmCtx::logging] for every `WasmCtx` you create, so that each plugin sees the end of
/// its request. If the request task ends before `logging`, dropping the `WasmCtx` ends each open
/// context without `proxy_on_log`.
pub struct WasmCtx {
    pub(crate) chain: WasmChain,
    pub(crate) records: Vec<Option<PluginRecord>>,
    pub(crate) scheme: Scheme,
    pub(super) response_progress: ResponseProgress,
    pub(super) request_body: RequestBodyState,
    pub(super) held: HeldBodies,
    pub(super) callouts: RequestCallouts,
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
        let stream = PingoraStream::new(chain.runtime.fixed_properties.clone());
        WasmCtx {
            response_progress: ResponseProgress::NotStarted,
            request_body: RequestBodyState::new(),
            held: HeldBodies::default(),
            callouts: RequestCallouts::default(),
            chain,
            records,
            scheme: Scheme::HTTP,
            stream,
            spare_request: None,
            spare_response: None,
        }
    }

    /// Run `body` on the guest, lending it this request's stream state for the call.
    pub(crate) fn run<R>(
        &mut self,
        guest: &mut Guest,
        body: impl FnOnce(&mut CallScope<'_, PingoraStream>) -> R,
    ) -> R {
        let (result, stream) = guest.with(mem::take(&mut self.stream), body);
        self.stream = stream;
        result
    }

    /// Run `body` on the guest on behalf of `context`, one plugin's context for this request.
    ///
    /// Only callouts sent from `context` are accepted during the call. They are kept on the
    /// request until the phase starts them, and are discarded by the next call if it does not.
    pub(crate) fn run_for_context<R>(
        &mut self,
        loaded: &mut Loaded,
        context: ContextId,
        body: impl FnOnce(&mut CallScope<'_, PingoraStream>) -> R,
    ) -> R {
        self.stream.clear_continue_requests();
        let service = loaded.callout_service.clone();
        let guest_call = || self.run(&mut loaded.guest, body);
        let (result, accepted) = service.record_callouts(context, guest_call);
        self.callouts.set_accepted(accepted);
        loaded.report_to_root_callbacks();
        result
    }

    /// Set a property on this request for plugins to read with `proxy_get_property`.
    ///
    /// Use this for anything only your proxy knows, e.g. the name of the route it picked. A
    /// property has to be set before the phase a plugin reads it in. Setting the same path again
    /// replaces the value, and plugins cannot overwrite it.
    pub fn set_property(&mut self, path: &[&str], value: impl Into<WasmPropertyValue>) {
        self.stream.proxy_properties.insert(path, value);
    }

    /// Return a property that a plugin set on this request with `proxy_set_property`.
    ///
    /// Returns `None` if no plugin has set a value at `path`.
    pub fn guest_property(&self, path: &[&str]) -> Option<&[u8]> {
        self.stream.guest_properties.get(path)
    }

    /// Record the upstream peer for the `upstream.address` and `upstream.port` properties.
    ///
    /// Call this from your `connected_to_upstream`. No plugin is run. Calling it again, e.g. after
    /// a retry, replaces the recorded peer. A peer without an IP address, such as a Unix socket,
    /// clears both properties.
    pub fn upstream_connected(&mut self, peer: &HttpPeer) {
        self.stream.request_facts.upstream_address = peer.address().as_inet().copied();
    }

    /// Return the guest pool of the plugin at `position` in the chain.
    pub(crate) fn pool_at(&self, position: usize) -> &GuestPool {
        &self.chain.runtime.pools[self.chain.plugins[position]]
    }

    /// Build an [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) error for the plugin at `position`.
    ///
    /// `what` is the part of the message after the plugin name.
    pub(crate) fn plugin_error(&self, position: usize, what: &str) -> Box<Error> {
        plugin_unavailable(&self.pool_at(position).name, what)
    }

    /// Move the session's request header into the stream state for the duration of a callback.
    ///
    /// The session is left with a placeholder header until [Self::request_out] is called.
    pub(crate) fn request_in(&mut self, header: &mut RequestHeader) {
        let spare = self
            .spare_request
            .take()
            .unwrap_or_else(placeholder_request);
        let request = mem::replace(header, spare);
        self.stream.request = Some(RequestHeaders::new(request, self.scheme.clone()));
    }

    /// Move the request header back into the session, keeping the placeholder for reuse.
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
        self.callouts.clear();
        for position in (0..self.records.len()).rev() {
            let Some(record) = self.records[position].take() else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            let Ok(mut locked) = LockedSlot::of_request(pool, &record) else {
                continue;
            };
            let Ok(loaded) = locked.loaded() else {
                continue;
            };
            let result = self.run_for_context(loaded, record.context, |scope| {
                finish(scope, record.context, false)
            });
            self.end_or_hold_context(position, locked, record.context, result, false);
        }
    }
}

fn placeholder_request() -> RequestHeader {
    RequestHeader::build(Method::GET, b"/", Some(0)).expect("static request line should be valid")
}

fn placeholder_response() -> ResponseHeader {
    ResponseHeader::build(StatusCode::OK, Some(0)).expect("static status should be valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        add_request_header, fixture, one_plugin, plugin, session, wat_guest, Wat, GET,
    };
    use crate::WasmRuntime;
    use proxy_wasm_host::abi::v0_2_1::{Invocation, StreamState};
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
    fn drop_ends_open_context() {
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
    fn request_out_restores_session_header() {
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
    async fn ctx_finishes_on_original_runtime_after_reload() {
        let (old, mut ctx) = one_plugin(add_request_header());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        let weak = Arc::downgrade(&old.inner);
        let (new, _) = one_plugin(add_request_header());
        drop(old);

        ctx.logging(&mut session).await;

        let old = weak
            .upgrade()
            .expect("request should keep its runtime alive");
        assert_eq!(old.pools[0].open_contexts(), 0);
        assert_eq!(new.open_contexts(), 0);
    }

    #[tokio::test]
    async fn last_ctx_releases_old_runtime() {
        let (old, mut ctx) = one_plugin(add_request_header());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        ctx.logging(&mut session).await;
        let weak = Arc::downgrade(&old.inner);
        drop(old);

        drop(ctx);

        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn later_upstream_replaces_upstream_properties() {
        let (_runtime, mut ctx) = one_plugin(add_request_header());
        let first = HttpPeer::new("10.0.0.1:8080", false, String::new());
        let retry = HttpPeer::new("10.0.0.2:9090", false, String::new());
        ctx.upstream_connected(&first);

        ctx.upstream_connected(&retry);

        let call = Invocation::new(GuestId::next(), ContextId::try_from(1).unwrap());
        let mut address = Vec::new();
        let mut port = Vec::new();
        let address_path: [&[u8]; 2] = [b"upstream", b"address"];
        let port_path: [&[u8]; 2] = [b"upstream", b"port"];
        ctx.stream()
            .property(call, &address_path, &mut address)
            .unwrap();
        ctx.stream().property(call, &port_path, &mut port).unwrap();
        assert_eq!(address, b"10.0.0.2:9090");
        assert_eq!(port, 9090_i64.to_le_bytes());
    }
}
