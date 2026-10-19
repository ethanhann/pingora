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

//! Stream state for callbacks outside of a request

use crate::properties::built_in::{write_built_in_property, ReadableHeaders, RequestFacts};
use crate::properties::{join_path, WasmProperties};
use crate::WasmForeignFunctions;
use log::warn;
use parking_lot::Mutex;
use proxy_wasm_host::abi::v0_2_1::types::{BufferType, MapType, Status, StreamType};
use proxy_wasm_host::abi::v0_2_1::{
    Access, ContextId, ForeignCall, GuestId, Invocation, LocalResponse, StreamState,
};
use proxy_wasm_host::{Buffer, HeaderMap, VecHeaderMap};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Per-plugin state shared by every [RootStream] of that plugin.
pub(crate) struct RootCallbackPluginState {
    pub(crate) plugin_name: String,
    pub(crate) fixed_properties: Arc<WasmProperties>,
    foreign_functions: Arc<WasmForeignFunctions>,
    request_change_warning_logged: AtomicBool,
    /// Facts of the open TCP connections, for a root callback whose effective context is one
    /// of their contexts.
    tcp_connections: Mutex<HashMap<(GuestId, ContextId), Arc<RequestFacts>>>,
}

impl RootCallbackPluginState {
    pub(crate) fn new(
        plugin_name: &str,
        fixed_properties: Arc<WasmProperties>,
        foreign_functions: Arc<WasmForeignFunctions>,
    ) -> Self {
        RootCallbackPluginState {
            plugin_name: plugin_name.to_string(),
            fixed_properties,
            foreign_functions,
            request_change_warning_logged: AtomicBool::new(false),
            tcp_connections: Mutex::default(),
        }
    }

    pub(crate) fn add_tcp_connection(
        &self,
        guest: GuestId,
        context: ContextId,
        facts: Arc<RequestFacts>,
    ) {
        self.tcp_connections.lock().insert((guest, context), facts);
    }

    pub(crate) fn remove_tcp_connection(&self, guest: GuestId, context: ContextId) {
        self.tcp_connections.lock().remove(&(guest, context));
    }

    fn tcp_connection(&self, call: Invocation) -> Option<Arc<RequestFacts>> {
        let connections = self.tcp_connections.lock();
        connections.get(&(call.guest, call.context)).cloned()
    }

    fn warn_of_request_change_once(&self, function_name: &str) {
        if !self
            .request_change_warning_logged
            .swap(true, Ordering::Relaxed)
        {
            warn!(
                "wasm plugin {}: {function_name} called outside of a request, no effect, further occurrences are not logged",
                self.plugin_name
            );
        }
    }
}

/// Stream state for a callback that runs outside of a request.
///
/// Used for root context callbacks and for ending a context the guest kept after its request.
pub(crate) struct RootStream {
    plugin: Arc<RootCallbackPluginState>,
    empty_header_map: VecHeaderMap,
    joined_path: Vec<u8>,
}

impl RootStream {
    pub(crate) fn new(plugin: Arc<RootCallbackPluginState>) -> Self {
        RootStream {
            plugin,
            empty_header_map: VecHeaderMap::default(),
            joined_path: Vec::new(),
        }
    }
}

impl StreamState for RootStream {
    fn header_map(
        &mut self,
        _call: Invocation,
        access: Access,
        _map: MapType,
    ) -> Result<&mut dyn HeaderMap, Status> {
        if access != Access::Read {
            return Err(Status::NotFound);
        }
        // An empty map rather than an error, because the Rust SDK panics on any status other
        // than `Ok` when it reads header pairs
        Ok(&mut self.empty_header_map)
    }

    fn buffer(
        &mut self,
        _call: Invocation,
        _access: Access,
        _buffer: BufferType,
    ) -> Result<&mut dyn Buffer, Status> {
        Err(Status::NotFound)
    }

    fn continue_stream(&mut self, _call: Invocation, _stream: StreamType) -> Result<(), Status> {
        self.plugin
            .warn_of_request_change_once("proxy_continue_stream");
        Ok(())
    }

    fn close_stream(&mut self, _call: Invocation, _stream: StreamType) -> Result<(), Status> {
        self.plugin
            .warn_of_request_change_once("proxy_close_stream");
        Ok(())
    }

    fn send_local_response(
        &mut self,
        _call: Invocation,
        _response: LocalResponse<'_>,
    ) -> Result<(), Status> {
        self.plugin
            .warn_of_request_change_once("proxy_send_local_response");
        Ok(())
    }

    fn property(
        &mut self,
        call: Invocation,
        path: &[&[u8]],
        out: &mut Vec<u8>,
    ) -> Result<(), Status> {
        join_path(path.iter().copied(), &mut self.joined_path);
        if let Some(facts) = self.plugin.tcp_connection(call) {
            let headers = ReadableHeaders {
                request: None,
                response: None,
            };
            if write_built_in_property(&self.joined_path, &facts, &headers, out) {
                return Ok(());
            }
        }
        match self.plugin.fixed_properties.get_joined(&self.joined_path) {
            Some(value) => {
                out.extend_from_slice(value);
                Ok(())
            }
            None => Err(Status::NotFound),
        }
    }

    fn call_foreign_function(
        &mut self,
        _call: Invocation,
        request: ForeignCall<'_>,
        out: &mut Vec<u8>,
    ) -> Result<(), Status> {
        let plugin = &self.plugin;
        let functions = &plugin.foreign_functions;
        functions.call(&plugin.plugin_name, &request.name, &request.arguments, out)
    }

    fn set_property(
        &mut self,
        _call: Invocation,
        _path: &[&[u8]],
        _value: &[u8],
    ) -> Result<(), Status> {
        Err(Status::NotFound)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{crate_log_lines_with, record_crate_logs};
    use proxy_wasm_host::abi::v0_2_1::{Callback, ContextId, GuestId};

    fn tick_invocation() -> Invocation {
        Invocation::new(GuestId::next(), ContextId::try_from(1).unwrap())
            .with_callback(Callback::Tick)
    }

    #[test]
    fn root_callback_calls_foreign_function() {
        let mut functions = WasmForeignFunctions::new();
        functions.insert("echo", |call| {
            Ok([call.plugin_name.as_bytes(), call.arguments].concat())
        });
        let plugin = RootCallbackPluginState::new("a", Arc::default(), Arc::new(functions));
        let mut stream = RootStream::new(Arc::new(plugin));
        let mut out = Vec::new();

        let called = stream.call_foreign_function(
            tick_invocation(),
            ForeignCall::new(&b"echo"[..], &b"-ping"[..]),
            &mut out,
        );

        assert_eq!(called, Ok(()));
        assert_eq!(out, b"a-ping");
    }

    fn root_stream(plugin_name: &str, fixed_properties: WasmProperties) -> RootStream {
        RootStream::new(Arc::new(RootCallbackPluginState::new(
            plugin_name,
            Arc::new(fixed_properties),
            Arc::default(),
        )))
    }

    #[test]
    fn header_map_reads_empty_and_rejects_writes() {
        let mut stream = root_stream("maps", WasmProperties::new());

        let read = stream
            .header_map(tick_invocation(), Access::Read, MapType::HttpRequestHeaders)
            .map(|map| map.len());
        let write = stream
            .header_map(
                tick_invocation(),
                Access::Write,
                MapType::HttpRequestHeaders,
            )
            .map(|map| map.len());

        assert_eq!(read, Ok(0));
        assert_eq!(write, Err(Status::NotFound));
    }

    #[test]
    fn property_resolves_fixed_paths_only() {
        let mut fixed = WasmProperties::new();
        fixed.insert(&["node", "name"], "edge-1");
        let mut stream = root_stream("fixed", fixed);
        let mut value = Vec::new();

        let found = stream.property(tick_invocation(), &[b"node", b"name"], &mut value);
        let missing = stream.property(tick_invocation(), &[b"request", b"path"], &mut Vec::new());

        assert_eq!(found, Ok(()));
        assert_eq!(value, b"edge-1");
        assert_eq!(missing, Err(Status::NotFound));
    }

    #[test]
    fn continue_stream_returns_ok_and_warns_once_per_plugin() {
        record_crate_logs();
        let mut stream = root_stream("continue-from-tick", WasmProperties::new());

        let first = stream.continue_stream(tick_invocation(), StreamType::HttpRequest);
        let second = stream.continue_stream(tick_invocation(), StreamType::HttpRequest);

        assert_eq!((first, second), (Ok(()), Ok(())));
        let warnings = crate_log_lines_with("wasm plugin continue-from-tick:");
        let want = "wasm plugin continue-from-tick: proxy_continue_stream called outside of a \
                    request, no effect, further occurrences are not logged";
        assert_eq!(warnings, [want]);
    }

    #[test]
    fn close_stream_returns_ok_and_warns() {
        record_crate_logs();
        let mut stream = root_stream("close-from-tick", WasmProperties::new());

        let closed = stream.close_stream(tick_invocation(), StreamType::Downstream);

        assert_eq!(closed, Ok(()));
        let warnings = crate_log_lines_with("wasm plugin close-from-tick:");
        let want = "wasm plugin close-from-tick: proxy_close_stream called outside of a \
                    request, no effect, further occurrences are not logged";
        assert_eq!(warnings, [want]);
    }
}
