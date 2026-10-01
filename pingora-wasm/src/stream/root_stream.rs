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

//! The stream state of the callbacks that run with no request.

use crate::properties::{join_path, WasmProperties};
use log::warn;
use proxy_wasm_host::abi::v0_2_1::types::{BufferType, MapType, Status, StreamType};
use proxy_wasm_host::abi::v0_2_1::{Access, Invocation, LocalResponse, StreamState};
use proxy_wasm_host::{Buffer, HeaderMap, VecHeaderMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// The plugin data that the callbacks with no request use.
///
/// It holds the plugin name, the fixed properties, and whether the warning about a call that
/// tries to change a request was logged.
pub(crate) struct RootCallbackPluginState {
    pub(crate) plugin_name: String,
    pub(crate) fixed_properties: Arc<WasmProperties>,
    request_change_warning_logged: AtomicBool,
}

impl RootCallbackPluginState {
    pub(crate) fn new(plugin_name: &str, fixed_properties: Arc<WasmProperties>) -> Self {
        RootCallbackPluginState {
            plugin_name: plugin_name.to_string(),
            fixed_properties,
            request_change_warning_logged: AtomicBool::new(false),
        }
    }

    fn warn_of_request_change_once(&self, function_name: &str) {
        if !self
            .request_change_warning_logged
            .swap(true, Ordering::Relaxed)
        {
            warn!(
                "wasm plugin {} called {function_name} outside a request phase, which has no effect",
                self.plugin_name
            );
        }
    }
}

/// The stream state of a callback that runs with no request.
///
/// Such a callback is a root callback, or the end of a context that the guest held after its
/// request. A plugin reads empty header maps and the fixed properties.
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
    // A guest built with the Rust SDK panics on a status other than `Ok` from a read of the
    // pairs, so every map reads as empty, as in Envoy
    fn header_map(
        &mut self,
        _call: Invocation,
        access: Access,
        _map: MapType,
    ) -> Result<&mut dyn HeaderMap, Status> {
        if access != Access::Read {
            return Err(Status::NotFound);
        }
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
        _call: Invocation,
        path: &[&[u8]],
        out: &mut Vec<u8>,
    ) -> Result<(), Status> {
        join_path(path.iter().copied(), &mut self.joined_path);
        match self.plugin.fixed_properties.get_joined(&self.joined_path) {
            Some(value) => {
                out.extend_from_slice(value);
                Ok(())
            }
            None => Err(Status::NotFound),
        }
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

    fn root_stream(plugin_name: &str, fixed_properties: WasmProperties) -> RootStream {
        RootStream::new(Arc::new(RootCallbackPluginState::new(
            plugin_name,
            Arc::new(fixed_properties),
        )))
    }

    #[test]
    fn a_header_map_reads_as_empty_and_refuses_a_write() {
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
    fn a_fixed_property_is_found_and_another_path_is_not() {
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
    fn proxy_continue_stream_returns_ok_and_warns_once_for_each_plugin() {
        record_crate_logs();
        let mut stream = root_stream("continue-from-tick", WasmProperties::new());

        let first = stream.continue_stream(tick_invocation(), StreamType::HttpRequest);
        let second = stream.continue_stream(tick_invocation(), StreamType::HttpRequest);

        assert_eq!((first, second), (Ok(()), Ok(())));
        let warnings = crate_log_lines_with("continue-from-tick called proxy_continue_stream");
        assert_eq!(warnings.len(), 1);
    }
}
