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

use crate::properties::{join_path, WasmProperties};
use log::warn;
use proxy_wasm_host::abi::v0_2_1::types::{BufferType, MapType, Status, StreamType};
use proxy_wasm_host::abi::v0_2_1::{Access, Invocation, LocalResponse, StreamState};
use proxy_wasm_host::{Buffer, HeaderMap, VecHeaderMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// The settings of a plugin that its root callbacks use.
pub(crate) struct RootCallbackConf {
    pub(crate) plugin_name: String,
    pub(crate) fixed_properties: Arc<WasmProperties>,
    request_change_warning_logged: AtomicBool,
}

impl RootCallbackConf {
    pub(crate) fn new(plugin_name: &str, fixed_properties: Arc<WasmProperties>) -> Self {
        RootCallbackConf {
            plugin_name: plugin_name.to_string(),
            fixed_properties,
            request_change_warning_logged: AtomicBool::new(false),
        }
    }

    fn warn_of_request_change_once(&self, what: &str) {
        if !self
            .request_change_warning_logged
            .swap(true, Ordering::Relaxed)
        {
            warn!(
                "wasm plugin {} called {what} outside a request phase, which has no effect",
                self.plugin_name
            );
        }
    }
}

/// The stream state of a callback that runs with no request: a root callback, or the end of a
/// context that the guest held after its request.
///
/// A plugin reads empty header maps and the fixed properties.
pub(crate) struct RootStream {
    conf: Arc<RootCallbackConf>,
    empty: VecHeaderMap,
    path_key: Vec<u8>,
}

impl RootStream {
    pub(crate) fn new(conf: Arc<RootCallbackConf>) -> Self {
        RootStream {
            conf,
            empty: VecHeaderMap::default(),
            path_key: Vec::new(),
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
        Ok(&mut self.empty)
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
        self.conf
            .warn_of_request_change_once("proxy_continue_stream");
        Ok(())
    }

    fn send_local_response(
        &mut self,
        _call: Invocation,
        _response: LocalResponse<'_>,
    ) -> Result<(), Status> {
        self.conf
            .warn_of_request_change_once("proxy_send_local_response");
        Ok(())
    }

    fn property(
        &mut self,
        _call: Invocation,
        path: &[&[u8]],
        out: &mut Vec<u8>,
    ) -> Result<(), Status> {
        join_path(path.iter().copied(), &mut self.path_key);
        match self.conf.fixed_properties.get_joined(&self.path_key) {
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

    fn call() -> Invocation {
        Invocation::new(GuestId::next(), ContextId::try_from(1).unwrap())
            .with_callback(Callback::Tick)
    }

    fn root_stream(plugin: &str, fixed: WasmProperties) -> RootStream {
        RootStream::new(Arc::new(RootCallbackConf::new(plugin, Arc::new(fixed))))
    }

    #[test]
    fn a_header_map_reads_as_empty_and_refuses_a_write() {
        let mut stream = root_stream("maps", WasmProperties::new());

        let read = stream
            .header_map(call(), Access::Read, MapType::HttpRequestHeaders)
            .map(|map| map.len());
        let write = stream
            .header_map(call(), Access::Write, MapType::HttpRequestHeaders)
            .map(|map| map.len());

        assert_eq!(read, Ok(0));
        assert_eq!(write, Err(Status::NotFound));
    }

    #[test]
    fn a_fixed_property_reads_and_others_are_not_found() {
        let mut fixed = WasmProperties::new();
        fixed.insert(&["node", "name"], "edge-1");
        let mut stream = root_stream("fixed", fixed);
        let mut value = Vec::new();

        let found = stream.property(call(), &[b"node", b"name"], &mut value);
        let missing = stream.property(call(), &[b"request", b"path"], &mut Vec::new());

        assert_eq!(found, Ok(()));
        assert_eq!(value, b"edge-1");
        assert_eq!(missing, Err(Status::NotFound));
    }

    #[test]
    fn a_continue_returns_ok_and_warns_once_for_each_plugin() {
        record_crate_logs();
        let mut stream = root_stream("continue-from-tick", WasmProperties::new());

        let first = stream.continue_stream(call(), StreamType::HttpRequest);
        let second = stream.continue_stream(call(), StreamType::HttpRequest);

        assert_eq!((first, second), (Ok(()), Ok(())));
        let warnings = crate_log_lines_with("continue-from-tick called proxy_continue_stream");
        assert_eq!(warnings.len(), 1);
    }
}
