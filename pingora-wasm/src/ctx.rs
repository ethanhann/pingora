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

use crate::chain::{finish, finished, WasmChain};
use crate::headers::{RequestHeaders, ResponseHeaders};
use crate::stream::PingoraStream;
use pingora_http::{RequestHeader, ResponseHeader};
use proxy_wasm_host::abi::v0_2_1::{CallScope, ContextId, Guest, GuestId};
use proxy_wasm_host::HeaderMap;
use std::fmt;
use std::mem;

/// Where one plugin keeps the context of one request.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PluginRecord {
    pub(crate) slot: usize,
    pub(crate) guest: GuestId,
    pub(crate) context: ContextId,
}

/// The state of one request in each plugin of one chain.
///
/// Create it with [WasmChain::new_ctx] and keep it in the context of your proxy. Call
/// [WasmCtx::logging] for every `WasmCtx` you create, so each plugin sees the end of the
/// request.
pub struct WasmCtx {
    pub(crate) chain: WasmChain,
    pub(crate) records: Vec<Option<PluginRecord>>,
    pub(crate) scheme: &'static str,
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
            chain,
            records,
            scheme: "http",
            stream: PingoraStream::default(),
            spare_request: None,
            spare_response: None,
        }
    }

    /// Runs `body` with the guest while the guest holds the stream state.
    pub(crate) fn run<R>(
        &mut self,
        guest: &mut Guest,
        body: impl FnOnce(&mut CallScope<'_, PingoraStream>) -> R,
    ) -> R {
        let (result, stream) = guest.with(mem::take(&mut self.stream), body);
        self.stream = stream;
        result
    }

    /// Moves the session request header in for the guest, and a placeholder into the session.
    pub(crate) fn request_in(&mut self, header: &mut RequestHeader) {
        let spare = self
            .spare_request
            .take()
            .unwrap_or_else(placeholder_request);
        let request = mem::replace(header, spare);
        self.stream.request = Some(RequestHeaders::new(request, self.scheme));
    }

    /// Moves the request header back into the session.
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
            let Some(mut guard) = pool.lock(record.slot, record.guest) else {
                continue;
            };
            let Some(loaded) = guard.as_mut() else {
                continue;
            };
            let result = self.run(&mut loaded.guest, |scope| {
                finish(scope, record.context, false)
            });
            finished(pool, record.slot, guard, result);
        }
    }
}

fn placeholder_request() -> RequestHeader {
    RequestHeader::build("GET", b"/", Some(0)).expect("a static request line is valid")
}

fn placeholder_response() -> ResponseHeader {
    ResponseHeader::build(200, Some(0)).expect("a static status is valid")
}
