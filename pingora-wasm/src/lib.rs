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

//! Proxy-Wasm plugins for Pingora proxies.
//!
//! A [WasmRuntime] compiles and runs a set of plugins, and a [WasmChain] is an ordered list of
//! them. For each request, a proxy creates a [WasmCtx] from a chain and calls its phases from
//! its own `ProxyHttp` hooks, at the place in the request it chooses.

mod chain;
mod runtime;
mod stream;
#[cfg(test)]
mod test_support;

pub use chain::{RequestOutcome, WasmChain, WasmCtx};
pub use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
pub use proxy_wasm_host::abi::v0_2_1::{LogContext, LogSink};
pub use proxy_wasm_host::Limits;
pub use runtime::{WasmPluginConf, WasmRuntime};
pub use stream::write_plugin_response;

use http::StatusCode;
use pingora_error::{Error, ErrorType};
use proxy_wasm_host::abi::v0_2_1::GuestError;

/// The error type of a phase that fails because of a plugin. Pingora answers it with 503.
pub const ERR_PLUGIN_FAILED: ErrorType =
    ErrorType::HTTPStatus(StatusCode::SERVICE_UNAVAILABLE.as_u16());

pub(crate) fn plugin_failure(name: &str, what: &str, cause: GuestError) -> Box<Error> {
    Error::because(
        ERR_PLUGIN_FAILED,
        format!("wasm plugin {name} {what}"),
        cause,
    )
}

pub(crate) fn plugin_unavailable(name: &str, what: &str) -> Box<Error> {
    Error::explain(ERR_PLUGIN_FAILED, format!("wasm plugin {name} {what}"))
}
