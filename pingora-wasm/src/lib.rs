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
mod ctx;
mod headers;
mod local;
mod logging;
mod plugin;
mod pool;
mod runtime;
mod stream;
#[cfg(test)]
mod test_support;

pub use chain::{RequestOutcome, WasmChain};
pub use ctx::WasmCtx;
pub use local::write_local_response;
pub use plugin::WasmPluginConf;
pub use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
pub use proxy_wasm_host::abi::v0_2_1::{LogContext, LogSink};
pub use proxy_wasm_host::Limits;
pub use runtime::WasmRuntime;
