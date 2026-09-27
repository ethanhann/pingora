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

//! Run [Proxy-Wasm](https://github.com/proxy-wasm/spec) plugins in a Pingora proxy.
//!
//! A [WasmRuntime] compiles your plugins once, before the server starts. A [WasmChain] is an
//! ordered list of those plugins. Build one chain for all traffic, or one chain for each route.
//! For each request, create a [WasmCtx] from a chain and call its phases from your `ProxyHttp`
//! filters:
//!
//! - [WasmCtx::request_filter] from `request_filter`, after the checks your proxy runs itself
//! - [WasmCtx::response_filter] from `response_filter`
//! - [WasmCtx::logging] from `logging`, for every request that created a `WasmCtx`
//!
//! When a plugin sends its own response, [WasmCtx::request_filter] returns
//! [RequestOutcome::Respond] and the request does not go to the upstream. When a plugin fails,
//! a phase returns an error of type [ERR_PLUGIN_FAILED], and Pingora responds with 503.
//!
//! ```no_run
//! use async_trait::async_trait;
//! use pingora_core::upstreams::peer::HttpPeer;
//! use pingora_core::{Error, Result};
//! use pingora_http::ResponseHeader;
//! use pingora_proxy::{ProxyHttp, Session};
//! use pingora_wasm::{
//!     write_plugin_response, RequestOutcome, WasmChain, WasmCtx, WasmPluginConf, WasmRuntime,
//! };
//!
//! struct MyProxy {
//!     chain: WasmChain,
//! }
//!
//! #[async_trait]
//! impl ProxyHttp for MyProxy {
//!     type CTX = WasmCtx;
//!
//!     fn new_ctx(&self) -> WasmCtx {
//!         self.chain.new_ctx()
//!     }
//!
//!     async fn request_filter(&self, session: &mut Session, ctx: &mut WasmCtx) -> Result<bool> {
//!         match ctx.request_filter(session).await? {
//!             RequestOutcome::Respond(header, body) => {
//!                 write_plugin_response(session, header, body).await?;
//!                 Ok(true)
//!             }
//!             _ => Ok(false),
//!         }
//!     }
//!
//!     async fn upstream_peer(
//!         &self,
//!         _session: &mut Session,
//!         _ctx: &mut WasmCtx,
//!     ) -> Result<Box<HttpPeer>> {
//!         Ok(Box::new(HttpPeer::new(("127.0.0.1", 8080), false, String::new())))
//!     }
//!
//!     async fn response_filter(
//!         &self,
//!         session: &mut Session,
//!         upstream_response: &mut ResponseHeader,
//!         ctx: &mut WasmCtx,
//!     ) -> Result<()> {
//!         ctx.response_filter(session, upstream_response).await
//!     }
//!
//!     async fn logging(&self, session: &mut Session, _e: Option<&Error>, ctx: &mut WasmCtx) {
//!         ctx.logging(session).await
//!     }
//! }
//!
//! # fn main() -> Result<()> {
//! let runtime = WasmRuntime::new(vec![WasmPluginConf::new("auth", "auth.wasm")])?;
//! let proxy = MyProxy {
//!     chain: runtime.chain(&["auth"])?,
//! };
//! # Ok(())
//! # }
//! ```

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

/// The error type that a phase returns when a plugin fails.
///
/// The default `fail_to_proxy` of `ProxyHttp` responds with 503 for this type. Check for it in
/// your own `fail_to_proxy` to send a different response.
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
