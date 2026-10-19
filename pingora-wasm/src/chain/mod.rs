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

//! Plugin chain and its per-request filters

mod body;
mod ctx;
mod failure;
mod logging;
mod request;
mod respond;
mod response;
mod slot;
mod tcp;
mod wait;

pub use ctx::WasmCtx;
pub use respond::write_plugin_response;
pub use tcp::{WasmTcpConnection, WasmTcpProxy, WasmTcpUpstream};

use ctx::ResponseProgress;

use crate::runtime::pool::PluginPhases;
use crate::runtime::RuntimeInner;
use bytes::Bytes;
use pingora_http::ResponseHeader;
use std::fmt;
use std::sync::Arc;

/// An ordered list of plugins from one [WasmRuntime](crate::WasmRuntime).
///
/// Build a chain with [WasmRuntime::chain](crate::WasmRuntime::chain). Cloning is cheap, so you
/// can keep a copy wherever requests are handled. Call [WasmChain::new_ctx] once per request.
#[derive(Clone)]
pub struct WasmChain {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) plugins: Arc<[usize]>,
    phases: ChainPhases,
}

/// The body and trailer phases at least one plugin in the chain runs in.
#[derive(Debug, Clone, Copy)]
struct ChainPhases {
    request_body: bool,
    response_body: bool,
    response_trailers: bool,
}

/// What to do with a request after [WasmCtx::request_filter].
///
/// A plugin can respond to a request itself, for example to deny it with 403. From
/// [WasmCtx::request_filter] you get that response as [RequestOutcome::Respond], and you write
/// it to the downstream. [WasmCtx::request_body_filter] and [WasmCtx::response_filter] write the
/// response for you, and then return an error with the status of the response to stop the
/// request. After that error, [WasmCtx::plugin_responded] returns `true`.
#[derive(Debug)]
#[non_exhaustive]
pub enum RequestOutcome {
    /// No plugin stopped the request, so it should be proxied to the upstream.
    Continue,
    /// A plugin sent its own response, given here as a header and a body.
    ///
    /// Write it to the downstream, e.g. with [write_plugin_response], and return `Ok(true)` from
    /// your `request_filter`.
    Respond(Box<ResponseHeader>, Bytes),
}

impl WasmChain {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, plugins: Vec<usize>) -> Self {
        let any = |phase: fn(&PluginPhases) -> bool| {
            plugins
                .iter()
                .any(|index| phase(&runtime.pools[*index].phases))
        };
        let phases = ChainPhases {
            request_body: any(|conf| conf.request),
            response_body: any(|conf| conf.response),
            response_trailers: any(|conf| conf.trailers),
        };
        WasmChain {
            runtime,
            plugins: plugins.into(),
            phases,
        }
    }

    /// Create the per-request state for this chain.
    ///
    /// Call this from `new_ctx` and keep the result in your proxy's `CTX`.
    pub fn new_ctx(&self) -> WasmCtx {
        WasmCtx::new(self.clone())
    }

    fn plugin_names(&self) -> Vec<&str> {
        self.plugins
            .iter()
            .map(|index| &*self.runtime.pools[*index].name)
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
