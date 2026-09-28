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

//! A chain of plugins and the phases that run them on each request.

mod body;
mod ctx;
mod logging;
mod request;
mod respond;
mod response;
mod slot;
mod wait;

pub use ctx::WasmCtx;

use ctx::ResponseProgress;

use crate::runtime::pool::PluginPhases;
use crate::runtime::RuntimeInner;
use bytes::Bytes;
use pingora_http::ResponseHeader;
use std::fmt;
use std::sync::Arc;

/// An ordered list of plugins from one [WasmRuntime](crate::WasmRuntime).
///
/// Build it with [WasmRuntime::chain](crate::WasmRuntime::chain) and clone it where you need
/// it. Create a [WasmCtx] from it for each request.
#[derive(Clone)]
pub struct WasmChain {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) plugins: Arc<[usize]>,
    phases: ChainPhases,
}

/// The body and trailer phases that the plugins of a chain run.
#[derive(Debug, Clone, Copy)]
struct ChainPhases {
    request_body: bool,
    response_body: bool,
    response_trailers: bool,
}

/// The result of [WasmCtx::request_filter].
#[derive(Debug)]
#[non_exhaustive]
pub enum RequestOutcome {
    /// Every plugin let the request continue to the upstream.
    Continue,
    /// A plugin sent its own response. Write it to the downstream, for example with
    /// [write_plugin_response](crate::write_plugin_response), and return `Ok(true)` from
    /// `request_filter`.
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

    /// Create the state of one request. Keep it in the `CTX` of your proxy.
    pub fn new_ctx(&self) -> WasmCtx {
        WasmCtx::new(self.clone())
    }

    fn plugin_names(&self) -> Vec<&str> {
        self.plugins
            .iter()
            .map(|index| self.runtime.pools[*index].name.as_str())
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
