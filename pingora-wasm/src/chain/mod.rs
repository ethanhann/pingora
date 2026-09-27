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

//! The work done for each request: a chain of plugins and the phases that run them.

mod ctx;
mod logging;
mod request;
mod response;

pub use ctx::WasmCtx;

use crate::runtime::RuntimeInner;
use bytes::Bytes;
use pingora_http::ResponseHeader;
use std::fmt;
use std::sync::Arc;

/// An ordered list of plugins from one [WasmRuntime](crate::WasmRuntime).
///
/// The request phase runs the plugins in order, and the response phase runs them in reverse.
#[derive(Clone)]
pub struct WasmChain {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) plugins: Arc<[usize]>,
}

/// What a proxy does after the request phase of a chain.
#[derive(Debug)]
#[non_exhaustive]
pub enum RequestOutcome {
    /// Every plugin let the request continue.
    Continue,
    /// A plugin answered the request. Write this response and end the request.
    Respond(Box<ResponseHeader>, Bytes),
}

impl WasmChain {
    pub(crate) fn new(runtime: Arc<RuntimeInner>, plugins: Vec<usize>) -> Self {
        WasmChain {
            runtime,
            plugins: plugins.into(),
        }
    }

    /// The state of one request in this chain.
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
