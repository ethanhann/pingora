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

//! HTTP callouts from plugins.
//!
//! A plugin sends a callout with `proxy_http_call`. The callout service of its guest accepts
//! the callout, and a task sends it to a peer. The phase that ran the plugin delivers the result
//! to the plugin, and the root callback thread delivers the result of a callout that a root
//! context sent.

mod client;
pub(crate) mod headers;
mod request_callouts;
mod result;
mod service;
mod upstreams;

pub(crate) use client::{CalloutSender, ConnectorSender};
pub(crate) use request_callouts::{PendingResult, RequestCallouts};
pub(crate) use result::CalloutResult;
pub(crate) use service::GuestCalloutService;
pub use upstreams::{CalloutTarget, CalloutUpstreams, StaticCalloutUpstreams};

use bytes::Bytes;
use log::warn;
use pingora_http::RequestHeader;
use proxy_wasm_host::abi::v0_2_1::CalloutId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The callout settings of one plugin. All guests of the plugin share them.
pub(crate) struct PluginCalloutConf {
    pub(crate) plugin_name: String,
    pub(crate) upstreams: Arc<dyn CalloutUpstreams>,
    pub(crate) timeout_limit: Duration,
    pub(crate) response_limit: usize,
    timeout_warning_logged: AtomicBool,
    overflow_warning_logged: AtomicBool,
}

impl PluginCalloutConf {
    pub(crate) fn new(
        plugin_name: &str,
        upstreams: Arc<dyn CalloutUpstreams>,
        timeout_limit: Duration,
        response_limit: usize,
    ) -> Self {
        PluginCalloutConf {
            plugin_name: plugin_name.to_string(),
            upstreams,
            timeout_limit,
            response_limit,
            timeout_warning_logged: AtomicBool::new(false),
            overflow_warning_logged: AtomicBool::new(false),
        }
    }

    /// Return the timeout to use for a callout, given the timeout that the plugin passed.
    pub(crate) fn effective_timeout(&self, passed: Duration) -> Duration {
        if !passed.is_zero() && passed <= self.timeout_limit {
            return passed;
        }
        if !self.timeout_warning_logged.swap(true, Ordering::Relaxed) {
            warn!(
                "wasm plugin {} passed a callout timeout of {passed:?}, so its callout_timeout_limit of {:?} applies",
                self.plugin_name, self.timeout_limit
            );
        }
        self.timeout_limit
    }

    /// Warn once that the plugin sent a callout over the limit of the runtime.
    pub(crate) fn warn_of_overflow_once(&self) {
        if !self.overflow_warning_logged.swap(true, Ordering::Relaxed) {
            warn!(
                "wasm plugin {} sent a callout over max_callouts_in_flight, and receives a 503 response for it",
                self.plugin_name
            );
        }
    }
}

/// A callout that a plugin sent with `proxy_http_call` and that no task has started yet.
pub(crate) struct AcceptedCallout {
    pub(crate) id: CalloutId,
    pub(crate) plugin_conf: Arc<PluginCalloutConf>,
    pub(crate) upstream: String,
    pub(crate) request: Box<RequestHeader>,
    pub(crate) body: Bytes,
    pub(crate) timeout: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: Duration = Duration::from_secs(10);

    #[test]
    fn the_limit_replaces_a_timeout_that_is_zero_or_longer() {
        let upstreams = Arc::new(StaticCalloutUpstreams::new());
        let conf = PluginCalloutConf::new("a", upstreams, LIMIT, 1024);
        let negative = Duration::from_millis(u64::from(u32::MAX));
        let cases = [
            (Duration::from_secs(1), Duration::from_secs(1)),
            (LIMIT, LIMIT),
            (Duration::from_secs(30), LIMIT),
            (Duration::ZERO, LIMIT),
            (negative, LIMIT),
        ];

        let got = cases.map(|(passed, _)| conf.effective_timeout(passed));

        assert_eq!(got, cases.map(|(_, timeout)| timeout));
    }
}
