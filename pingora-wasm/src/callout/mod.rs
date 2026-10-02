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

//! HTTP callouts from plugins
//!
//! A plugin makes a callout with `proxy_http_call`. The call is checked and recorded by the
//! guest's callout service, and a spawned task then sends the request to a peer of the chosen
//! upstream. The result is delivered to `proxy_on_http_call_response` by the filter that ran the
//! plugin, or by the root callback thread when the callout was made from a root context.

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
use headers::RejectedCalloutHeader;
use log::warn;
use pingora_http::RequestHeader;
use proxy_wasm_host::abi::v0_2_1::{Callback, CalloutId};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct PluginCalloutConf {
    pub(crate) plugin_name: String,
    pub(crate) upstreams: Arc<dyn CalloutUpstreams>,
    pub(crate) timeout_limit: Duration,
    pub(crate) wait_limit: Duration,
    pub(crate) response_limit: usize,
    zero_timeout_warning_logged: AtomicBool,
    long_timeout_warning_logged: AtomicBool,
    overflow_warning_logged: AtomicBool,
    rejected_header_warning_logged: AtomicBool,
}

impl PluginCalloutConf {
    pub(crate) fn new(
        plugin_name: &str,
        upstreams: Arc<dyn CalloutUpstreams>,
        timeout_limit: Duration,
        wait_limit: Duration,
        response_limit: usize,
    ) -> Self {
        PluginCalloutConf {
            plugin_name: plugin_name.to_string(),
            upstreams,
            timeout_limit,
            wait_limit,
            response_limit,
            zero_timeout_warning_logged: AtomicBool::new(false),
            long_timeout_warning_logged: AtomicBool::new(false),
            overflow_warning_logged: AtomicBool::new(false),
            rejected_header_warning_logged: AtomicBool::new(false),
        }
    }

    pub(crate) fn effective_timeout(&self, passed: Duration) -> Duration {
        if !passed.is_zero() && passed <= self.timeout_limit {
            return passed;
        }
        let plugin = &self.plugin_name;
        let limit = self.timeout_limit;
        if passed.is_zero() {
            if !self
                .zero_timeout_warning_logged
                .swap(true, Ordering::Relaxed)
            {
                warn!("wasm plugin {plugin}: callout timeout is zero, using callout_timeout_limit {limit:?}, further occurrences are not logged");
            }
        } else if !self
            .long_timeout_warning_logged
            .swap(true, Ordering::Relaxed)
        {
            warn!("wasm plugin {plugin}: callout timeout {passed:?} is over the limit, using callout_timeout_limit {limit:?}, further occurrences are not logged");
        }
        limit
    }

    pub(crate) fn warn_of_overflow_once(&self) {
        if !self.overflow_warning_logged.swap(true, Ordering::Relaxed) {
            warn!(
                "wasm plugin {}: max_callouts_in_flight reached, callout failed with a 503 response, further occurrences are not logged",
                self.plugin_name
            );
        }
    }

    pub(crate) fn warn_of_rejected_header_once(&self, header: &RejectedCalloutHeader) {
        if !self
            .rejected_header_warning_logged
            .swap(true, Ordering::Relaxed)
        {
            warn!(
                "wasm plugin {}: callout rejected, {header}, further occurrences are not logged",
                self.plugin_name
            );
        }
    }
}

/// A callout accepted from `proxy_http_call` whose task has not been started yet.
pub(crate) struct AcceptedCallout {
    pub(crate) id: CalloutId,
    pub(crate) plugin_conf: Arc<PluginCalloutConf>,
    pub(crate) upstream: String,
    pub(crate) request: Box<RequestHeader>,
    pub(crate) body: Bytes,
    pub(crate) timeout: Duration,
    pub(crate) callback: Option<Callback>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{crate_log_lines_with, record_crate_logs};

    const LIMIT: Duration = Duration::from_secs(10);

    #[test]
    fn timeout_of_zero_or_over_limit_uses_limit() {
        let upstreams = Arc::new(StaticCalloutUpstreams::new());
        let conf = PluginCalloutConf::new("a", upstreams, LIMIT, LIMIT, 1024);
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

    #[test]
    fn replaced_timeout_warns_once_per_cause() {
        record_crate_logs();
        let upstreams = Arc::new(StaticCalloutUpstreams::new());
        let conf = PluginCalloutConf::new("replaced-timeout", upstreams, LIMIT, LIMIT, 1024);
        let passed = [Duration::ZERO, Duration::from_secs(30)];

        for timeout in passed.into_iter().chain(passed) {
            conf.effective_timeout(timeout);
        }

        let lines = crate_log_lines_with("wasm plugin replaced-timeout: callout timeout");
        let want = [
            "wasm plugin replaced-timeout: callout timeout is zero, using \
             callout_timeout_limit 10s, further occurrences are not logged",
            "wasm plugin replaced-timeout: callout timeout 30s is over the limit, using \
             callout_timeout_limit 10s, further occurrences are not logged",
        ];
        assert_eq!(lines, want);
    }
}
