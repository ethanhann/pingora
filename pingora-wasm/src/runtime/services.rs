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

//! Proxy services for plugins

use crate::callout::{
    AcceptedCallout, CalloutResult, CalloutSender, CalloutUpstreams, PendingResult,
    StaticCalloutUpstreams,
};
use crate::invalid_conf;
use crate::observability::{CalloutFailure, LogCrateSink, NoMetricSink, WasmMetricSink};
use crate::properties::WasmProperties;
use futures::FutureExt;
use log::warn;
use pingora_core::connectors::http::Connector;
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::{Callback, LogSink};
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

const MAX_CALLOUTS_IN_FLIGHT: usize = 1024;
const SHUTDOWN_WAIT_LIMIT: Duration = Duration::from_secs(5);

/// The services your proxy provides to the plugins of a [WasmRuntime](crate::WasmRuntime).
///
/// Start from [WasmServices::default] and set the fields you need.
#[non_exhaustive]
#[derive(Clone)]
pub struct WasmServices {
    /// The sink for guest log lines.
    ///
    /// The default sink writes them to the `log` crate under the target `pingora_wasm::guest`.
    pub log_sink: Arc<dyn LogSink>,
    /// The upstreams plugins may send callouts to.
    ///
    /// The default has no upstreams, so `proxy_http_call` returns `BAD_ARGUMENT` for every
    /// callout. Pass a [StaticCalloutUpstreams] for a fixed list of peers, or your own
    /// implementation of [CalloutUpstreams].
    pub callout_upstreams: Arc<dyn CalloutUpstreams>,
    /// The connector used to send callouts, which also pools their connections. Default `None`.
    ///
    /// With `None` the runtime creates a connector with the default options. If you pass your own
    /// connector, pass the same one to the runtime that replaces this one, so that its pooled
    /// connections are kept.
    ///
    /// Callouts a plugin sends outside of a request, e.g. from `proxy_on_tick`, do not use this
    /// connector. They are sent through a separate connector with the default options.
    pub callout_connector: Option<Arc<Connector>>,
    /// The maximum number of callouts the runtime will have in flight at once. Default 1024.
    ///
    /// A callout over this limit is not sent, and its plugin receives a 503 response instead.
    /// The limit must be at least 1 and no greater than `tokio::sync::Semaphore::MAX_PERMITS`.
    pub max_callouts_in_flight: usize,
    /// The sink for metrics defined by plugins and for reports of failed callouts, plugin
    /// failures, and replaced guests.
    ///
    /// The default sink publishes nothing. When you replace a runtime to reload plugins, pass
    /// the same sink to the new one.
    pub metric_sink: Arc<dyn WasmMetricSink>,
    /// Properties for values of your proxy that never change, such as `node.metadata.NAME`.
    /// Default empty.
    ///
    /// Every plugin can read them, including from `proxy_on_configure`. A plugin cannot override
    /// a fixed property. During a request, its `proxy_set_property` call on a fixed path still
    /// succeeds, but the plugin reads the fixed value back.
    /// [WasmCtx::guest_property](crate::WasmCtx::guest_property) returns what the plugin wrote.
    pub fixed_properties: WasmProperties,
    /// How long the end of a runtime waits for its plugins to finish. Default 5 seconds.
    ///
    /// A runtime ends when the server shuts down gracefully or when a reload replaces it. Once
    /// its requests have finished, each plugin gets `proxy_on_done` on its root context. A
    /// plugin that returns `false`, e.g. to send what it collected, has this long to call
    /// `proxy_done`, and so do the contexts it kept after their requests ended.
    ///
    /// Pingora stops the server when its grace period ends, so set `grace_period_seconds` longer
    /// than your requests need plus this limit. A fast shutdown gives no time. The limit must be
    /// greater than zero.
    pub shutdown_wait_limit: Duration,
}

impl Default for WasmServices {
    fn default() -> Self {
        WasmServices {
            log_sink: Arc::new(LogCrateSink),
            callout_upstreams: Arc::new(StaticCalloutUpstreams::new()),
            callout_connector: None,
            max_callouts_in_flight: MAX_CALLOUTS_IN_FLIGHT,
            metric_sink: Arc::new(NoMetricSink),
            fixed_properties: WasmProperties::new(),
            shutdown_wait_limit: SHUTDOWN_WAIT_LIMIT,
        }
    }
}

impl fmt::Debug for WasmServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmServices")
            .field("max_callouts_in_flight", &self.max_callouts_in_flight)
            .field("shutdown_wait_limit", &self.shutdown_wait_limit)
            .finish_non_exhaustive()
    }
}

pub(crate) struct CalloutSenders {
    pub(crate) for_requests: Arc<dyn CalloutSender>,
    pub(crate) root_callback: Arc<dyn CalloutSender>,
}

pub(crate) struct CalloutLauncher {
    senders: CalloutSenders,
    metric_sink: Arc<dyn WasmMetricSink>,
    in_flight_permits: Arc<Semaphore>,
    limit: usize,
}

impl CalloutLauncher {
    pub(crate) fn new(
        senders: CalloutSenders,
        metric_sink: Arc<dyn WasmMetricSink>,
        limit: usize,
    ) -> Result<Self> {
        if limit == 0 || limit > Semaphore::MAX_PERMITS {
            return Err(invalid_conf(format!(
                "invalid max_callouts_in_flight {limit} in wasm services, must be 1 to {}",
                Semaphore::MAX_PERMITS
            )));
        }
        Ok(CalloutLauncher {
            senders,
            metric_sink,
            in_flight_permits: Arc::new(Semaphore::new(limit)),
            limit,
        })
    }

    pub(crate) fn in_flight_count(&self) -> usize {
        self.limit - self.in_flight_permits.available_permits()
    }

    pub(crate) fn spawn(&self, callout: AcceptedCallout) -> Option<PendingResult> {
        self.spawn_with(&self.senders.for_requests, callout)
    }

    pub(crate) fn spawn_for_root_callback(
        &self,
        callout: AcceptedCallout,
    ) -> Option<PendingResult> {
        self.spawn_with(&self.senders.root_callback, callout)
    }

    fn spawn_with(
        &self,
        sender: &Arc<dyn CalloutSender>,
        callout: AcceptedCallout,
    ) -> Option<PendingResult> {
        let plugin_name = callout.plugin_conf.plugin_name.clone();
        let Ok(permit) = self.in_flight_permits.clone().try_acquire_owned() else {
            callout.plugin_conf.warn_of_overflow_once();
            self.metric_sink
                .callout_failed(&plugin_name, CalloutFailure::Overflow);
            return Some(PendingResult::Known(CalloutResult::overflow_response()));
        };
        let Ok(tokio_runtime) = Handle::try_current() else {
            let callback = callout
                .callback
                .map_or("an unknown callback", Callback::export_name);
            warn!("wasm plugin {plugin_name}: callout from {callback} dropped, no tokio runtime is running");
            self.metric_sink
                .callout_failed(&plugin_name, CalloutFailure::TaskFailed);
            return None;
        };
        let sender = sender.clone();
        let metric_sink = self.metric_sink.clone();
        let task = tokio_runtime.spawn(async move {
            let sent = AssertUnwindSafe(sender.send(callout)).catch_unwind().await;
            drop(permit);
            sent.unwrap_or_else(|_| {
                metric_sink.callout_failed(&plugin_name, CalloutFailure::TaskFailed);
                CalloutResult::Failed
            })
        });
        Some(PendingResult::FromTask(task))
    }
}
