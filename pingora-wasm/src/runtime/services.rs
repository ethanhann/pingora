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
//!
//! [WasmServices] is what a proxy provides to its plugins. The rest of the module spawns callout
//! tasks and limits how many callouts are in flight.

use crate::callout::{
    AcceptedCallout, CalloutResult, CalloutSender, CalloutUpstreams, PendingResult,
    StaticCalloutUpstreams,
};
use crate::observability::{CalloutFailure, LogCrateSink, NoMetricSink, WasmMetricSink};
use crate::properties::WasmProperties;
use futures::FutureExt;
use log::warn;
use pingora_core::connectors::http::Connector;
use pingora_error::{Error, ErrorType, Result};
use proxy_wasm_host::abi::v0_2_1::LogSink;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

const MAX_CALLOUTS_IN_FLIGHT: usize = 1024;

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
    /// callout.
    pub callout_upstreams: Arc<dyn CalloutUpstreams>,
    /// The connector used to send callouts, which also pools their connections. Default `None`.
    ///
    /// With `None` the runtime creates a connector with the default options. If you pass your own
    /// connector, pass the same one to the runtime that replaces this one, so that its pooled
    /// connections are kept.
    ///
    /// Callouts a plugin sends outside of a request, e.g. from `proxy_on_tick`, do not use this
    /// connector. They run on a thread that stops with the runtime, and are sent through a
    /// separate connector with the default options.
    pub callout_connector: Option<Arc<Connector>>,
    /// The maximum number of callouts the runtime will have in flight at once. Default 1024.
    ///
    /// A callout over this limit is not sent, and its plugin receives a 503 response instead.
    /// The limit must be at least 1 and no greater than `tokio::sync::Semaphore::MAX_PERMITS`.
    pub max_callouts_in_flight: usize,
    /// The sink for metrics defined by plugins and for reports of failed callouts.
    ///
    /// The default sink publishes nothing. When you replace a runtime to reload plugins, pass
    /// the same sink to the new one.
    pub metric_sink: Arc<dyn WasmMetricSink>,
    /// Properties for values of your proxy that never change, such as `node.metadata.NAME`.
    /// Default empty.
    ///
    /// Every plugin can read them, including from `proxy_on_configure`. A plugin can override a
    /// fixed property for its own request with `proxy_set_property`, which a property set with
    /// [WasmCtx::set_property](crate::WasmCtx::set_property) does not allow.
    pub fixed_properties: WasmProperties,
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
        }
    }
}

impl fmt::Debug for WasmServices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WasmServices")
            .field("max_callouts_in_flight", &self.max_callouts_in_flight)
            .finish_non_exhaustive()
    }
}

/// The callout senders of a runtime.
pub(crate) struct CalloutSenders {
    /// The sender for callouts a request is waiting for.
    pub(crate) for_requests: Arc<dyn CalloutSender>,
    /// The sender for callouts whose results are delivered by the root callback thread.
    ///
    /// It has a connector of its own because a connection is tied to the tokio runtime that
    /// opened it, and that thread's tokio runtime is shut down when the `WasmRuntime` is dropped.
    pub(crate) root_callback: Arc<dyn CalloutSender>,
}

/// A runtime's callout task launcher, which enforces the limit on callouts in flight.
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
            return Error::e_explain(
                ErrorType::InternalError,
                format!("invalid max_callouts_in_flight {limit} in wasm services"),
            );
        }
        Ok(CalloutLauncher {
            senders,
            metric_sink,
            in_flight_permits: Arc::new(Semaphore::new(limit)),
            limit,
        })
    }

    /// Return the number of callouts in flight.
    pub(crate) fn in_flight_count(&self) -> usize {
        self.limit - self.in_flight_permits.available_permits()
    }

    /// Spawn the task that sends `callout` on behalf of a request.
    ///
    /// A callout over the in-flight limit is not spawned, and a ready 503 response is returned in
    /// its place. Returns `None` if no tokio runtime is running, in which case the callout is
    /// dropped.
    pub(crate) fn spawn(&self, callout: AcceptedCallout) -> Option<PendingResult> {
        self.spawn_with(&self.senders.for_requests, callout)
    }

    /// Spawn the task that sends `callout` on behalf of the root callback thread.
    ///
    /// Behaves like [Self::spawn], except that the callout goes through the root callback sender.
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
            warn!("wasm plugin {plugin_name}: callout dropped, no tokio runtime is running");
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
