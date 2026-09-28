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

//! The services of the proxy that the plugins of a runtime use.

use super::log_sink::LogCrateSink;
use crate::callout::{
    AcceptedCallout, CalloutResult, CalloutSender, CalloutUpstreams, PendingResult,
    StaticCalloutUpstreams,
};
use log::warn;
use pingora_core::connectors::http::Connector;
use pingora_error::{Error, ErrorType, Result};
use proxy_wasm_host::abi::v0_2_1::LogSink;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

const MAX_CALLOUTS_IN_FLIGHT: usize = 1024;

/// The services of your proxy that the plugins of a [WasmRuntime](crate::WasmRuntime) use.
///
/// Start from [WasmServices::default] and set the fields you need.
#[non_exhaustive]
#[derive(Clone)]
pub struct WasmServices {
    /// The destination of guest log lines. The default sends them to the `log` crate with the
    /// target `pingora_wasm::guest`.
    pub log_sink: Arc<dyn LogSink>,
    /// The upstreams that plugins can send callouts to. The default has no upstream, so
    /// `proxy_http_call` returns `BAD_ARGUMENT` for every callout.
    pub callout_upstreams: Arc<dyn CalloutUpstreams>,
    /// The connector that sends the callouts and pools their connections. Default `None`, in
    /// which case the runtime creates a connector with the default options.
    ///
    /// When you replace a runtime to reload plugins, pass the connector of the old runtime to
    /// the new one to keep the connections.
    pub callout_connector: Option<Arc<Connector>>,
    /// The maximum number of callouts that the runtime sends at the same time. Default 1024.
    ///
    /// A callout over this limit is not sent, and its plugin receives a 503 response. The
    /// limit cannot be zero or more than `tokio::sync::Semaphore::MAX_PERMITS`.
    pub max_callouts_in_flight: usize,
}

impl Default for WasmServices {
    fn default() -> Self {
        WasmServices {
            log_sink: Arc::new(LogCrateSink),
            callout_upstreams: Arc::new(StaticCalloutUpstreams::new()),
            callout_connector: None,
            max_callouts_in_flight: MAX_CALLOUTS_IN_FLIGHT,
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

/// The launcher of callout tasks for a runtime, with the limit on how many are in flight.
pub(crate) struct CalloutLauncher {
    sender: Arc<dyn CalloutSender>,
    in_flight_permits: Arc<Semaphore>,
    limit: usize,
}

impl CalloutLauncher {
    pub(crate) fn new(sender: Arc<dyn CalloutSender>, limit: usize) -> Result<Self> {
        if limit == 0 || limit > Semaphore::MAX_PERMITS {
            return Error::e_explain(
                ErrorType::InternalError,
                format!("max_callouts_in_flight of the wasm services cannot be {limit}"),
            );
        }
        Ok(CalloutLauncher {
            sender,
            in_flight_permits: Arc::new(Semaphore::new(limit)),
            limit,
        })
    }

    /// Return the number of callouts that are being sent.
    pub(crate) fn in_flight(&self) -> usize {
        self.limit - self.in_flight_permits.available_permits()
    }

    /// Start the task that sends `callout`.
    ///
    /// Return a response in place of the task for a callout over the limit, and `None` when no
    /// tokio runtime is running, so the callout cannot be sent.
    pub(crate) fn spawn(&self, callout: AcceptedCallout) -> Option<PendingResult> {
        let Ok(permit) = self.in_flight_permits.clone().try_acquire_owned() else {
            callout.conf.warn_of_overflow_once();
            return Some(PendingResult::Ready(CalloutResult::overflow_response()));
        };
        let Ok(tokio_runtime) = Handle::try_current() else {
            warn!(
                "wasm plugin {} sent a callout with no tokio runtime running, and the callout is dropped",
                callout.conf.plugin
            );
            return None;
        };
        let sender = self.sender.clone();
        let task = tokio_runtime.spawn(async move {
            let result = sender.send(callout).await;
            drop(permit);
            result
        });
        Some(PendingResult::Running(task))
    }
}
