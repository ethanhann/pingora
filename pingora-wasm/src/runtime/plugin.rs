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

use super::fail_policy::FailPolicy;
use super::pool::PluginPhases;
use crate::callout::{CalloutUpstreams, PluginCalloutConf};
use crate::invalid_conf;
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
use proxy_wasm_host::abi::v0_2_1::{LogSink, PluginConfig, SharedServices, VmServices};
use proxy_wasm_host::Limits;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const BODY_LIMIT: usize = 1024 * 1024;
const CALLOUT_TIMEOUT_LIMIT: Duration = Duration::from_secs(10);
const CALLOUT_WAIT_LIMIT: Duration = Duration::from_secs(30);
const CALLOUT_RESPONSE_LIMIT: usize = 1024 * 1024;

/// Configuration for one Proxy-Wasm plugin.
///
/// Create one with [WasmPluginConf::new], then set the fields you need.
#[non_exhaustive]
#[derive(Clone)]
pub struct WasmPluginConf {
    /// The name of the plugin, which must be unique within a runtime.
    ///
    /// Chains refer to the plugin by this name, and the default log sink puts it in front of
    /// every guest log line.
    pub name: String,
    /// The path to the compiled `.wasm` file.
    pub path: PathBuf,
    /// The root id passed to the plugin when it is configured. Default empty.
    ///
    /// Proxy-Wasm SDKs use it to select the plugin's root context.
    pub root_id: String,
    /// The VM id. Defaults to the plugin's name.
    ///
    /// Plugins with the same VM id share data, queues, and metrics.
    pub vm_id: String,
    /// The VM configuration, which the plugin can read when its VM starts. Default empty.
    pub vm_configuration: Vec<u8>,
    /// The plugin configuration, which the plugin can read when it is configured, e.g. a JSON
    /// document. Default empty.
    pub configuration: Vec<u8>,
    /// The log level reported to the plugin when it asks the host for one. Default `Info`.
    ///
    /// The Rust SDK sets its own level and never asks.
    pub log_level: LogLevel,
    /// The resource limits of each guest, such as its memory and its CPU time per callback.
    ///
    /// Fuel limits are not supported, and a configuration that sets one is rejected.
    pub limits: Limits,
    /// The number of guests to run. Default 1.
    ///
    /// A guest runs one callback at a time, so set this to the thread count of the service that
    /// uses the plugin. Must be at least 1.
    pub slots: usize,
    /// Whether to run the plugin on request bodies. Default `false`.
    ///
    /// When enabled, `proxy_on_request_body` is called once for each chunk of a request body. A
    /// guest runs one callback at a time, so a chunk has to wait while another request is running
    /// a callback in the same guest. In the worst case the wait lasts as long as the CPU time
    /// allowed by [limits](Self::limits). Only enable this for a plugin that reads request
    /// bodies, and set [slots](Self::slots) to the thread count of the service.
    ///
    /// The phases each plugin runs on are logged when the runtime is built.
    pub request_body: bool,
    /// Whether to run the plugin on response bodies. Default `false`.
    ///
    /// When enabled, `proxy_on_response_body` is called once for each chunk of a response body.
    /// A chunk may have to wait for its guest in the same way as with
    /// [request_body](Self::request_body).
    pub response_body: bool,
    /// Whether to run the plugin on response trailers. Default `false`.
    pub response_trailers: bool,
    /// The maximum number of request body bytes the plugin may hold while it pauses the body.
    /// Default 1 MiB.
    ///
    /// A request whose plugin holds more fails with
    /// [ERR_REQUEST_BODY_TOO_LARGE](crate::ERR_REQUEST_BODY_TOO_LARGE). Must be greater than
    /// zero.
    pub request_body_limit: usize,
    /// The maximum number of response body bytes the plugin may hold while it pauses the body.
    /// Default 1 MiB.
    ///
    /// A request whose plugin holds more fails with
    /// [ERR_RESPONSE_BODY_TOO_LARGE](crate::ERR_RESPONSE_BODY_TOO_LARGE). Must be greater than
    /// zero.
    pub response_body_limit: usize,
    /// The longest a single callout from the plugin may take. Default 10 seconds.
    ///
    /// A callout normally uses the timeout the plugin passes to `proxy_http_call`. If that
    /// timeout is zero, which some hosts treat as no timeout, or longer than this limit, the
    /// limit is used instead and a warning is logged the first time it happens. A request can
    /// therefore stay open for up to this long for each callout its plugin waits for, and for up
    /// to [callout_wait_limit](Self::callout_wait_limit) for the wait as a whole.
    ///
    /// If the plugin sends callouts from a body filter or from the response header filter, keep
    /// this limit below the `read_timeout` of your upstream peers. Must be greater than zero and
    /// less than [callout_wait_limit](Self::callout_wait_limit).
    pub callout_timeout_limit: Duration,
    /// The longest a filter may wait for the plugin's callouts. Default 30 seconds.
    ///
    /// A callout wait begins when the plugin pauses a filter to wait for a callout, and ends
    /// when the plugin continues or sends a response. It covers every callout the plugin sends
    /// in the meantime. Each callout is already bounded by
    /// [callout_timeout_limit](Self::callout_timeout_limit), but a plugin may send a new callout
    /// from each `proxy_on_http_call_response`, and without this limit it could keep a request
    /// open indefinitely.
    ///
    /// When a wait reaches the limit, the filter stops waiting and treats this as a plugin
    /// failure, so [fail_policy](Self::fail_policy) decides whether the request fails or
    /// continues without the plugin. Callouts still in flight run to completion and their results
    /// are discarded.
    ///
    /// The limit applies to each wait separately. A body filter runs once per chunk, so a plugin
    /// that waits on every chunk gets the full limit each time. If the plugin sends callouts
    /// from a body filter or from the response header filter, keep this limit below the
    /// `read_timeout` of your upstream peers. Must be greater than
    /// [callout_timeout_limit](Self::callout_timeout_limit).
    ///
    /// A timeout of your own around a filter cannot replace this limit. Once the future of a
    /// filter has been dropped during a callout wait, every later header or trailer filter of the
    /// request, and every later body filter that has a plugin to run, returns an error, whatever
    /// the fail policy.
    pub callout_wait_limit: Duration,
    /// The maximum size in bytes of a callout response body. Default 1 MiB.
    ///
    /// A callout with a larger response body fails, and the plugin receives a result with no
    /// headers and no body. Must be greater than zero.
    pub callout_response_limit: usize,
    /// What happens to a request when the plugin fails. Default [FailPolicy::Closed].
    ///
    /// With [FailPolicy::Closed], a plugin failure fails the request. The filter returns
    /// [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED), and Pingora responds with 503 or, once the
    /// response header has been sent, ends the response early. The exception is
    /// [WasmCtx::response_trailer_filter](crate::WasmCtx::response_trailer_filter). Pingora only
    /// logs an error from `response_trailer_filter` and still sends the trailers. If a plugin
    /// was holding body bytes, the filter logs the failure and returns those bytes in place of
    /// the error.
    ///
    /// With [FailPolicy::Open], the failure is logged, the plugin is skipped, and the request
    /// continues with the next plugin. No later header, body, or trailer filter of that request
    /// runs the skipped plugin again. Per plugin, one skip every 10 seconds is logged as a
    /// warning, and the rest at debug level. A plugin is skipped when it:
    ///
    /// - traps or returns an error in a callback, including `proxy_on_http_call_response`
    /// - has no guest in any of its slots when the request starts
    /// - loses the guest that held the request's context, e.g. to a trap in another request
    /// - pauses on request headers, response headers, response trailers, or the last chunk of a
    ///   body with no callout pending
    /// - waits for callouts longer than [callout_wait_limit](Self::callout_wait_limit)
    ///
    /// `Open` on a plugin that authorizes requests therefore lets a request through each time
    /// the plugin crashes, hangs, or is slow.
    /// [WasmCtx::skipped_plugins](crate::WasmCtx::skipped_plugins) returns the plugins skipped
    /// on a request, so your proxy can enforce a rule of its own, e.g. deny the request, add a
    /// header, or tag its access log.
    ///
    /// Some failures fail the request under both policies:
    ///
    /// - A failure while a body the plugin changed can still have bytes to come. A plugin
    ///   changes a body when it writes to the body bytes, or when one of its writes changes the
    ///   value of the `content-length` or `transfer-encoding` header of that message. A write
    ///   that leaves the header as it was does not count. A body only counts if the plugin runs
    ///   on it, i.e. [request_body](Self::request_body) or
    ///   [response_body](Self::response_body) is enabled and the plugin exports the callback. A
    ///   request body counts until its last chunk has run through the plugins. A response body
    ///   counts until the response has ended, which it has if it has no body, once its last
    ///   body chunk has run through the plugins, or once its trailers have arrived. Neither
    ///   counts once a plugin has sent its own response, since no body is proxied after that.
    ///   Until then, the rest of the body would go out without the plugin's changes, so the
    ///   upstream or the downstream would get a complete message with a mixed body.
    /// - More held body bytes than [request_body_limit](Self::request_body_limit) or
    ///   [response_body_limit](Self::response_body_limit) allows. Otherwise a client could
    ///   bypass the plugin by padding its request.
    /// - A response sent after the response header. It can no longer replace the response that
    ///   has already started.
    /// - Any filter that runs after an earlier filter of the request was cancelled during a
    ///   callout wait, since the plugins may have been left halfway through that filter.
    ///
    /// A skipped plugin keeps what it did before it failed, e.g. a header it added or a property
    /// it set. Body bytes it was holding are released as it left them. They go to the next
    /// plugin if it failed in the filter of that body, and otherwise ahead of the next chunk of
    /// that body. The response trailer filter also releases held response bytes. If the body has
    /// no further chunk and no trailers, the bytes are not sent, and
    /// [WasmCtx::logging](crate::WasmCtx::logging) logs how many were left.
    ///
    /// A response or callout from the failing callback is dropped, and the results of its
    /// pending callouts are discarded. If its guest is still usable, `logging` runs its
    /// `proxy_on_log` as usual, so a statistics plugin still counts the request.
    ///
    /// A plugin that cannot start fails [WasmRuntime::new](crate::WasmRuntime::new) under both
    /// policies.
    pub fail_policy: FailPolicy,
}

// Either configuration may hold a secret, so only their lengths are printed
impl fmt::Debug for WasmPluginConf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = |configuration: &[u8]| format!("{} bytes", configuration.len());
        f.debug_struct("WasmPluginConf")
            .field("name", &self.name)
            .field("path", &self.path)
            .field("root_id", &self.root_id)
            .field("vm_id", &self.vm_id)
            .field("vm_configuration", &bytes(&self.vm_configuration))
            .field("configuration", &bytes(&self.configuration))
            .field("log_level", &self.log_level)
            .field("limits", &self.limits)
            .field("slots", &self.slots)
            .field("request_body", &self.request_body)
            .field("response_body", &self.response_body)
            .field("response_trailers", &self.response_trailers)
            .field("request_body_limit", &self.request_body_limit)
            .field("response_body_limit", &self.response_body_limit)
            .field("callout_timeout_limit", &self.callout_timeout_limit)
            .field("callout_wait_limit", &self.callout_wait_limit)
            .field("callout_response_limit", &self.callout_response_limit)
            .field("fail_policy", &self.fail_policy)
            .finish()
    }
}

impl WasmPluginConf {
    /// Create the configuration for the plugin at `path`.
    ///
    /// The plugin starts out with one slot, its name as the VM id, an empty root id, no
    /// configuration, the log level `Info`, and the default limits. It runs on request and
    /// response headers only.
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        let name = name.into();
        WasmPluginConf {
            vm_id: name.clone(),
            name,
            path: path.into(),
            root_id: String::new(),
            vm_configuration: Vec::new(),
            configuration: Vec::new(),
            log_level: LogLevel::Info,
            limits: Limits::default(),
            slots: 1,
            request_body: false,
            response_body: false,
            response_trailers: false,
            request_body_limit: BODY_LIMIT,
            response_body_limit: BODY_LIMIT,
            callout_timeout_limit: CALLOUT_TIMEOUT_LIMIT,
            callout_wait_limit: CALLOUT_WAIT_LIMIT,
            callout_response_limit: CALLOUT_RESPONSE_LIMIT,
            fail_policy: FailPolicy::Closed,
        }
    }

    pub(crate) fn check(&self) -> Result<()> {
        let name = &self.name;
        let (wait, timeout) = (self.callout_wait_limit, self.callout_timeout_limit);
        let mistake = if self.slots == 0 {
            "slots must be at least 1"
        } else if self.limits.fuel().is_some() {
            "fuel limits are not supported"
        } else if self.request_body_limit == 0 {
            "request_body_limit must be greater than zero"
        } else if self.response_body_limit == 0 {
            "response_body_limit must be greater than zero"
        } else if timeout.is_zero() {
            "callout_timeout_limit must be greater than zero"
        } else if wait <= timeout {
            return Err(invalid_conf(format!(
                "wasm plugin {name}: callout_wait_limit {wait:?} must be greater than callout_timeout_limit {timeout:?}"
            )));
        } else if self.callout_response_limit == 0 {
            "callout_response_limit must be greater than zero"
        } else {
            return Ok(());
        };
        Err(invalid_conf(format!("wasm plugin {name}: {mistake}")))
    }

    pub(crate) fn callout_conf(&self, upstreams: Arc<dyn CalloutUpstreams>) -> PluginCalloutConf {
        PluginCalloutConf::new(
            &self.name,
            upstreams,
            self.callout_timeout_limit,
            self.callout_wait_limit,
            self.callout_response_limit,
        )
    }

    pub(crate) fn phase_conf(&self) -> PluginPhases {
        PluginPhases {
            request: self.request_body,
            response: self.response_body,
            trailers: self.response_trailers,
            request_limit: self.request_body_limit,
            response_limit: self.response_body_limit,
        }
    }

    pub(crate) fn plugin_config(&self) -> PluginConfig {
        PluginConfig::new()
            .with_name(self.name.as_bytes())
            .with_root_id(self.root_id.as_bytes())
            .with_configuration(self.configuration.clone())
    }

    pub(crate) fn services(
        &self,
        sink: Arc<dyn LogSink>,
        shared: Arc<dyn SharedServices>,
    ) -> VmServices {
        VmServices::new(sink)
            .with_shared(shared)
            .with_vm_id(self.vm_id.as_bytes())
            .with_vm_configuration(self.vm_configuration.clone())
            .with_log_level(self.log_level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ERR_INVALID_CONF;

    #[test]
    fn new_sets_defaults() {
        let conf = WasmPluginConf::new("auth", "/plugins/auth.wasm");

        assert_eq!(conf.name, "auth");
        assert_eq!(conf.vm_id, "auth");
        assert_eq!(conf.root_id, "");
        assert!(conf.vm_configuration.is_empty());
        assert!(conf.configuration.is_empty());
        assert_eq!(conf.log_level, LogLevel::Info);
        assert_eq!(conf.slots, 1);
        assert_eq!(conf.limits.fuel(), None);
        assert!(!conf.request_body && !conf.response_body && !conf.response_trailers);
        assert_eq!(conf.request_body_limit, BODY_LIMIT);
        assert_eq!(conf.response_body_limit, BODY_LIMIT);
        assert_eq!(conf.callout_timeout_limit, Duration::from_secs(10));
        assert_eq!(conf.callout_wait_limit, Duration::from_secs(30));
        assert_eq!(conf.callout_response_limit, CALLOUT_RESPONSE_LIMIT);
        assert_eq!(conf.fail_policy, FailPolicy::Closed);
    }

    #[test]
    fn check_rejects_zero_slots_fuel_and_zero_limits() {
        let mut zero = WasmPluginConf::new("zero", "zero.wasm");
        zero.slots = 0;
        let mut fuel = WasmPluginConf::new("fuel", "fuel.wasm");
        fuel.limits = Limits::default().with_fuel(1000);
        let mut request = WasmPluginConf::new("request", "request.wasm");
        request.request_body_limit = 0;
        let mut response = WasmPluginConf::new("response", "response.wasm");
        response.response_body_limit = 0;
        let mut timeout = WasmPluginConf::new("timeout", "timeout.wasm");
        timeout.callout_timeout_limit = Duration::ZERO;
        let mut callout = WasmPluginConf::new("callout", "callout.wasm");
        callout.callout_response_limit = 0;
        let mut wait = WasmPluginConf::new("wait", "wait.wasm");
        wait.callout_wait_limit = Duration::from_secs(9);
        let mut equal = WasmPluginConf::new("equal", "equal.wasm");
        equal.callout_wait_limit = equal.callout_timeout_limit;

        let errors = [
            zero.check(),
            fuel.check(),
            request.check(),
            response.check(),
            timeout.check(),
            callout.check(),
            wait.check(),
            equal.check(),
        ]
        .map(|r| r.unwrap_err());

        assert!(errors.iter().all(|e| e.etype() == &ERR_INVALID_CONF));
        let errors = errors.map(|e| e.to_string());

        assert!(errors[0].contains("wasm plugin zero: slots must be at least 1"));
        assert!(errors[1].contains("wasm plugin fuel: fuel limits are not supported"));
        assert!(errors[2].contains("request: request_body_limit must be greater than zero"));
        assert!(errors[3].contains("response: response_body_limit must be greater than zero"));
        assert!(errors[4].contains("timeout: callout_timeout_limit must be greater than zero"));
        assert!(errors[5].contains("callout: callout_response_limit must be greater than zero"));
        let wait = "wait: callout_wait_limit 9s must be greater than callout_timeout_limit 10s";
        assert!(errors[6].contains(wait), "{}", errors[6]);
        let equal = "equal: callout_wait_limit 10s must be greater than callout_timeout_limit 10s";
        assert!(errors[7].contains(equal), "{}", errors[7]);
    }

    #[test]
    fn debug_output_has_configuration_lengths_only() {
        let mut conf = WasmPluginConf::new("auth", "auth.wasm");
        conf.configuration = b"token=s3cret".to_vec();
        conf.vm_configuration = b"key=hidden".to_vec();

        let debug = format!("{conf:?}");

        assert!(debug.contains("configuration: \"12 bytes\""), "{debug}");
        assert!(debug.contains("vm_configuration: \"10 bytes\""), "{debug}");
        assert!(
            !debug.contains("s3cret") && !debug.contains("115"),
            "{debug}"
        );
    }

    #[test]
    fn plugin_config_carries_name_root_id_and_configuration() {
        let mut conf = WasmPluginConf::new("auth", "auth.wasm");
        conf.root_id = "root".to_string();
        conf.configuration = b"conf".to_vec();

        let plugin = conf.plugin_config();

        assert_eq!(plugin.name(), b"auth");
        assert_eq!(plugin.root_id(), b"root");
        assert_eq!(plugin.configuration(), b"conf");
    }
}
