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

use super::pool::PluginPhases;
use crate::callout::{CalloutUpstreams, PluginCalloutConf};
use pingora_error::{Error, ErrorType, Result};
use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
use proxy_wasm_host::abi::v0_2_1::{LogSink, PluginConfig, SharedServices, VmServices};
use proxy_wasm_host::Limits;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const BODY_LIMIT: usize = 1024 * 1024;
const CALLOUT_TIMEOUT_LIMIT: Duration = Duration::from_secs(10);
const CALLOUT_RESPONSE_LIMIT: usize = 1024 * 1024;

/// Configuration for one Proxy-Wasm plugin.
///
/// Create one with [WasmPluginConf::new], then set the fields you need.
#[non_exhaustive]
#[derive(Debug, Clone)]
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
    /// therefore stay open for up to this long while its plugin waits for a callout.
    ///
    /// If the plugin sends callouts from a body phase or from the response headers phase, keep
    /// this limit below the `read_timeout` of your upstream peers. Must be greater than zero.
    pub callout_timeout_limit: Duration,
    /// The maximum size in bytes of a callout response body. Default 1 MiB.
    ///
    /// A callout with a larger response body fails, and the plugin receives a result with no
    /// headers and no body. Must be greater than zero.
    pub callout_response_limit: usize,
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
            callout_response_limit: CALLOUT_RESPONSE_LIMIT,
        }
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.slots == 0 {
            return Error::e_explain(
                ErrorType::InternalError,
                format!("wasm plugin {}: slots must be at least 1", self.name),
            );
        }
        if self.limits.fuel().is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                format!("wasm plugin {}: fuel limits are not supported", self.name),
            );
        }
        if self.request_body_limit == 0 || self.response_body_limit == 0 {
            return Error::e_explain(
                ErrorType::InternalError,
                format!(
                    "wasm plugin {}: request_body_limit and response_body_limit must be greater than zero",
                    self.name
                ),
            );
        }
        if self.callout_timeout_limit.is_zero() {
            return Error::e_explain(
                ErrorType::InternalError,
                format!(
                    "wasm plugin {}: callout_timeout_limit must be greater than zero",
                    self.name
                ),
            );
        }
        if self.callout_response_limit == 0 {
            return Error::e_explain(
                ErrorType::InternalError,
                format!(
                    "wasm plugin {}: callout_response_limit must be greater than zero",
                    self.name
                ),
            );
        }
        Ok(())
    }

    pub(crate) fn callout_conf(&self, upstreams: Arc<dyn CalloutUpstreams>) -> PluginCalloutConf {
        PluginCalloutConf::new(
            &self.name,
            upstreams,
            self.callout_timeout_limit,
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
        assert_eq!(conf.callout_response_limit, CALLOUT_RESPONSE_LIMIT);
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

        let errors = [
            zero.check(),
            fuel.check(),
            request.check(),
            response.check(),
            timeout.check(),
            callout.check(),
        ]
        .map(|r| r.unwrap_err().to_string());

        assert!(errors[0].contains("wasm plugin zero: slots must be at least 1"));
        assert!(errors[1].contains("wasm plugin fuel: fuel limits are not supported"));
        assert!(errors[2].contains("request: request_body_limit and response_body_limit must be"));
        assert!(errors[3].contains("response: request_body_limit and response_body_limit must be"));
        assert!(errors[4].contains("timeout: callout_timeout_limit must be greater than zero"));
        assert!(errors[5].contains("callout: callout_response_limit must be greater than zero"));
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
