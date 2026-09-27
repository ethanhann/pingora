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

use pingora_error::{Error, ErrorType, Result};
use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
use proxy_wasm_host::abi::v0_2_1::{LogSink, PluginConfig, SharedServices, VmServices};
use proxy_wasm_host::Limits;
use std::path::PathBuf;
use std::sync::Arc;

/// The configuration of one Proxy-Wasm plugin.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct WasmPluginConf {
    /// The plugin name, which must be unique in a runtime.
    pub name: String,
    /// The path of the `.wasm` module.
    pub path: PathBuf,
    /// The root id that the guest receives with its plugin configuration.
    pub root_id: String,
    /// The VM id. Plugins with the same VM id share data and queues.
    pub vm_id: String,
    /// The configuration that the guest reads in `proxy_on_vm_start`.
    pub vm_configuration: Vec<u8>,
    /// The configuration that the guest reads in `proxy_on_configure`.
    pub configuration: Vec<u8>,
    /// The level a guest receives when it asks for its log level.
    pub log_level: LogLevel,
    /// Limits for each guest. A fuel limit is refused.
    pub limits: Limits,
    /// The number of guests. Set it to the thread count of the service.
    pub slots: usize,
}

impl WasmPluginConf {
    /// A plugin at `path` with one slot, no configuration, the log level `Info`, and default
    /// limits.
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
        }
    }

    pub(crate) fn check(&self) -> Result<()> {
        if self.slots == 0 {
            return Error::e_explain(
                ErrorType::InternalError,
                format!("wasm plugin {} has zero slots", self.name),
            );
        }
        if self.limits.fuel().is_some() {
            return Error::e_explain(
                ErrorType::InternalError,
                format!("wasm plugin {} sets a fuel limit", self.name),
            );
        }
        Ok(())
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
    fn new_sets_the_defaults() {
        let conf = WasmPluginConf::new("auth", "/plugins/auth.wasm");

        assert_eq!(conf.name, "auth");
        assert_eq!(conf.vm_id, "auth");
        assert_eq!(conf.root_id, "");
        assert!(conf.vm_configuration.is_empty());
        assert!(conf.configuration.is_empty());
        assert_eq!(conf.log_level, LogLevel::Info);
        assert_eq!(conf.slots, 1);
        assert_eq!(conf.limits.fuel(), None);
    }

    #[test]
    fn check_refuses_zero_slots() {
        let mut conf = WasmPluginConf::new("auth", "auth.wasm");
        conf.slots = 0;

        let err = conf.check().unwrap_err();

        assert!(err.to_string().contains("wasm plugin auth has zero slots"));
    }

    #[test]
    fn check_refuses_a_fuel_limit() {
        let mut conf = WasmPluginConf::new("auth", "auth.wasm");
        conf.limits = Limits::default().with_fuel(1000);

        let err = conf.check().unwrap_err();

        assert!(err
            .to_string()
            .contains("wasm plugin auth sets a fuel limit"));
    }

    #[test]
    fn plugin_config_maps_the_fields() {
        let mut conf = WasmPluginConf::new("auth", "auth.wasm");
        conf.root_id = "root".to_string();
        conf.configuration = b"conf".to_vec();

        let plugin = conf.plugin_config();

        assert_eq!(plugin.name(), b"auth");
        assert_eq!(plugin.root_id(), b"root");
        assert_eq!(plugin.configuration(), b"conf");
    }
}
