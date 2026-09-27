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
///
/// Start from [WasmPluginConf::new] and set the fields you need.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct WasmPluginConf {
    /// A name that is unique in the runtime. Chains refer to the plugin by it, and guest log
    /// lines start with it.
    pub name: String,
    /// The path of the compiled `.wasm` file.
    pub path: PathBuf,
    /// The root id that the plugin receives when it is configured. SDKs use it to pick a root
    /// context.
    pub root_id: String,
    /// The VM id. Plugins with the same VM id share data and queues.
    pub vm_id: String,
    /// Bytes that the plugin reads when its VM starts.
    pub vm_configuration: Vec<u8>,
    /// Bytes that the plugin reads when it is configured, for example a JSON document.
    pub configuration: Vec<u8>,
    /// The level that the plugin receives when it asks the host for its log level. Most SDKs
    /// set their own level and never ask.
    pub log_level: LogLevel,
    /// The memory and CPU time limits of each guest. Fuel limits are not supported.
    pub limits: Limits,
    /// The number of guests. A guest runs one callback at a time, so set it to the thread
    /// count of the service.
    pub slots: usize,
}

impl WasmPluginConf {
    /// A plugin at `path` with one slot, its name as the VM id, no configuration, the log
    /// level `Info`, and default limits.
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
    fn check_refuses_zero_slots_and_a_fuel_limit() {
        let mut zero = WasmPluginConf::new("zero", "zero.wasm");
        zero.slots = 0;
        let mut fuel = WasmPluginConf::new("fuel", "fuel.wasm");
        fuel.limits = Limits::default().with_fuel(1000);

        let errors = [zero.check(), fuel.check()].map(|r| r.unwrap_err().to_string());

        assert!(errors[0].contains("wasm plugin zero has zero slots"));
        assert!(errors[1].contains("wasm plugin fuel sets a fuel limit"));
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
