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

//! Plugin configuration from a file
//!
//! `WasmPluginConf` cannot derive `Deserialize` because `Limits` and `LogLevel` belong to another
//! crate and have no serde support. It is deserialized through the private types in this module
//! instead.

use crate::runtime::{FailPolicy, WasmPluginConf};
use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
use proxy_wasm_host::Limits;
use serde::{Deserialize, Deserializer};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PluginConfInFile {
    name: String,
    path: PathBuf,
    root_id: Option<String>,
    vm_id: Option<String>,
    configuration: Option<String>,
    vm_configuration: Option<String>,
    log_level: Option<LogLevelInFile>,
    slots: Option<usize>,
    request_body: Option<bool>,
    response_body: Option<bool>,
    response_trailers: Option<bool>,
    request_body_limit: Option<usize>,
    response_body_limit: Option<usize>,
    callout_timeout_limit_seconds: Option<u64>,
    callout_wait_limit_seconds: Option<u64>,
    callout_response_limit: Option<usize>,
    fail_policy: Option<FailPolicy>,
    #[serde(default)]
    limits: LimitsInFile,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum LogLevelInFile {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Critical,
}

impl From<LogLevelInFile> for LogLevel {
    fn from(level: LogLevelInFile) -> Self {
        match level {
            LogLevelInFile::Trace => LogLevel::Trace,
            LogLevelInFile::Debug => LogLevel::Debug,
            LogLevelInFile::Info => LogLevel::Info,
            LogLevelInFile::Warn => LogLevel::Warn,
            LogLevelInFile::Error => LogLevel::Error,
            LogLevelInFile::Critical => LogLevel::Critical,
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsInFile {
    cpu_time_ms: Option<u64>,
    memory_bytes: Option<usize>,
    max_decoded_pairs: Option<u32>,
    max_decoded_map_bytes: Option<usize>,
    max_shared_names: Option<usize>,
    max_name_bytes: Option<usize>,
    max_log_bytes: Option<usize>,
    max_open_callouts: Option<usize>,
    table_elements: Option<usize>,
}

impl From<LimitsInFile> for Limits {
    fn from(file: LimitsInFile) -> Self {
        let mut limits = Limits::default();
        if let Some(milliseconds) = file.cpu_time_ms {
            limits = limits.with_cpu_time(Duration::from_millis(milliseconds));
        }
        if let Some(bytes) = file.memory_bytes {
            limits = limits.with_memory_bytes(bytes);
        }
        if let Some(pairs) = file.max_decoded_pairs {
            limits = limits.with_max_decoded_pairs(pairs);
        }
        if let Some(bytes) = file.max_decoded_map_bytes {
            limits = limits.with_max_decoded_map_bytes(bytes);
        }
        if let Some(names) = file.max_shared_names {
            limits = limits.with_max_shared_names(names);
        }
        if let Some(bytes) = file.max_name_bytes {
            limits = limits.with_max_name_bytes(bytes);
        }
        if let Some(bytes) = file.max_log_bytes {
            limits = limits.with_max_log_bytes(bytes);
        }
        if let Some(callouts) = file.max_open_callouts {
            limits = limits.with_max_open_callouts(callouts);
        }
        if let Some(elements) = file.table_elements {
            limits = limits.with_table_elements(elements);
        }
        limits
    }
}

impl From<PluginConfInFile> for WasmPluginConf {
    fn from(file: PluginConfInFile) -> Self {
        let mut conf = WasmPluginConf::new(file.name, file.path);
        conf.limits = file.limits.into();
        if let Some(root_id) = file.root_id {
            conf.root_id = root_id;
        }
        if let Some(vm_id) = file.vm_id {
            conf.vm_id = vm_id;
        }
        if let Some(configuration) = file.configuration {
            conf.configuration = configuration.into_bytes();
        }
        if let Some(vm_configuration) = file.vm_configuration {
            conf.vm_configuration = vm_configuration.into_bytes();
        }
        if let Some(log_level) = file.log_level {
            conf.log_level = log_level.into();
        }
        if let Some(slots) = file.slots {
            conf.slots = slots;
        }
        if let Some(request_body) = file.request_body {
            conf.request_body = request_body;
        }
        if let Some(response_body) = file.response_body {
            conf.response_body = response_body;
        }
        if let Some(response_trailers) = file.response_trailers {
            conf.response_trailers = response_trailers;
        }
        if let Some(limit) = file.request_body_limit {
            conf.request_body_limit = limit;
        }
        if let Some(limit) = file.response_body_limit {
            conf.response_body_limit = limit;
        }
        if let Some(seconds) = file.callout_timeout_limit_seconds {
            conf.callout_timeout_limit = Duration::from_secs(seconds);
        }
        if let Some(seconds) = file.callout_wait_limit_seconds {
            conf.callout_wait_limit = Duration::from_secs(seconds);
        }
        if let Some(limit) = file.callout_response_limit {
            conf.callout_response_limit = limit;
        }
        if let Some(fail_policy) = file.fail_policy {
            conf.fail_policy = fail_policy;
        }
        conf
    }
}

impl<'de> Deserialize<'de> for WasmPluginConf {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        PluginConfInFile::deserialize(deserializer).map(WasmPluginConf::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plugin(yaml: &str) -> WasmPluginConf {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn name_and_path_alone_give_defaults_of_new() {
        let read = plugin("name: auth\npath: /plugins/auth.wasm\n");

        let built = WasmPluginConf::new("auth", "/plugins/auth.wasm");
        assert_eq!(format!("{read:?}"), format!("{built:?}"));
    }

    #[test]
    fn plugin_keys_set_their_fields() {
        let read = plugin(
            r#"
name: auth
path: auth.wasm
root_id: auth_root
vm_id: shared
configuration: '{"mode": "strict"}'
vm_configuration: vm
slots: 4
request_body: true
response_body: true
response_trailers: true
request_body_limit: 10
response_body_limit: 20
callout_timeout_limit_seconds: 3
callout_wait_limit_seconds: 5
callout_response_limit: 30
fail_policy: open
"#,
        );

        assert_eq!(read.root_id, "auth_root");
        assert_eq!(read.vm_id, "shared");
        assert_eq!(read.configuration, br#"{"mode": "strict"}"#);
        assert_eq!(read.vm_configuration, b"vm");
        assert_eq!(read.slots, 4);
        assert!(read.request_body && read.response_body && read.response_trailers);
        assert_eq!(read.request_body_limit, 10);
        assert_eq!(read.response_body_limit, 20);
        assert_eq!(read.callout_timeout_limit, Duration::from_secs(3));
        assert_eq!(read.callout_wait_limit, Duration::from_secs(5));
        assert_eq!(read.callout_response_limit, 30);
        assert_eq!(read.fail_policy, FailPolicy::Open);
    }

    #[test]
    fn each_limit_key_sets_its_limit() {
        let read = plugin(
            r#"
name: auth
path: auth.wasm
limits:
  cpu_time_ms: 250
  memory_bytes: 1048576
  max_decoded_pairs: 11
  max_decoded_map_bytes: 12
  max_shared_names: 13
  max_name_bytes: 14
  max_log_bytes: 15
  max_open_callouts: 16
  table_elements: 17
"#,
        );

        let want = Limits::default()
            .with_cpu_time(Duration::from_millis(250))
            .with_memory_bytes(1_048_576)
            .with_max_decoded_pairs(11)
            .with_max_decoded_map_bytes(12)
            .with_max_shared_names(13)
            .with_max_name_bytes(14)
            .with_max_log_bytes(15)
            .with_max_open_callouts(16)
            .with_table_elements(17);
        assert_eq!(read.limits, want);
    }

    #[test]
    fn log_level_is_read_by_lowercase_name() {
        let cases = [
            ("trace", LogLevel::Trace),
            ("debug", LogLevel::Debug),
            ("info", LogLevel::Info),
            ("warn", LogLevel::Warn),
            ("error", LogLevel::Error),
            ("critical", LogLevel::Critical),
        ];

        for (name, level) in cases {
            let read = plugin(&format!("name: a\npath: a.wasm\nlog_level: {name}\n"));

            assert_eq!(read.log_level, level);
        }
    }

    #[test]
    fn unknown_or_missing_key_is_an_error() {
        let cases = [
            (
                "name: a\npath: a.wasm\nresponse_bdy: true\n",
                "unknown field `response_bdy`",
            ),
            (
                "name: a\npath: a.wasm\nlimits:\n  fuel: 10\n",
                "unknown field `fuel`",
            ),
            (
                "name: a\npath: a.wasm\nlog_level: verbose\n",
                "unknown variant `verbose`",
            ),
            ("name: a\n", "missing field `path`"),
        ];

        for (yaml, message) in cases {
            let err = serde_yaml::from_str::<WasmPluginConf>(yaml).unwrap_err();

            assert!(err.to_string().contains(message), "{err}");
        }
    }
}
