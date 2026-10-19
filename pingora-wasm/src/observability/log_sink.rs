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

use log::Level;
use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
use proxy_wasm_host::abi::v0_2_1::{LogContext, LogSink};

pub(crate) const GUEST_TARGET: &str = "pingora_wasm::guest";

pub(crate) struct LogCrateSink;

impl LogSink for LogCrateSink {
    fn log(&self, context: LogContext<'_>, level: LogLevel, message: &[u8]) {
        let level = log_level(level);
        if !log::log_enabled!(target: GUEST_TARGET, level) {
            return;
        }
        let plugin = context.plugin_name.as_deref().unwrap_or(&context.vm_id);
        log::log!(
            target: GUEST_TARGET,
            level,
            "{} #{}: {}",
            String::from_utf8_lossy(plugin),
            context.call.map_or(0, |call| call.context.get()),
            String::from_utf8_lossy(message)
        );
    }
}

pub(crate) fn log_level(level: LogLevel) -> Level {
    match level {
        LogLevel::Trace => Level::Trace,
        LogLevel::Debug => Level::Debug,
        LogLevel::Info => Level::Info,
        LogLevel::Warn => Level::Warn,
        LogLevel::Error | LogLevel::Critical => Level::Error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_level_maps_every_guest_level() {
        let levels: Vec<_> = LogLevel::ALL.iter().map(|l| log_level(*l)).collect();

        assert_eq!(
            levels,
            [
                Level::Trace,
                Level::Debug,
                Level::Info,
                Level::Warn,
                Level::Error,
                Level::Error
            ]
        );
    }
}
