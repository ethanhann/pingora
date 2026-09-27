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

use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
use proxy_wasm_host::abi::v0_2_1::{LogContext, LogSink};
use std::borrow::Cow;

macro_rules! emit {
    ($level:expr, $context:expr, $line:expr) => {
        match $level {
            LogLevel::Trace => tracing::trace!(
                target: "guest",
                plugin = %plugin_of(&$context),
                context = context_of(&$context),
                "{}", $line
            ),
            LogLevel::Debug => tracing::debug!(
                target: "guest",
                plugin = %plugin_of(&$context),
                context = context_of(&$context),
                "{}", $line
            ),
            LogLevel::Info => tracing::info!(
                target: "guest",
                plugin = %plugin_of(&$context),
                context = context_of(&$context),
                "{}", $line
            ),
            LogLevel::Warn => tracing::warn!(
                target: "guest",
                plugin = %plugin_of(&$context),
                context = context_of(&$context),
                "{}", $line
            ),
            LogLevel::Error | LogLevel::Critical => tracing::error!(
                target: "guest",
                plugin = %plugin_of(&$context),
                context = context_of(&$context),
                "{}", $line
            ),
        }
    };
}

/// The plugin a line came from, for a root that has not been configured yet.
const UNCONFIGURED: &str = "<unconfigured>";

/// The plugin a line came from, as text.
fn plugin_of<'a>(context: &'a LogContext<'_>) -> Cow<'a, str> {
    match &context.plugin_name {
        Some(name) => String::from_utf8_lossy(name),
        None => Cow::Borrowed(UNCONFIGURED),
    }
}

/// The context a line came from, or zero when no callback was running.
fn context_of(context: &LogContext<'_>) -> u32 {
    context.call.map_or(0, |call| call.context.get())
}

pub(crate) struct TracingSink;

impl LogSink for TracingSink {
    fn log(&self, context: LogContext<'_>, level: LogLevel, message: &[u8]) {
        emit!(level, context, String::from_utf8_lossy(message));
    }
}
