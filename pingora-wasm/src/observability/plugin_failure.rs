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

//! Plugin failure reports

use std::fmt;

/// The kind of plugin failure, as reported to [WasmMetricSink::plugin_failed].
///
/// [as_str](Self::as_str) returns the value [PrometheusMetricSink](crate::PrometheusMetricSink)
/// uses for its `failure` label.
///
/// [WasmMetricSink::plugin_failed]: crate::WasmMetricSink::plugin_failed
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PluginFailure {
    /// A callback trapped or returned an error.
    GuestError,
    /// The plugin had no guest in any of its slots when the request started.
    Unavailable,
    /// The guest holding the request's context was gone from its slot when a filter was about to
    /// run the plugin.
    ///
    /// A guest leaves its slot once a failure has made it unusable, e.g. a trap in another
    /// request. A request that lost its context that way reports this the next time a filter is
    /// about to run the plugin, so a request that only reaches `logging` after the loss reports
    /// nothing.
    /// [WasmMetricSink::guest_replaced](crate::WasmMetricSink::guest_replaced) is called
    /// separately, once for each new guest.
    GuestLost,
    /// The plugin paused on headers, on trailers, or on the last chunk of a body with no callout
    /// pending.
    PausedWithoutCallout,
    /// A callout wait lasted longer than
    /// [callout_wait_limit](crate::WasmPluginConf::callout_wait_limit).
    WaitLimit,
    /// The plugin failed while a body it had changed could still have bytes to come.
    ///
    /// Only reported for a plugin with [FailPolicy::Open](crate::FailPolicy::Open), where it is
    /// the reason the request failed and the plugin was not skipped, so the outcome is always
    /// [PluginFailureOutcome::Failed]. [fail_policy](crate::WasmPluginConf::fail_policy) describes
    /// when a changed body has this effect. For a plugin with `Closed`, the report has the
    /// failure itself, e.g. [GuestError](Self::GuestError).
    BodyChanged,
    /// The plugin held more body bytes than its limit.
    BodyLimit,
    /// An earlier filter of the request was cancelled during a callout wait.
    CancelledWait,
    /// The plugin sent a response after the response header.
    LateResponse,
}

impl PluginFailure {
    /// Return the failure as a snake_case string, e.g. `guest_error`.
    ///
    /// `Display` writes the same string.
    pub fn as_str(&self) -> &'static str {
        match self {
            PluginFailure::GuestError => "guest_error",
            PluginFailure::Unavailable => "unavailable",
            PluginFailure::GuestLost => "guest_lost",
            PluginFailure::PausedWithoutCallout => "paused_without_callout",
            PluginFailure::WaitLimit => "wait_limit",
            PluginFailure::BodyChanged => "body_changed",
            PluginFailure::BodyLimit => "body_limit",
            PluginFailure::CancelledWait => "cancelled_wait",
            PluginFailure::LateResponse => "late_response",
        }
    }
}

impl fmt::Display for PluginFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a plugin failure did to its request.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PluginFailureOutcome {
    /// The request failed.
    ///
    /// Also the outcome of a failure outside of a request, e.g. in `proxy_on_tick`, of a
    /// failure in [WasmCtx::logging](crate::WasmCtx::logging), and of a failure in
    /// [WasmCtx::response_trailer_filter](crate::WasmCtx::response_trailer_filter) that does not
    /// skip the plugin.
    Failed,
    /// The plugin was skipped for the rest of the request, which continued without it.
    Skipped,
}

impl PluginFailureOutcome {
    /// Return the outcome as a lowercase string, `failed` or `skipped`.
    ///
    /// `Display` writes the same string.
    pub fn as_str(&self) -> &'static str {
        match self {
            PluginFailureOutcome::Failed => "failed",
            PluginFailureOutcome::Skipped => "skipped",
        }
    }
}

impl fmt::Display for PluginFailureOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One plugin failure, as passed to [WasmMetricSink::plugin_failed].
///
/// [WasmMetricSink::plugin_failed]: crate::WasmMetricSink::plugin_failed
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct PluginFailureReport<'a> {
    /// The name of the plugin that failed.
    pub plugin_name: &'a str,
    /// The kind of failure.
    pub failure: PluginFailure,
    /// What the failure did to the request.
    pub outcome: PluginFailureOutcome,
    /// The ABI name of the callback the failure belongs to, e.g. `proxy_on_request_headers`.
    ///
    /// For a callback that trapped or returned an error, this is that callback. For a pause, a
    /// wait limit, a body limit, or a late response, it is the callback of the filter in which
    /// the plugin paused, waited, held the body, or responded. `None` when the plugin had no
    /// guest, when its guest was lost, and after a cancelled wait.
    pub callback: Option<&'static str>,
}

impl<'a> PluginFailureReport<'a> {
    /// Create a report with no callback.
    ///
    /// The struct is non-exhaustive, so use this to build a report in your own code, e.g. to test
    /// your [WasmMetricSink](crate::WasmMetricSink). Set [callback](Self::callback) on the result
    /// if you need one.
    pub fn new(
        plugin_name: &'a str,
        failure: PluginFailure,
        outcome: PluginFailureOutcome,
    ) -> Self {
        PluginFailureReport {
            plugin_name,
            failure,
            outcome,
            callback: None,
        }
    }
}
