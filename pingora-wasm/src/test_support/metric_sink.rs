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

//! Recording metric sink for tests

use crate::{
    CalloutFailure, PluginFailure, PluginFailureOutcome, PluginFailureReport, WasmMetricSink,
};
use parking_lot::Mutex;

/// A failure report as [RecordedFailures] stores it, made up of the plugin name, the failure, the
/// outcome, and the callback.
pub(crate) type RecordedFailure = (String, PluginFailure, PluginFailureOutcome, Option<String>);

/// A metric sink that records every plugin failure, callout failure, and replaced guest.
#[derive(Default)]
pub(crate) struct RecordedFailures {
    failures: Mutex<Vec<RecordedFailure>>,
    callouts: Mutex<Vec<CalloutFailure>>,
    replaced: Mutex<Vec<String>>,
}

impl RecordedFailures {
    pub(crate) fn failures(&self) -> Vec<RecordedFailure> {
        self.failures.lock().clone()
    }

    pub(crate) fn callout_failures(&self) -> Vec<CalloutFailure> {
        self.callouts.lock().clone()
    }

    /// Return the plugin name of each replaced guest, in the order they were reported.
    pub(crate) fn replaced_guests(&self) -> Vec<String> {
        self.replaced.lock().clone()
    }
}

impl WasmMetricSink for RecordedFailures {
    fn plugin_failed(&self, report: &PluginFailureReport<'_>) {
        self.failures.lock().push((
            report.plugin_name.to_string(),
            report.failure,
            report.outcome,
            report.callback.map(str::to_string),
        ));
    }

    fn callout_failed(&self, _plugin_name: &str, failure: CalloutFailure) {
        self.callouts.lock().push(failure);
    }

    fn guest_replaced(&self, plugin_name: &str) {
        self.replaced.lock().push(plugin_name.to_string());
    }
}
