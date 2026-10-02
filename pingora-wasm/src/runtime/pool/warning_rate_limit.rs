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

//! Warning rate limit
//!
//! A plugin that fails on every request gets a new guest each time, and with
//! `FailPolicy::Open` it is also skipped each time. To limit the log volume, the warning for a
//! replaced guest and the warning for a skipped plugin are each written at most once per
//! interval for a plugin, with the number of events since the previous warning if there was more
//! than one.

use parking_lot::Mutex;
use std::time::{Duration, Instant};

/// Minimum time between two warnings of the same kind for one plugin.
const WARNING_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Default)]
struct WarningState {
    last_warning: Option<Instant>,
    events_since_last_warning: u64,
}

/// Rate limit for one kind of warning from one plugin.
#[derive(Default)]
pub(crate) struct WarningRateLimit {
    state: Mutex<WarningState>,
}

impl WarningRateLimit {
    /// Count an event at `now` and return whether to warn about it.
    ///
    /// If a warning is due, returns the number of events since the last warning, including this
    /// one. Returns `None` if the last warning was less than [WARNING_INTERVAL] ago, in which
    /// case the event is counted towards the next warning.
    pub(crate) fn count_event(&self, now: Instant) -> Option<u64> {
        let mut state = self.state.lock();
        state.events_since_last_warning += 1;
        let due = state
            .last_warning
            .is_none_or(|last| now.saturating_duration_since(last) >= WARNING_INTERVAL);
        if !due {
            return None;
        }
        state.last_warning = Some(now);
        Some(std::mem::take(&mut state.events_since_last_warning))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn warning_is_due_once_per_interval_with_count() {
        let warnings = WarningRateLimit::default();
        let start = Instant::now();
        let seconds_after_start = [0, 1, 9, 10, 11, 25];

        let due = seconds_after_start
            .map(|seconds| warnings.count_event(start + Duration::from_secs(seconds)));

        assert_eq!(due, [Some(1), None, None, Some(3), None, Some(2)]);
    }
}
