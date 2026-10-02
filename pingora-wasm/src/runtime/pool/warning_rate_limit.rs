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

//! Rate limiting for repeated warnings
//!
//! A plugin failing on every request would log a warning for each one. Its pool warns at most
//! once every 10 seconds and reports how many events the warning covers.

use parking_lot::Mutex;
use std::time::{Duration, Instant};

const WARNING_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Default)]
struct WarningState {
    last_warning: Option<Instant>,
    events_since_last_warning: u64,
}

#[derive(Default)]
pub(crate) struct WarningRateLimit {
    state: Mutex<WarningState>,
}

impl WarningRateLimit {
    /// Count an event at `now` and return whether to warn about it.
    ///
    /// If a warning is due, returns the number of events since the last warning, including this
    /// one.
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
