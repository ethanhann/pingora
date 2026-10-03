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

//! Guest rebuild

use super::{install_started_guest, GuestPool};
use log::{error, warn};
use proxy_wasm_host::abi::v0_2_1::GuestError;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

// `Instant + Duration` panics on overflow
const LONGEST_REBUILD_WAIT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

impl GuestPool {
    pub(super) fn rebuild_due(&self, index: usize) -> bool {
        let slot = &self.slots[index];
        let due = Instant::now() >= *slot.next_build.lock();
        due && slot.guest.try_lock().is_some_and(|guard| guard.is_none())
    }

    /// Return when a slot built now may be built again, after 1 to 1.5 rebuild intervals.
    pub(super) fn next_build_time(&self) -> Instant {
        let fraction = RandomState::new().build_hasher().finish() % 1000;
        let jitter = self.rebuild_interval.mul_f64(fraction as f64 / 2000.0);
        let wait = self.rebuild_interval.saturating_add(jitter);
        Instant::now() + wait.min(LONGEST_REBUILD_WAIT)
    }

    /// Start a new guest in an empty slot.
    ///
    /// `failure` is the error that emptied the slot when the rebuild immediately follows it, and
    /// is only used for logging.
    pub(super) fn rebuild(&self, index: usize, failure: Option<&GuestError>) {
        if self.rebuilds_stopped.load(Ordering::Relaxed) {
            return;
        }
        let slot = &self.slots[index];
        let name = &self.name;
        *slot.next_build.lock() = self.next_build_time();
        match self.start_guest(index) {
            Ok(started) => {
                let mut guard = slot.guest.lock();
                if guard.is_none() {
                    slot.open.store(0, Ordering::Relaxed);
                    slot.held.store(0, Ordering::Relaxed);
                    install_started_guest(&mut guard, started);
                    drop(guard);
                    self.metric_sink.guest_replaced(name);
                    let warnings = &self.replaced_guest_warnings;
                    let Some(replaced) = warnings.count_event(Instant::now()) else {
                        return;
                    };
                    let what = match failure {
                        Some(failure) => format!("replaced after failure: {failure}"),
                        None => "rebuilt".to_string(),
                    };
                    let count = match replaced {
                        1 => String::new(),
                        replaced => format!(", {replaced} guests replaced since the last warning"),
                    };
                    warn!("wasm plugin {name}: guest in slot {index} {what}{count}");
                }
            }
            Err(e) => {
                match failure {
                    Some(failure) => error!(
                        "wasm plugin {name}: guest in slot {index} lost after failure: {failure}, rebuild failed: {e}"
                    ),
                    None => error!("wasm plugin {name}: failed to rebuild slot {index}: {e}"),
                }
            }
        }
    }
}
