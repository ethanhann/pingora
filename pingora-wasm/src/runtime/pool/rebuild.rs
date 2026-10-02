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
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

const REBUILD_BACKOFF: Duration = Duration::from_secs(1);

impl GuestPool {
    pub(super) fn rebuild_due(&self, index: usize) -> bool {
        let slot = &self.slots[index];
        let due = slot
            .failed_at
            .lock()
            .is_none_or(|at| at.elapsed() >= REBUILD_BACKOFF);
        due && slot.guest.try_lock().is_some_and(|guard| guard.is_none())
    }

    /// Start a new guest in an empty slot.
    ///
    /// `failure` is the error that emptied the slot when the rebuild immediately follows it, and
    /// is only used for logging.
    pub(super) fn rebuild(&self, index: usize, failure: Option<&GuestError>) {
        let slot = &self.slots[index];
        let name = &self.name;
        match self.start_guest(index) {
            Ok(started) => {
                let mut guard = slot.guest.lock();
                if guard.is_none() {
                    slot.open.store(0, Ordering::Relaxed);
                    slot.held.store(0, Ordering::Relaxed);
                    install_started_guest(&mut guard, started);
                    *slot.failed_at.lock() = None;
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
                *slot.failed_at.lock() = Some(Instant::now());
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
