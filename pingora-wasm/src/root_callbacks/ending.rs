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

//! End of the root contexts

use super::callback_loop::RootCallbackLoop;
use super::work::{Work, WorkOutcome};
use crate::runtime::pool::events::GuestAddress;
use crate::runtime::pool::SlotLockAttempt;
use crate::runtime::RuntimeInner;
use proxy_wasm_host::abi::v0_2_1::ContextState;
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::watch;

/// How far the end of a runtime's plugins has come.
#[derive(Debug, Clone, Default)]
pub(crate) struct EndProgress {
    pub(crate) finished: bool,
    /// The plugins with a root context that has not finished.
    pub(crate) waiting_for: Vec<Arc<str>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum EndStep {
    CallDone,
    WaitForDone,
    Delete,
}

pub(super) struct Ending {
    progress: watch::Sender<EndProgress>,
    step: EndStep,
    roots: Vec<GuestAddress>,
    /// Roots whose `proxy_on_done` failed. They cannot finish, so the end does not wait for them.
    pub(super) failed_roots: Vec<GuestAddress>,
}

impl Ending {
    pub(super) fn new(progress: watch::Sender<EndProgress>) -> Self {
        Ending {
            progress,
            step: EndStep::CallDone,
            roots: Vec::new(),
            failed_roots: Vec::new(),
        }
    }
}

impl RootCallbackLoop {
    /// Move the end of the plugins forward, and return `true` once it has finished.
    pub(super) fn advance_end(&mut self, runtime: &RuntimeInner) -> bool {
        let Some(step) = self.ending.as_ref().map(|ending| ending.step) else {
            return false;
        };
        match step {
            EndStep::CallDone => {
                let roots = runtime
                    .pools
                    .iter()
                    .flat_map(|pool| pool.guest_addresses())
                    .collect::<Vec<_>>();
                self.run_on_each_root(runtime, &roots, Work::EndRoot);
                self.set_end_step(EndStep::WaitForDone, roots);
                self.advance_end(runtime)
            }
            EndStep::WaitForDone => {
                let roots = self.end_roots();
                let waiting_for: BTreeSet<_> = roots
                    .iter()
                    .filter(|address| !is_root_finished(runtime, address))
                    .map(|address| runtime.pools[address.slot.pool_index].name.clone())
                    .collect();
                if !waiting_for.is_empty() {
                    self.report_end_progress(false, waiting_for.into_iter().collect());
                    return false;
                }
                self.run_on_each_root(runtime, &roots, Work::DeleteRoot);
                self.set_end_step(EndStep::Delete, roots);
                self.advance_end(runtime)
            }
            EndStep::Delete => {
                if !self.retries_are_empty() {
                    return false;
                }
                self.report_end_progress(true, Vec::new());
                true
            }
        }
    }

    fn run_on_each_root(
        &mut self,
        runtime: &RuntimeInner,
        roots: &[GuestAddress],
        work: fn(GuestAddress) -> Work,
    ) {
        for address in roots {
            let work = work(*address);
            if let WorkOutcome::SlotBusy = self.run_work(runtime, &work) {
                self.retry_later(work);
            }
        }
    }

    /// Return the roots that the end still runs callbacks on.
    fn end_roots(&self) -> Vec<GuestAddress> {
        let Some(ending) = &self.ending else {
            return Vec::new();
        };
        let roots = ending.roots.iter().copied();
        roots
            .filter(|address| !ending.failed_roots.contains(address))
            .collect()
    }

    fn set_end_step(&mut self, step: EndStep, roots: Vec<GuestAddress>) {
        if let Some(ending) = &mut self.ending {
            ending.step = step;
            ending.roots = roots;
        }
    }

    fn report_end_progress(&self, finished: bool, waiting_for: Vec<Arc<str>>) {
        if let Some(ending) = &self.ending {
            ending.progress.send_replace(EndProgress {
                finished,
                waiting_for,
            });
        }
    }
}

/// Return whether the root at `address` is done and has no stream context left, or is gone.
fn is_root_finished(runtime: &RuntimeInner, address: &GuestAddress) -> bool {
    let pool = &runtime.pools[address.slot.pool_index];
    match pool.try_lock_guest(address.slot.slot_index, address.guest) {
        SlotLockAttempt::LockedGuest(guard) => guard.as_ref().is_none_or(|loaded| {
            let guest = &loaded.guest;
            guest.context_state(loaded.root) == Some(ContextState::Done)
                && !guest.has_stream_contexts(loaded.root)
        }),
        SlotLockAttempt::Busy => false,
        SlotLockAttempt::GuestGone => true,
    }
}
