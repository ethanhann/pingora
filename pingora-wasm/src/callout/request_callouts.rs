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

//! Per-request callouts

use super::{AcceptedCallout, CalloutResult};
use proxy_wasm_host::abi::v0_2_1::CalloutId;
use std::future::{poll_fn, Future};
use std::mem;
use std::pin::Pin;
use std::task::Poll;
use tokio::task::JoinHandle;

/// The result of a started callout that has not been delivered to the plugin yet.
#[derive(Debug)]
pub(crate) enum PendingResult {
    /// The result will be returned by the task sending the callout.
    FromTask(JoinHandle<CalloutResult>),
    /// The result was decided without sending the callout.
    Known(CalloutResult),
}

struct PendingCallout {
    position: usize,
    id: CalloutId,
    result: PendingResult,
}

/// Callouts made by the plugins of one request.
///
/// Callouts made during a guest call are kept in `accepted` until the phase starts their tasks.
/// If the plugin is paused at that point, the started callouts move to `pending`, where the
/// phase waits for their results. Both lists stay empty, and allocate nothing, for a request
/// that makes no callouts.
#[derive(Default)]
pub(crate) struct RequestCallouts {
    accepted: Vec<AcceptedCallout>,
    pending: Vec<PendingCallout>,
    /// Whether a phase is waiting for a callout result. This is left `true` if the phase's
    /// future is dropped mid-wait, which makes the following phases fail the request.
    pub(crate) in_callout_wait: bool,
}

impl RequestCallouts {
    /// Replace the accepted callouts with those of the latest guest call.
    pub(crate) fn set_accepted(&mut self, accepted: Vec<AcceptedCallout>) {
        self.accepted = accepted;
    }

    /// Take the callouts accepted during the latest guest call.
    pub(crate) fn take_accepted(&mut self) -> Vec<AcceptedCallout> {
        mem::take(&mut self.accepted)
    }

    /// Track a started callout of the plugin at `position` so the phase can wait for its result.
    pub(crate) fn add_pending(&mut self, position: usize, id: CalloutId, result: PendingResult) {
        self.pending.push(PendingCallout {
            position,
            id,
            result,
        });
    }

    /// Return `true` if the plugin at `position` has a pending callout.
    pub(crate) fn has_pending(&self, position: usize) -> bool {
        self.pending.iter().any(|p| p.position == position)
    }

    /// Stop tracking the pending callouts of the plugin at `position`.
    ///
    /// Their tasks keep running.
    pub(crate) fn forget_pending(&mut self, position: usize) {
        self.pending.retain(|p| p.position != position);
    }

    /// Drop every callout of the request.
    ///
    /// Tasks that are already running are not cancelled.
    pub(crate) fn clear(&mut self) {
        self.accepted.clear();
        self.pending.clear();
    }

    /// Wait for the next pending callout of the plugin at `position` to finish.
    ///
    /// Returns `None` if the plugin has no pending callout. A task that panicked or was
    /// cancelled yields [CalloutResult::Failed].
    pub(crate) async fn next_result(
        &mut self,
        position: usize,
    ) -> Option<(CalloutId, CalloutResult)> {
        if !self.has_pending(position) {
            return None;
        }
        let (index, result) = poll_fn(|cx| {
            for (index, pending) in self.pending.iter_mut().enumerate() {
                if pending.position != position {
                    continue;
                }
                let ready = match &mut pending.result {
                    PendingResult::Known(result) => Poll::Ready(result.clone()),
                    PendingResult::FromTask(task) => Pin::new(task)
                        .poll(cx)
                        .map(|joined| joined.unwrap_or(CalloutResult::Failed)),
                };
                if let Poll::Ready(result) = ready {
                    return Poll::Ready((index, result));
                }
            }
            Poll::Pending
        })
        .await;
        let callout = self.pending.remove(index);
        Some((callout.id, result))
    }
}
