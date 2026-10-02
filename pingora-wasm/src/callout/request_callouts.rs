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

#[derive(Debug)]
pub(crate) enum PendingResult {
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
/// Callouts made during a guest call are kept in `accepted` until the filter starts their tasks.
/// If the plugin is paused at that point, the started callouts move to `pending`, where the
/// filter waits for their results.
#[derive(Default)]
pub(crate) struct RequestCallouts {
    accepted: Vec<AcceptedCallout>,
    pending: Vec<PendingCallout>,
    /// The chain position of the plugin a filter is waiting on for a callout result.
    pub(crate) waiting_position: Option<usize>,
}

impl RequestCallouts {
    pub(crate) fn set_accepted(&mut self, accepted: Vec<AcceptedCallout>) {
        self.accepted = accepted;
    }

    pub(crate) fn take_accepted(&mut self) -> Vec<AcceptedCallout> {
        mem::take(&mut self.accepted)
    }

    pub(crate) fn add_pending(&mut self, position: usize, id: CalloutId, result: PendingResult) {
        self.pending.push(PendingCallout {
            position,
            id,
            result,
        });
    }

    pub(crate) fn has_pending(&self, position: usize) -> bool {
        self.pending.iter().any(|p| p.position == position)
    }

    pub(crate) fn forget_pending(&mut self, position: usize) {
        self.pending.retain(|p| p.position != position);
    }

    pub(crate) fn clear(&mut self) {
        // Tasks that are already running are not cancelled
        self.accepted.clear();
        self.pending.clear();
    }

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
                    // A task that panicked or was cancelled yields `Failed`
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
