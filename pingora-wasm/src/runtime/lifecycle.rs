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

//! Runtime lifecycle

use once_cell::sync::OnceCell;
use pingora_error::Result;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Default)]
pub(crate) struct Lifecycle {
    /// Whether the threads were started, set once by the first start or by the end.
    threads_started: OnceCell<bool>,
    ending: AtomicBool,
    live_ctxs: AtomicUsize,
    last_ctx_dropped: Notify,
}

impl Lifecycle {
    pub(crate) fn start_threads_once(&self, start: impl FnOnce() -> Result<()>) -> Result<()> {
        self.threads_started.get_or_try_init(|| -> Result<bool> {
            if self.ending.load(Ordering::Acquire) {
                return Ok(false);
            }
            start()?;
            Ok(true)
        })?;
        Ok(())
    }

    /// Mark the runtime as ending, and return whether its threads were started.
    ///
    /// A start that is in progress finishes first. Once this returns, no filter starts the
    /// threads.
    pub(crate) fn begin_end(&self) -> bool {
        self.ending.store(true, Ordering::Release);
        *self.threads_started.get_or_init(|| false)
    }

    pub(crate) fn is_ending(&self) -> bool {
        self.ending.load(Ordering::Acquire)
    }

    pub(crate) fn ctx_created(&self) {
        self.live_ctxs.fetch_add(1, Ordering::AcqRel);
    }

    pub(crate) fn ctx_dropped(&self) {
        if self.live_ctxs.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.last_ctx_dropped.notify_waiters();
        }
    }

    pub(crate) async fn wait_for_no_live_ctx(&self) {
        loop {
            // Enabled before the count is read, so a drop between the two still wakes this task
            let mut dropped = pin!(self.last_ctx_dropped.notified());
            dropped.as_mut().enable();
            if self.live_ctxs.load(Ordering::Acquire) == 0 {
                return;
            }
            dropped.await;
        }
    }
}
