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

//! Request body replay on retry
//!
//! When Pingora retries a request it replays the request body it buffered. The plugins have
//! already run on those bytes, so their output is kept here and sent again without running them
//! a second time.

use crate::chain::WasmCtx;
use bytes::{Bytes, BytesMut};
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_proxy::Session;

/// Progress of the request body through the plugins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestBodyProgress {
    /// The request has no body, or the request header phase has not run.
    Absent,
    /// A body is expected, but no chunk has been run through the plugins yet.
    Waiting,
    Streaming,
    Ended,
}

/// Request body filter state, including the plugin output kept for a retry.
#[derive(Debug)]
pub(crate) struct RequestBodyState {
    pub(super) progress: RequestBodyProgress,
    pub(super) attempts: usize,
    /// Set when a retry starts, as its first body chunk may be a replay of bytes the plugins
    /// have already run on.
    pub(super) replay_due: bool,
    /// Plugin output so far, sent again on a retry. `None` once Pingora has truncated its own
    /// retry buffer.
    pub(super) kept: Option<BytesMut>,
}

impl RequestBodyState {
    pub(crate) fn new() -> Self {
        RequestBodyState {
            progress: RequestBodyProgress::Absent,
            attempts: 0,
            replay_due: false,
            kept: Some(BytesMut::new()),
        }
    }

    /// Return `true` if the request has a body whose last chunk has not been run through the
    /// plugins yet.
    pub(crate) fn is_unfinished(&self) -> bool {
        matches!(
            self.progress,
            RequestBodyProgress::Waiting | RequestBodyProgress::Streaming
        )
    }

    /// Mark the request as having a body.
    pub(crate) fn expect_body(&mut self) {
        self.progress = RequestBodyProgress::Waiting;
    }
}

impl WasmCtx {
    /// Record the start of an upstream attempt.
    ///
    /// Call this from your `upstream_peer`, which Pingora runs once per attempt.
    ///
    /// When Pingora retries a request it replays the request body it buffered through
    /// `request_body_filter`. The plugins have already run on those bytes, so
    /// [WasmCtx::request_body_filter] sends the upstream what they produced the first time. It
    /// relies on this call to tell a retry from the first attempt, and returns an error on a
    /// request body if the call was never made.
    pub fn upstream_attempt(&mut self) {
        self.request_body.attempts += 1;
        self.request_body.replay_due = self.request_body.attempts > 1;
    }

    /// Append `output` to the bytes kept for a retry.
    ///
    /// Everything kept is dropped once Pingora has truncated its own retry buffer, since the body
    /// can no longer be replayed.
    pub(super) fn keep_for_retry<DS: DownstreamSession>(
        &mut self,
        session: &Session<DS>,
        output: &Bytes,
    ) {
        if session.as_downstream().retry_buffer_truncated() {
            self.request_body.kept = None;
        } else if let Some(kept) = self.request_body.kept.as_mut() {
            kept.extend_from_slice(output);
        }
    }
}
