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

//! The request body that a retry sends again.

use crate::chain::WasmCtx;
use bytes::{Bytes, BytesMut};
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_proxy::Session;

/// The progress of a request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestBodyProgress {
    /// The request has no body, or the request headers did not run.
    Absent,
    /// The plugins did not run on a chunk.
    Waiting,
    Streaming,
    Ended,
}

/// The state of the request body phase.
#[derive(Debug)]
pub(crate) struct RequestBodyState {
    pub(super) progress: RequestBodyProgress,
    pub(super) attempts: usize,
    /// Set when a retry starts, because its first body call can repeat earlier bytes.
    pub(super) replay_due: bool,
    /// The output of the plugins, which a retry sends again.
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

    /// Record that the request has a body.
    pub(crate) fn expect_body(&mut self) {
        self.progress = RequestBodyProgress::Waiting;
    }
}

impl WasmCtx {
    /// Record the start of an upstream attempt.
    ///
    /// Call it from `upstream_peer`, which Pingora runs once for each attempt.
    ///
    /// When Pingora retries a request, it sends the request body that it kept through
    /// `request_body_filter` again. The plugins already ran on those bytes, so
    /// [WasmCtx::request_body_filter] sends the upstream what they returned the first time. It
    /// needs this call to know that a retry start_request, and it returns an error for a request body
    /// when the call is missing.
    pub fn upstream_attempt(&mut self) {
        self.request_body.attempts += 1;
        self.request_body.replay_due = self.request_body.attempts > 1;
    }

    /// Keep `output` for a retry, while Pingora keeps the request body for one.
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
