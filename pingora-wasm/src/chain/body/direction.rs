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

//! Body direction
//!
//! [BodyDirection] selects the request body or the response body, and provides everything that
//! differs between the two in the shared body pass.

use crate::chain::wait::PausedPhase;
use crate::runtime::pool::PluginPhases;
use crate::{ERR_REQUEST_BODY_TOO_LARGE, ERR_RESPONSE_BODY_TOO_LARGE};
use pingora_error::ErrorType;
use proxy_wasm_host::abi::v0_2_1::types::StreamType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BodyDirection {
    Request,
    Response,
}

impl BodyDirection {
    pub(super) fn runs(self, conf: &PluginPhases) -> bool {
        match self {
            BodyDirection::Request => conf.request,
            BodyDirection::Response => conf.response,
        }
    }

    pub(super) fn limit(self, conf: &PluginPhases) -> usize {
        match self {
            BodyDirection::Request => conf.request_limit,
            BodyDirection::Response => conf.response_limit,
        }
    }

    pub(super) fn too_large(self) -> ErrorType {
        match self {
            BodyDirection::Request => ERR_REQUEST_BODY_TOO_LARGE,
            BodyDirection::Response => ERR_RESPONSE_BODY_TOO_LARGE,
        }
    }

    pub(super) fn stream_type(self) -> StreamType {
        match self {
            BodyDirection::Request => StreamType::HttpRequest,
            BodyDirection::Response => StreamType::HttpResponse,
        }
    }

    pub(super) fn paused_phase(self) -> PausedPhase<'static> {
        match self {
            BodyDirection::Request => PausedPhase::RequestBody,
            BodyDirection::Response => PausedPhase::ResponseBody,
        }
    }

    /// Return the chain position of the plugin that runs at `step` of a pass over `count` plugins.
    ///
    /// Request bodies run in chain order and response bodies in reverse.
    pub(super) fn position_at_step(self, step: usize, count: usize) -> usize {
        match self {
            BodyDirection::Request => step,
            BodyDirection::Response => count - 1 - step,
        }
    }

    pub(super) fn failure(self) -> &'static str {
        match self {
            BodyDirection::Request => "proxy_on_request_body failed",
            BodyDirection::Response => "proxy_on_response_body failed",
        }
    }
}
