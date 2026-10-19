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

//! Stream state of a TCP connection

use super::BodyBuffer;
use proxy_wasm_host::abi::v0_2_1::types::StreamType;

/// The data buffers and the continue and close calls of a plugin during one callback on a TCP
/// connection.
///
/// A buffer is `Some` while the plugin can read and change that direction's data, in its data
/// callback or in a callout delivery while it has that direction paused.
#[derive(Default)]
pub(crate) struct TcpCallbackState {
    pub(crate) downstream_data: Option<BodyBuffer>,
    pub(crate) upstream_data: Option<BodyBuffer>,
    continue_downstream: bool,
    continue_upstream: bool,
    close_requested: bool,
}

impl TcpCallbackState {
    pub(crate) fn data_mut(&mut self, direction: StreamType) -> &mut Option<BodyBuffer> {
        match direction {
            StreamType::Upstream => &mut self.upstream_data,
            StreamType::Downstream => &mut self.downstream_data,
            StreamType::HttpRequest | StreamType::HttpResponse => {
                unreachable!("a TCP connection has data for the two TCP stream types only")
            }
        }
    }

    pub(crate) fn request_continue(&mut self, direction: StreamType) {
        match direction {
            StreamType::Downstream => self.continue_downstream = true,
            StreamType::Upstream => self.continue_upstream = true,
            // A TCP connection has no HTTP stream to continue
            StreamType::HttpRequest | StreamType::HttpResponse => {}
        }
    }

    pub(crate) fn continue_requested(&self, direction: StreamType) -> bool {
        match direction {
            StreamType::Downstream => self.continue_downstream,
            StreamType::Upstream => self.continue_upstream,
            _ => false,
        }
    }

    pub(crate) fn clear_requests(&mut self) {
        self.continue_downstream = false;
        self.continue_upstream = false;
        self.close_requested = false;
    }

    pub(crate) fn request_close(&mut self, direction: StreamType) {
        if matches!(direction, StreamType::Downstream | StreamType::Upstream) {
            self.close_requested = true;
        }
    }

    pub(crate) fn take_close_request(&mut self) -> bool {
        std::mem::take(&mut self.close_requested)
    }
}
