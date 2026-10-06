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

//! The two directions of a TCP connection

use proxy_wasm_host::abi::v0_2_1::types::StreamType;
use proxy_wasm_host::abi::v0_2_1::Callback;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Direction {
    Downstream,
    Upstream,
}

impl Direction {
    pub(super) const BOTH: [Direction; 2] = [Direction::Downstream, Direction::Upstream];

    pub(super) fn index(self) -> usize {
        match self {
            Direction::Downstream => 0,
            Direction::Upstream => 1,
        }
    }

    pub(super) fn stream_type(self) -> StreamType {
        match self {
            Direction::Downstream => StreamType::Downstream,
            Direction::Upstream => StreamType::Upstream,
        }
    }

    pub(super) fn data_callback(self) -> Callback {
        match self {
            Direction::Downstream => Callback::DownstreamData,
            Direction::Upstream => Callback::UpstreamData,
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Direction::Downstream => "downstream",
            Direction::Upstream => "upstream",
        }
    }

    pub(super) fn positions_after(self, plugins: usize, after: Option<usize>) -> Vec<usize> {
        // Upstream data runs the plugins in reverse chain order, as the response of a request does
        match (self, after) {
            (Direction::Downstream, None) => (0..plugins).collect(),
            (Direction::Downstream, Some(position)) => (position + 1..plugins).collect(),
            (Direction::Upstream, None) => (0..plugins).rev().collect(),
            (Direction::Upstream, Some(position)) => (0..position).rev().collect(),
        }
    }
}

/// The bytes of one direction that the plugin at one position paused.
#[derive(Debug, Default)]
pub(super) struct HeldData {
    pub(super) bytes: Vec<u8>,
    /// Whether the end of the direction came with the bytes.
    pub(super) end: bool,
    pub(super) paused: bool,
}

#[derive(Debug)]
pub(super) struct DirectionState {
    pub(super) held: Vec<HeldData>,
    pub(super) changed: Vec<bool>,
    /// Whether the end of the direction has passed the last plugin.
    pub(super) ended: bool,
}

impl DirectionState {
    pub(super) fn new(plugins: usize) -> Self {
        DirectionState {
            held: (0..plugins).map(|_| HeldData::default()).collect(),
            changed: vec![false; plugins],
            ended: false,
        }
    }

    pub(super) fn held_bytes(&self) -> usize {
        self.held.iter().map(|held| held.bytes.len()).sum()
    }
}
