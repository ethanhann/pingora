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

use super::BodyDirection;
use crate::chain::failure::FilterFailure;
use crate::chain::WasmCtx;
use bytes::Bytes;
use pingora_error::Result;
use std::mem;

/// Body bytes held back for paused plugins, indexed by chain position.
#[derive(Debug, Default)]
pub(crate) struct HeldBodies {
    request: Vec<Vec<u8>>,
    response: Vec<Vec<u8>>,
}

impl HeldBodies {
    fn list(&mut self, direction: BodyDirection) -> &mut Vec<Vec<u8>> {
        match direction {
            BodyDirection::Request => &mut self.request,
            BodyDirection::Response => &mut self.response,
        }
    }

    pub(crate) fn take(&mut self, direction: BodyDirection, position: usize) -> Vec<u8> {
        match self.list(direction).get_mut(position) {
            Some(bytes) => mem::take(bytes),
            None => Vec::new(),
        }
    }

    pub(crate) fn put(&mut self, direction: BodyDirection, position: usize, bytes: Vec<u8>) {
        let list = self.list(direction);
        if list.len() <= position {
            // Each list only grows once a plugin has bytes to hold, so a request where nothing
            // is held allocates nothing
            if bytes.is_empty() {
                return;
            }
            list.resize_with(position + 1, Vec::new);
        }
        list[position] = bytes;
    }

    pub(crate) fn take_response(&mut self) -> Vec<Vec<u8>> {
        mem::take(&mut self.response)
    }

    pub(crate) fn len(&self, direction: BodyDirection, position: usize) -> usize {
        let list = match direction {
            BodyDirection::Request => &self.request,
            BodyDirection::Response => &self.response,
        };
        list.get(position).map_or(0, Vec::len)
    }
}

/// Whether a plugin that paused on a body chunk keeps holding its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BodyHold {
    Continues,
    EndedBySkip,
}

impl WasmCtx {
    pub(super) fn hold_body_or_skip_plugin(
        &mut self,
        direction: BodyDirection,
        position: usize,
        end_of_stream: bool,
    ) -> Result<BodyHold> {
        if end_of_stream {
            // No later chunk can release the bytes, so a pause here is a plugin failure
            let what = "paused on the last body chunk with no callout pending";
            let failure = FilterFailure::paused(direction.callback(), what);
            self.skip_plugin_or_fail_request(position, failure)?;
            return Ok(BodyHold::EndedBySkip);
        }
        let size = self.held.len(direction, position);
        let limit = direction.limit(&self.pool_at(position).phases);
        if size > limit {
            let failure = FilterFailure::body_limit(direction, size, limit);
            return Err(self.failed_request_error(position, failure));
        }
        Ok(BodyHold::Continues)
    }

    pub(super) fn prepend_held_bytes(
        &mut self,
        direction: BodyDirection,
        position: usize,
        chunk: Bytes,
    ) -> Bytes {
        let mut held = self.held.take(direction, position);
        if held.is_empty() {
            return chunk;
        }
        held.extend_from_slice(&chunk);
        Bytes::from(held)
    }
}
