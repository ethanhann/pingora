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
use std::mem;

/// The body bytes that the plugins hold, by position in the chain.
///
/// A list stays empty until a plugin holds bytes.
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
            if bytes.is_empty() {
                return;
            }
            list.resize_with(position + 1, Vec::new);
        }
        list[position] = bytes;
    }

    /// Take the response bytes that the plugins hold, in chain order.
    pub(crate) fn take_response(&mut self) -> Vec<Vec<u8>> {
        mem::take(&mut self.response)
    }

    /// Return the number of bytes that the plugin at `position` holds.
    pub(crate) fn len(&self, direction: BodyDirection, position: usize) -> usize {
        let list = match direction {
            BodyDirection::Request => &self.request,
            BodyDirection::Response => &self.response,
        };
        list.get(position).map_or(0, Vec::len)
    }

    pub(crate) fn request_len(&self) -> usize {
        self.request.iter().map(Vec::len).sum()
    }

    pub(crate) fn response_len(&self) -> usize {
        self.response.iter().map(Vec::len).sum()
    }
}
