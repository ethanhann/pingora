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

//! The body buffer that a guest reads and writes.

use bytes::Bytes;
use proxy_wasm_host::{Buffer, NotAllowed};

/// The body bytes that a guest reads and writes in one callback.
///
/// A chunk stays a shared `Bytes` until a guest writes to it or holds it, so a guest that only
/// reads a chunk makes no copy.
#[derive(Debug)]
pub(crate) enum BodyBuffer {
    Shared(Bytes),
    Owned(Vec<u8>),
}

impl Default for BodyBuffer {
    fn default() -> Self {
        BodyBuffer::Shared(Bytes::new())
    }
}

impl BodyBuffer {
    /// Create the buffer from the bytes that the plugin holds and the new chunk.
    pub(crate) fn new(mut held: Vec<u8>, chunk: Bytes) -> Self {
        if held.is_empty() {
            return BodyBuffer::Shared(chunk);
        }
        held.extend_from_slice(&chunk);
        BodyBuffer::Owned(held)
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        match self {
            BodyBuffer::Shared(bytes) => bytes,
            BodyBuffer::Owned(bytes) => bytes.into(),
        }
    }

    pub(crate) fn into_vec(self) -> Vec<u8> {
        match self {
            BodyBuffer::Shared(bytes) => bytes.into(),
            BodyBuffer::Owned(bytes) => bytes,
        }
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            BodyBuffer::Shared(bytes) => bytes,
            BodyBuffer::Owned(bytes) => bytes,
        }
    }
}

impl Buffer for BodyBuffer {
    fn len(&self) -> usize {
        self.as_slice().len()
    }

    fn copy_range_into(&self, start: usize, max_size: usize, out: &mut Vec<u8>) {
        let bytes = self.as_slice();
        let start = start.min(bytes.len());
        let end = start.saturating_add(max_size).min(bytes.len());
        out.extend_from_slice(&bytes[start..end]);
    }

    fn replace(&mut self, start: usize, size: usize, value: &[u8]) -> Result<(), NotAllowed> {
        let mut owned = std::mem::take(self).into_vec();
        let result = owned.replace(start, size, value);
        *self = BodyBuffer::Owned(owned);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_keeps_the_shared_chunk() {
        let chunk = Bytes::from_static(b"0123456789");
        let buffer = BodyBuffer::new(Vec::new(), chunk.clone());

        let read = buffer.copy_range(2, 3);

        assert_eq!(read, b"234");
        assert_eq!(buffer.len(), 10);
        assert_eq!(buffer.into_bytes().as_ptr(), chunk.as_ptr());
    }

    #[test]
    fn held_bytes_come_before_the_chunk() {
        let buffer = BodyBuffer::new(b"held ".to_vec(), Bytes::from_static(b"chunk"));

        assert_eq!(buffer.len(), 10);
        assert_eq!(buffer.into_vec(), b"held chunk");
    }

    #[test]
    fn held_bytes_keep_their_allocation() {
        let mut held = Vec::with_capacity(64);
        held.extend_from_slice(b"held");
        let allocation = held.as_ptr();

        let joined = BodyBuffer::new(held, Bytes::from_static(b" chunk")).into_vec();

        assert_eq!(joined, b"held chunk");
        assert_eq!(joined.as_ptr(), allocation);
    }

    #[test]
    fn a_write_gives_the_bytes_that_a_vector_gives() {
        let writes: [(usize, usize, &[u8]); 4] =
            [(0, 0, b"<"), (10, 0, b">"), (3, 0, b"-"), (3, 4, b"xy")];

        for (start, size, value) in writes {
            let mut buffer = BodyBuffer::new(Vec::new(), Bytes::from_static(b"0123456789"));
            let mut expected = b"0123456789".to_vec();
            expected.replace(start, size, value).unwrap();

            let result = buffer.replace(start, size, value);

            assert_eq!(result, Ok(()));
            assert_eq!(buffer.into_bytes(), expected);
        }
    }
}
