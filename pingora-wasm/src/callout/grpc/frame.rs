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

//! gRPC message framing

use bytes::{Buf, BufMut, Bytes, BytesMut};

const HEADER_LEN: usize = 5;
const UNCOMPRESSED: u8 = 0;

pub(crate) fn frame(message: &[u8]) -> Bytes {
    let mut framed = BytesMut::with_capacity(HEADER_LEN + message.len());
    framed.put_u8(UNCOMPRESSED);
    // A plugin message is limited to `i32::MAX` bytes by the ABI
    framed.put_u32(message.len() as u32);
    framed.put_slice(message);
    framed.freeze()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameError {
    Compressed,
    TooLarge,
}

/// Reader of the messages in a gRPC response body, which may span body chunks.
#[derive(Default)]
pub(crate) struct MessageReader {
    buffer: BytesMut,
}

impl MessageReader {
    pub(crate) fn push(&mut self, chunk: &[u8]) {
        self.buffer.extend_from_slice(chunk);
    }

    pub(crate) fn next_message(&mut self, limit: usize) -> Result<Option<Bytes>, FrameError> {
        if self.buffer.len() < HEADER_LEN {
            return Ok(None);
        }
        if self.buffer[0] != UNCOMPRESSED {
            return Err(FrameError::Compressed);
        }
        let length = u32::from_be_bytes([
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
            self.buffer[4],
        ]) as usize;
        if length > limit {
            return Err(FrameError::TooLarge);
        }
        if self.buffer.len() < HEADER_LEN + length {
            return Ok(None);
        }
        self.buffer.advance(HEADER_LEN);
        Ok(Some(self.buffer.split_to(length).freeze()))
    }

    pub(crate) fn has_partial_message(&self) -> bool {
        !self.buffer.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_splits_messages_across_chunks() {
        let mut body = frame(b"first").to_vec();
        body.extend_from_slice(&frame(b""));
        body.extend_from_slice(&frame(b"third"));
        let mut reader = MessageReader::default();
        let mut messages = Vec::new();

        for chunk in body.chunks(3) {
            reader.push(chunk);
            while let Some(message) = reader.next_message(1024).unwrap() {
                messages.push(message);
            }
        }

        assert_eq!(
            messages,
            ["first", "", "third"].map(|m| Bytes::from_static(m.as_bytes()))
        );
        assert!(!reader.has_partial_message());
    }

    #[test]
    fn reader_refuses_compressed_or_oversized_message() {
        let mut compressed = frame(b"x").to_vec();
        compressed[0] = 1;
        let cases = [
            (compressed, 1024, FrameError::Compressed),
            (frame(b"too long").to_vec(), 7, FrameError::TooLarge),
        ];
        for (body, limit, want) in cases {
            let mut reader = MessageReader::default();
            reader.push(&body);

            let got = reader.next_message(limit);

            assert_eq!(got, Err(want));
        }
    }
}
