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

//! Upstream names in serialized `GrpcService` messages

const ENVOY_GRPC: u64 = 1;
const GOOGLE_GRPC: u64 = 2;
/// `cluster_name` and `target_uri` are both field 1.
const NAME: u64 = 1;

const VARINT: u64 = 0;
const FIXED_64: u64 = 1;
const LENGTH_DELIMITED: u64 = 2;
const FIXED_32: u64 = 5;

pub(crate) fn upstream_name(upstream: &[u8]) -> &[u8] {
    service_target(upstream).unwrap_or(upstream)
}

fn service_target(message: &[u8]) -> Option<&[u8]> {
    let mut target = None;
    for field in Fields(message) {
        let (number, value) = field?;
        if let (ENVOY_GRPC | GOOGLE_GRPC, Some(value)) = (number, value) {
            target = Some(name_field(value)?);
        }
    }
    target
}

fn name_field(message: &[u8]) -> Option<&[u8]> {
    let mut name = None;
    for field in Fields(message) {
        if let (NAME, Some(value)) = field? {
            name = Some(value);
        }
    }
    name
}

/// The fields of a protobuf message, with the bytes of each length-delimited one. An item is
/// `None` where the message is malformed.
struct Fields<'a>(&'a [u8]);

impl<'a> Iterator for Fields<'a> {
    type Item = Option<(u64, Option<&'a [u8]>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        let field = self.read_field();
        if field.is_none() {
            self.0 = &[];
        }
        Some(field)
    }
}

impl<'a> Fields<'a> {
    fn read_field(&mut self) -> Option<(u64, Option<&'a [u8]>)> {
        let key = self.read_varint()?;
        let number = key >> 3;
        if number == 0 {
            return None;
        }
        let value = match key & 7 {
            VARINT => self.read_varint().map(|_| None)?,
            FIXED_64 => self.skip(8).map(|_| None)?,
            LENGTH_DELIMITED => {
                let length = usize::try_from(self.read_varint()?).ok()?;
                Some(self.skip(length)?)
            }
            FIXED_32 => self.skip(4).map(|_| None)?,
            _ => return None,
        };
        Some((number, value))
    }

    fn read_varint(&mut self) -> Option<u64> {
        let mut value = 0;
        for (i, byte) in self.0.iter().enumerate().take(10) {
            value |= u64::from(byte & 0x7f) << (7 * i);
            if byte & 0x80 == 0 {
                self.0 = &self.0[i + 1..];
                return Some(value);
            }
        }
        None
    }

    fn skip(&mut self, length: usize) -> Option<&'a [u8]> {
        let skipped = self.0.get(..length)?;
        self.0 = &self.0[length..];
        Some(skipped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn submessage(field: u8, inner: &[u8]) -> Vec<u8> {
        let mut message = vec![field << 3 | 2, inner.len() as u8];
        message.extend_from_slice(inner);
        message
    }

    #[test]
    fn upstream_name_reads_service_target_or_keeps_name() {
        let google = submessage(2, &submessage(1, b"logging.test:443"));
        let mut envoy = submessage(1, &submessage(1, b"authz"));
        // The timeout, field 3, is not read and is skipped
        envoy.extend_from_slice(&submessage(3, &[0x08, 0x05]));
        let cases: [(&[u8], &[u8]); 5] = [
            (&google, b"logging.test:443"),
            (&envoy, b"authz"),
            (b"grpcbin", b"grpcbin"),
            (b"", b""),
            (&submessage(2, b"\x0a\x09short"), b"\x12\x07\x0a\x09short"),
        ];

        let got = cases.map(|(upstream, _)| upstream_name(upstream));

        assert_eq!(got, cases.map(|(_, name)| name));
    }
}
