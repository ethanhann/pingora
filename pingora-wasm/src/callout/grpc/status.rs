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

//! gRPC status codes and the encodings of gRPC headers

use http::HeaderMap;
use proxy_wasm_host::abi::v0_2_1::GrpcStatus;
use std::fmt::Write;
use std::time::Duration;

pub(crate) const OK: u32 = 0;
pub(crate) const CANCELLED: u32 = 1;
pub(crate) const UNKNOWN: u32 = 2;
pub(crate) const PERMISSION_DENIED: u32 = 7;
pub(crate) const DEADLINE_EXCEEDED: u32 = 4;
pub(crate) const RESOURCE_EXHAUSTED: u32 = 8;
pub(crate) const UNIMPLEMENTED: u32 = 12;
pub(crate) const INTERNAL: u32 = 13;
pub(crate) const UNAVAILABLE: u32 = 14;
pub(crate) const UNAUTHENTICATED: u32 = 16;

pub(crate) const GRPC_STATUS: &str = "grpc-status";
pub(crate) const GRPC_MESSAGE: &str = "grpc-message";

/// Return the gRPC status code for an HTTP status, by the mapping that gRPC defines.
pub(crate) fn from_http_status(status: u16) -> u32 {
    match status {
        400 => INTERNAL,
        401 => UNAUTHENTICATED,
        403 => PERMISSION_DENIED,
        404 => UNIMPLEMENTED,
        429 | 502..=504 => UNAVAILABLE,
        _ => UNKNOWN,
    }
}

/// Return the status that ends a gRPC response, if `headers` has one.
pub(crate) fn from_headers(headers: &HeaderMap) -> Option<GrpcStatus> {
    let code = headers.get(GRPC_STATUS)?.to_str().ok()?.parse().ok()?;
    let message = headers
        .get(GRPC_MESSAGE)
        .map(|value| percent_decode(value.as_bytes()))
        .unwrap_or_default();
    Some(GrpcStatus::new(code, message))
}

/// Return the `grpc-timeout` value for `timeout`, which has at most eight digits.
pub(crate) fn timeout_header(timeout: Duration) -> String {
    const MAX_VALUE: u128 = 99_999_999;
    let mut value = timeout.as_millis();
    let mut units = ['m', 'S', 'M', 'H'].into_iter();
    let mut unit = units.next().unwrap_or('m');
    let divisors = [1000, 60, 60];
    for divisor in divisors {
        if value <= MAX_VALUE {
            break;
        }
        value /= divisor;
        unit = units.next().unwrap_or('H');
    }
    format!("{}{unit}", value.min(MAX_VALUE))
}

pub(crate) fn percent_encode(message: &str) -> String {
    let mut encoded = String::with_capacity(message.len());
    for byte in message.bytes() {
        if (0x20..=0x7e).contains(&byte) && byte != b'%' {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn percent_decode(value: &[u8]) -> String {
    let mut decoded = Vec::with_capacity(value.len());
    let mut rest = value;
    while let [byte, tail @ ..] = rest {
        let escaped = match tail {
            [high, low, ..] if *byte == b'%' => hex_digit(*high).zip(hex_digit(*low)),
            _ => None,
        };
        match escaped {
            Some((high, low)) => {
                decoded.push(high << 4 | low);
                rest = &tail[2..];
            }
            None => {
                decoded.push(*byte);
                rest = tail;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn hex_digit(digit: u8) -> Option<u8> {
    char::from(digit)
        .to_digit(16)
        .and_then(|value| u8::try_from(value).ok())
}

/// Encode the value of a `-bin` metadata key, which gRPC sends in base64.
pub(crate) fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for group in bytes.chunks(3) {
        let joined = group.iter().enumerate().fold(0u32, |joined, (i, byte)| {
            joined | u32::from(*byte) << (16 - 8 * i)
        });
        for i in 0..4 {
            if i <= group.len() {
                let index = (joined >> (18 - 6 * i)) & 0x3f;
                encoded.push(char::from(ALPHABET[index as usize]));
            } else {
                encoded.push('=');
            }
        }
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_status_maps_to_grpc_code() {
        let cases = [
            (400, INTERNAL),
            (401, UNAUTHENTICATED),
            (403, PERMISSION_DENIED),
            (404, UNIMPLEMENTED),
            (429, UNAVAILABLE),
            (503, UNAVAILABLE),
            (504, UNAVAILABLE),
            (500, UNKNOWN),
        ];

        let got = cases.map(|(http, _)| from_http_status(http));

        assert_eq!(got, cases.map(|(_, grpc)| grpc));
    }

    #[test]
    fn timeout_header_fits_eight_digits() {
        let cases = [
            (Duration::from_millis(1500), "1500m"),
            (Duration::from_millis(99_999_999), "99999999m"),
            (Duration::from_millis(100_000_000), "100000S"),
            (Duration::from_secs(100_000_000), "1666666M"),
            (Duration::from_secs(u64::MAX), "99999999H"),
        ];

        let got = cases.map(|(timeout, _)| timeout_header(timeout));

        assert_eq!(got, cases.map(|(_, header)| header));
    }

    #[test]
    fn message_survives_percent_encoding() {
        let message = "denied: 100% sure ü";

        let encoded = percent_encode(message);

        assert_eq!(encoded, "denied: 100%25 sure %C3%BC");
        assert_eq!(percent_decode(encoded.as_bytes()), message);
        assert_eq!(percent_decode(b"bad %zz end %4"), "bad %zz end %4");
    }

    #[test]
    fn status_comes_from_grpc_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(GRPC_STATUS, "10".parse().unwrap());
        headers.insert(GRPC_MESSAGE, "Aborted%20here".parse().unwrap());
        let mut bad = HeaderMap::new();
        bad.insert(GRPC_STATUS, "ten".parse().unwrap());

        let got = [
            from_headers(&headers),
            from_headers(&bad),
            from_headers(&HeaderMap::new()),
        ];

        assert_eq!(got, [Some(GrpcStatus::new(10, "Aborted here")), None, None]);
    }

    #[test]
    fn base64_pads_each_length() {
        let cases = [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
        ];

        let got = cases.map(|(raw, _)| base64(raw.as_bytes()));

        assert_eq!(got, cases.map(|(_, encoded)| encoded));
    }
}
