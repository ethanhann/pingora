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

//! The properties that the crate reads from the session and the headers of a request.

use crate::stream_state::RequestHeaders;
use http::header::CONTENT_LENGTH;
use http::Version;
use pingora_core::protocols::tls::SslDigest;
use pingora_http::ResponseHeader;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime};

/// Facts about a request that are not in its headers, recorded by the phases.
#[derive(Debug, Default)]
pub(crate) struct RequestFacts {
    pub(crate) client_address: Option<SocketAddr>,
    pub(crate) server_address: Option<SocketAddr>,
    pub(crate) tls: Option<TlsFacts>,
    pub(crate) start: Option<RequestStart>,
    pub(crate) request_body_bytes: usize,
    pub(crate) upstream_address: Option<SocketAddr>,
    /// The status of the response, which `response_filter` records.
    pub(crate) response_code: Option<u16>,
    /// Facts that are known only in `logging`.
    pub(crate) logging: Option<LoggingFacts>,
}

/// The time that `request_filter` started.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestStart {
    /// The wall time, for `request.time`.
    pub(crate) wall_time: SystemTime,
    /// The monotonic time, for `request.duration`.
    pub(crate) monotonic_time: Instant,
}

#[derive(Debug)]
pub(crate) struct TlsFacts {
    has_peer_certificate: bool,
    version: String,
}

impl TlsFacts {
    pub(crate) fn new(digest: &SslDigest) -> Self {
        TlsFacts {
            has_peer_certificate: !digest.cert_digest.is_empty(),
            version: envoy_tls_version(&digest.version),
        }
    }
}

#[derive(Debug)]
pub(crate) struct LoggingFacts {
    /// The time from the start of `request_filter`, or `None` when it did not run.
    pub(crate) duration: Option<Duration>,
    pub(crate) response_body_bytes: usize,
}

/// The headers that a plugin can read in the running callback.
pub(crate) struct ReadableHeaders<'a> {
    pub(crate) request: Option<&'a RequestHeaders>,
    pub(crate) response: Option<&'a ResponseHeader>,
}

/// Write the built-in property whose path segments are joined in `joined_path` to `out`.
///
/// Return `false` when the property is not a built-in one or has no value yet.
pub(crate) fn write_built_in_property(
    joined_path: &[u8],
    facts: &RequestFacts,
    headers: &ReadableHeaders<'_>,
    out: &mut Vec<u8>,
) -> bool {
    let request = headers.request;
    match joined_path {
        b"source\0address" => write_address(facts.client_address, out),
        b"source\0port" => write_port(facts.client_address, out),
        b"destination\0address" => write_address(facts.server_address, out),
        b"destination\0port" => write_port(facts.server_address, out),
        b"upstream\0address" => write_address(facts.upstream_address, out),
        b"upstream\0port" => write_port(facts.upstream_address, out),
        b"request\0path" => write_bytes(request.map(|r| r.header.raw_path()), out),
        b"request\0url_path" => {
            let path = request.map(|r| r.header.raw_path());
            write_bytes(
                path.map(|p| p.split(|b| *b == b'?').next().unwrap_or(p)),
                out,
            )
        }
        b"request\0host" => write_bytes(request.and_then(RequestHeaders::authority), out),
        b"request\0scheme" => write_bytes(request.map(|r| r.scheme().as_str().as_bytes()), out),
        b"request\0method" => {
            write_bytes(request.map(|r| r.header.method.as_str().as_bytes()), out)
        }
        b"request\0protocol" => {
            write_bytes(request.and_then(|r| protocol_name(r.header.version)), out)
        }
        b"request\0time" => {
            let since_epoch = facts
                .start
                .and_then(|start| start.wall_time.duration_since(SystemTime::UNIX_EPOCH).ok());
            write_int(since_epoch.map(duration_nanos), out)
        }
        b"request\0size" => {
            let length = request.and_then(|r| content_length(&r.header.headers));
            let size = length.or_else(|| i64::try_from(facts.request_body_bytes).ok());
            write_int(size, out)
        }
        b"request\0duration" => write_int(
            facts
                .logging
                .as_ref()
                .and_then(|l| l.duration)
                .map(duration_nanos),
            out,
        ),
        b"response\0code" => {
            let code = headers.response.map(|r| r.status.as_u16());
            write_int(code.or(facts.response_code).map(i64::from), out)
        }
        b"response\0size" => {
            let size = facts.logging.as_ref().map(|l| l.response_body_bytes);
            write_int(size.and_then(|s| i64::try_from(s).ok()), out)
        }
        b"connection\0mtls" => match &facts.tls {
            Some(tls) => {
                out.push(u8::from(tls.has_peer_certificate));
                true
            }
            None => false,
        },
        b"connection\0tls_version" => {
            write_bytes(facts.tls.as_ref().map(|t| t.version.as_bytes()), out)
        }
        _ => false,
    }
}

/// Return the TLS version in the form of Envoy and OpenSSL, such as `TLSv1.3`.
///
/// rustls writes `TLSv1_3`, and s2n writes `TLS13`.
fn envoy_tls_version(version: &str) -> String {
    match version {
        "TLSv1_3" | "TLS13" => "TLSv1.3".to_string(),
        "TLSv1_2" | "TLS12" => "TLSv1.2".to_string(),
        "TLSv1_1" | "TLS11" => "TLSv1.1".to_string(),
        "TLSv1_0" | "TLS10" => "TLSv1".to_string(),
        other => other.to_string(),
    }
}

fn protocol_name(version: Version) -> Option<&'static [u8]> {
    match version {
        Version::HTTP_10 => Some(b"HTTP/1.0"),
        Version::HTTP_11 => Some(b"HTTP/1.1"),
        Version::HTTP_2 => Some(b"HTTP/2"),
        _ => None,
    }
}

fn content_length(headers: &http::HeaderMap) -> Option<i64> {
    headers.get(CONTENT_LENGTH)?.to_str().ok()?.parse().ok()
}

fn duration_nanos(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

fn write_bytes(value: Option<&[u8]>, out: &mut Vec<u8>) -> bool {
    match value {
        Some(value) => {
            out.extend_from_slice(value);
            true
        }
        None => false,
    }
}

fn write_int(value: Option<i64>, out: &mut Vec<u8>) -> bool {
    match value {
        Some(value) => {
            out.extend_from_slice(&value.to_le_bytes());
            true
        }
        None => false,
    }
}

fn write_address(address: Option<SocketAddr>, out: &mut Vec<u8>) -> bool {
    write_bytes(
        address.map(|a| a.to_string()).as_deref().map(str::as_bytes),
        out,
    )
}

fn write_port(address: Option<SocketAddr>, out: &mut Vec<u8>) -> bool {
    write_int(address.map(|a| i64::from(a.port())), out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::uri::Scheme;
    use pingora_http::RequestHeader;
    use std::borrow::Cow;

    fn tls(version: &'static str, cert_digest: Vec<u8>) -> TlsFacts {
        let digest = SslDigest::new("TLS_AES_128_GCM_SHA256", version, None, None, cert_digest);
        TlsFacts::new(&digest)
    }

    fn read_property(
        key: &[u8],
        facts: &RequestFacts,
        request: Option<&RequestHeaders>,
    ) -> Option<Vec<u8>> {
        let headers = ReadableHeaders {
            request,
            response: None,
        };
        let mut out = Vec::new();
        write_built_in_property(key, facts, &headers, &mut out).then_some(out)
    }

    #[test]
    fn the_tls_version_has_the_form_of_envoy_for_each_backend() {
        for version in ["TLSv1_3", "TLSv1.3", "TLS13"] {
            let facts = RequestFacts {
                tls: Some(tls(version, Vec::new())),
                ..RequestFacts::default()
            };

            let read_version = read_property(b"connection\0tls_version", &facts, None);
            let mtls = read_property(b"connection\0mtls", &facts, None);

            assert_eq!(read_version.as_deref(), Some(&b"TLSv1.3"[..]), "{version}");
            assert_eq!(mtls, Some(vec![0]));
        }
    }

    #[test]
    fn the_request_properties_come_from_the_header_and_the_facts() {
        let mut header = RequestHeader::build("POST", b"/a/b?c=d", None).unwrap();
        header.insert_header("host", "shop.test").unwrap();
        header.insert_header("content-length", "12").unwrap();
        let request = RequestHeaders::new(header, Scheme::HTTPS);
        let facts = RequestFacts {
            client_address: Some("10.0.0.1:4000".parse().unwrap()),
            ..RequestFacts::default()
        };
        let cases: [(&[u8], Cow<'_, [u8]>); 8] = [
            (b"request\0path", b"/a/b?c=d"[..].into()),
            (b"request\0url_path", b"/a/b"[..].into()),
            (b"request\0host", b"shop.test"[..].into()),
            (b"request\0scheme", b"https"[..].into()),
            (b"request\0method", b"POST"[..].into()),
            (b"request\0protocol", b"HTTP/1.1"[..].into()),
            (b"request\0size", 12_i64.to_le_bytes().to_vec().into()),
            (b"source\0port", 4000_i64.to_le_bytes().to_vec().into()),
        ];

        for (key, expected) in cases {
            let value = read_property(key, &facts, Some(&request));
            assert_eq!(
                value.as_deref(),
                Some(&*expected),
                "{}",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[test]
    fn a_property_with_no_value_yet_is_not_found() {
        let facts = RequestFacts::default();

        for key in [
            &b"source\0address"[..],
            b"request\0duration",
            b"response\0code",
            b"no\0such",
        ] {
            assert_eq!(
                read_property(key, &facts, None),
                None,
                "{}",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[test]
    fn the_response_code_reads_in_a_phase_with_no_response_header() {
        let facts = RequestFacts {
            response_code: Some(201),
            ..RequestFacts::default()
        };

        let code = read_property(b"response\0code", &facts, None);

        assert_eq!(code, Some(201_i64.to_le_bytes().to_vec()));
    }

    #[test]
    fn the_duration_is_not_found_for_a_request_with_no_start() {
        let facts = RequestFacts {
            logging: Some(LoggingFacts {
                duration: None,
                response_body_bytes: 0,
            }),
            ..RequestFacts::default()
        };

        let duration = read_property(b"request\0duration", &facts, None);

        assert_eq!(duration, None);
    }
}
