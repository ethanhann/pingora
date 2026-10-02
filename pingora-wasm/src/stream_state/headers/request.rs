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

//! Request header map
//!
//! `:method`, `:path`, and `:authority` are derived from the typed fields of Pingora's
//! `RequestHeader`. `:scheme` comes from the request URI, or from whether the downstream
//! connection uses TLS when the URI has no scheme. `host` is left out of the listed pairs and
//! reported as `:authority` instead, as Proxy-Wasm plugins expect. Reading or setting `host` by
//! name acts on `:authority`.

use super::{
    classify, framing_header_values, framing_headers_differ, value_of, visit_headers, Name,
    Regular, WriteResult,
};
use http::header::{HeaderValue, HOST};
use http::uri::{Authority, PathAndQuery, Scheme};
use http::{Method, Uri, Version};
use pingora_http::RequestHeader;
use proxy_wasm_host::{HeaderMap, NotAllowed, PairVisitor};
use std::borrow::Cow;
use std::ops::ControlFlow;

pub(crate) struct RequestHeaders {
    pub(crate) header: RequestHeader,
    /// Whether a guest write through this map changed the value of `content-length` or
    /// `transfer-encoding`. A write that left both as they were does not set it.
    pub(crate) length_changed: bool,
    scheme: Scheme,
}

impl RequestHeaders {
    pub(crate) fn new(header: RequestHeader, scheme: Scheme) -> Self {
        RequestHeaders {
            header,
            length_changed: false,
            scheme,
        }
    }

    pub(crate) fn scheme(&self) -> &Scheme {
        &self.scheme
    }

    pub(crate) fn authority(&self) -> Option<&[u8]> {
        let from_uri = self.header.uri.authority().map(|a| a.as_str().as_bytes());
        let from_host = self.header.headers.get(HOST).map(HeaderValue::as_bytes);
        if self.header.version == Version::HTTP_2 {
            from_uri.or(from_host)
        } else {
            from_host.or(from_uri)
        }
    }

    fn path(&self) -> Option<&[u8]> {
        if self.header.method == Method::CONNECT {
            return None;
        }
        let raw = self.header.raw_path();
        if raw.first() == Some(&b'/') || raw == b"*" {
            return Some(raw);
        }
        // For an absolute-form target this is the path and query of its URI
        Some(
            self.header
                .uri
                .path_and_query()
                .map_or(&b"/"[..], |path| path.as_str().as_bytes()),
        )
    }
}

fn set_request_pseudo(
    header: &mut RequestHeader,
    scheme: &str,
    name: Name<'_>,
    value: &[u8],
) -> WriteResult {
    match name {
        Name::Method => {
            header.set_method(Method::from_bytes(value).map_err(|_| NotAllowed)?);
            Ok(())
        }
        Name::Path if header.method != Method::CONNECT => {
            if !is_origin_path(value) {
                return Err(NotAllowed);
            }
            if header.uri.authority().is_some() {
                let mut parts = header.uri.clone().into_parts();
                parts.path_and_query = Some(PathAndQuery::try_from(value).map_err(|_| NotAllowed)?);
                header.set_uri(Uri::from_parts(parts).map_err(|_| NotAllowed)?);
                Ok(())
            } else {
                header.set_raw_path(value).map_err(|_| NotAllowed)
            }
        }
        Name::Authority | Name::Host => {
            let host = value_of(value)?;
            if header.uri.authority().is_some() {
                let mut parts = header.uri.clone().into_parts();
                parts.authority = Some(Authority::try_from(value).map_err(|_| NotAllowed)?);
                header.set_uri(Uri::from_parts(parts).map_err(|_| NotAllowed)?);
            }
            if header.version != Version::HTTP_2 || header.headers.contains_key(HOST) {
                header.insert_header(HOST, host).map_err(|_| NotAllowed)?;
            }
            Ok(())
        }
        Name::Scheme if value == scheme.as_bytes() => Ok(()),
        _ => Err(NotAllowed),
    }
}

fn is_origin_path(value: &[u8]) -> bool {
    // Like an HTTP/2 `:path`, the value has to be in origin form or `*`
    (value.first() == Some(&b'/') || value == b"*")
        && !value.iter().any(|b| *b == b' ' || b.is_ascii_control())
}

impl HeaderMap for RequestHeaders {
    fn get(&self, key: &[u8]) -> Option<Cow<'_, [u8]>> {
        let value = match classify(key)? {
            Name::Method => Some(self.header.method.as_str().as_bytes()),
            Name::Path => self.path(),
            Name::Authority | Name::Host => self.authority(),
            Name::Scheme => Some(self.scheme.as_str().as_bytes()),
            Name::Status | Name::OtherPseudo => None,
            Name::Regular(name) => self.header.headers.get(name).map(HeaderValue::as_bytes),
        };
        value.map(Cow::Borrowed)
    }

    fn for_each_pair(&self, f: &mut PairVisitor<'_>) -> ControlFlow<()> {
        let pseudo = [
            Some((&b":method"[..], self.header.method.as_str().as_bytes())),
            Some((&b":scheme"[..], self.scheme.as_str().as_bytes())),
            self.authority().map(|value| (&b":authority"[..], value)),
            self.path().map(|value| (&b":path"[..], value)),
        ];
        for (name, value) in pseudo.into_iter().flatten() {
            if f(name, value).is_break() {
                return ControlFlow::Break(());
            }
        }
        visit_headers(&self.header.headers, true, f)
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> WriteResult {
        match classify(key).ok_or(NotAllowed)? {
            Name::Regular(_) => {
                let before = framing_header_values(&self.header.headers, key);
                self.header.insert(key, value)?;
                self.length_changed |= before != framing_header_values(&self.header.headers, key);
                Ok(())
            }
            name => set_request_pseudo(&mut self.header, self.scheme.as_str(), name, value),
        }
    }

    fn add(&mut self, key: &[u8], value: &[u8]) -> WriteResult {
        match classify(key).ok_or(NotAllowed)? {
            Name::Regular(_) => {
                let before = framing_header_values(&self.header.headers, key);
                self.header.append(key, value)?;
                self.length_changed |= before != framing_header_values(&self.header.headers, key);
                Ok(())
            }
            _ => Err(NotAllowed),
        }
    }

    fn remove(&mut self, key: &[u8]) -> WriteResult {
        match classify(key).ok_or(NotAllowed)? {
            Name::Regular(name) => {
                let before = framing_header_values(&self.header.headers, key);
                self.header.remove(name);
                self.length_changed |= before != framing_header_values(&self.header.headers, key);
                Ok(())
            }
            _ => Err(NotAllowed),
        }
    }

    fn replace_all(&mut self, pairs: &[(&[u8], &[u8])]) -> WriteResult {
        let mut next = self.header.clone();
        next.strip(true);
        for (key, value) in pairs {
            match classify(key).ok_or(NotAllowed)? {
                Name::Regular(_) => next.append(key, value)?,
                name => set_request_pseudo(&mut next, self.scheme.as_str(), name, value)?,
            }
        }
        self.length_changed |= framing_headers_differ(&self.header.headers, &next.headers);
        self.header = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{get, pairs};
    use bytes::BytesMut;

    fn request(method: &str, path: &[u8], host: Option<&str>) -> RequestHeaders {
        let mut header = RequestHeader::build(method, path, None).unwrap();
        if let Some(host) = host {
            header.insert_header("Host", host).unwrap();
        }
        header.insert_header("X-Trace", "abc").unwrap();
        RequestHeaders::new(header, Scheme::HTTP)
    }

    #[test]
    fn length_change_is_recorded_only_when_framing_header_differs() {
        type Write = fn(&mut RequestHeaders) -> WriteResult;
        let writes: [(&str, Write, bool); 5] = [
            ("same value", |map| map.set(b"content-length", b"5"), false),
            (
                "absent header removed",
                |map| map.remove(b"transfer-encoding"),
                false,
            ),
            ("other header", |map| map.set(b"x-trace", b"new"), false),
            ("new value", |map| map.set(b"Content-Length", b"6"), true),
            ("header removed", |map| map.remove(b"content-length"), true),
        ];

        for (case, write, changed) in writes {
            let mut map = request("POST", b"/", Some("example.test"));
            map.header.insert_header("content-length", "5").unwrap();

            let result = write(&mut map);

            assert_eq!(result, Ok(()), "{case}");
            assert_eq!(map.length_changed, changed, "{case}");
        }
    }

    #[test]
    fn request_reads_pseudo_headers() {
        let map = request("POST", b"/a?b=1", Some("example.test"));

        let read: Vec<_> = [
            ":method",
            ":path",
            ":authority",
            ":scheme",
            ":other",
            "host",
        ]
        .iter()
        .map(|k| get(&map, k))
        .collect();

        assert_eq!(
            read,
            [
                Some("POST".to_string()),
                Some("/a?b=1".to_string()),
                Some("example.test".to_string()),
                Some("http".to_string()),
                None,
                Some("example.test".to_string()),
            ]
        );
    }

    #[test]
    fn request_lists_pseudo_headers_first_and_hides_host() {
        let map = request("GET", b"/", Some("example.test"));

        let listed = pairs(&map);

        assert_eq!(
            listed,
            [
                (":method".to_string(), "GET".to_string()),
                (":scheme".to_string(), "http".to_string()),
                (":authority".to_string(), "example.test".to_string()),
                (":path".to_string(), "/".to_string()),
                ("x-trace".to_string(), "abc".to_string()),
            ]
        );
        assert_eq!(map.len(), 5);
    }

    #[test]
    fn pseudo_headers_follow_request_target() {
        let mut h2 = RequestHeader::build("GET", b"/", None).unwrap();
        h2.set_uri("https://h2.test/x".parse().unwrap());
        h2.set_version(Version::HTTP_2);
        let absolute = RequestHeader::build("GET", b"http://example.test/a?b=1", None).unwrap();
        let connect = RequestHeader::build("CONNECT", b"example.test:443", None).unwrap();
        let cases = [
            (h2, ":authority", Some("h2.test")),
            (absolute, ":path", Some("/a?b=1")),
            (connect, ":path", None),
        ];

        for (header, key, want) in cases {
            let map = RequestHeaders::new(header, Scheme::HTTP);

            let got = get(&map, key);

            assert_eq!(got.as_deref(), want, "{key}");
        }
    }

    #[test]
    fn request_writes_pseudo_headers() {
        let mut map = request("GET", b"/", Some("example.test"));
        let writes: [(&[u8], &[u8]); 4] = [
            (b":method", b"PUT"),
            (b":path", b"/new?q=2"),
            (b":authority", b"other.test"),
            (b":scheme", b"http"),
        ];

        for (key, value) in writes {
            map.set(key, value).unwrap();
        }

        assert_eq!(map.header.method, Method::PUT);
        assert_eq!(map.header.raw_path(), b"/new?q=2");
        assert_eq!(map.header.headers[HOST], "other.test");
        assert_eq!(get(&map, ":authority").as_deref(), Some("other.test"));
    }

    #[test]
    fn path_write_keeps_http2_authority() {
        let mut header = RequestHeader::build("GET", b"/", None).unwrap();
        header.set_uri("https://h2.test/x".parse().unwrap());
        header.set_version(Version::HTTP_2);
        let mut map = RequestHeaders::new(header, Scheme::HTTPS);

        map.set(b":path", b"/y").unwrap();

        assert_eq!(map.header.uri.to_string(), "https://h2.test/y");
    }

    #[test]
    fn request_rejects_invalid_writes() {
        let mut map = request("GET", b"/", Some("example.test"));

        let refused = [
            map.set(b":method", b"BAD METHOD"),
            map.set(b":path", b"no slash"),
            map.set(b":authority", b"bad\nhost"),
            map.set(b":scheme", b"https"),
            map.set(b":other", b"x"),
            map.add(b":path", b"/x"),
            map.add(b"host", b"x"),
            map.remove(b":path"),
            map.remove(b"host"),
            map.set(b"bad name", b"x"),
            map.set(b"x-ok", b"bad\nvalue"),
        ];

        assert!(refused.iter().all(|r| *r == Err(NotAllowed)));
        assert_eq!(map.header.method, Method::GET);
        assert_eq!(map.header.raw_path(), b"/");
        assert_eq!(map.header.headers[HOST], "example.test");
    }

    #[test]
    fn host_write_changes_authority() {
        let mut map = request("GET", b"/", Some("example.test"));

        map.set(b"Host", b"moved.test").unwrap();

        assert_eq!(get(&map, ":authority").as_deref(), Some("moved.test"));
    }

    #[test]
    fn header_reads_ignore_ascii_case() {
        let map = request("GET", b"/", None);

        let read = [
            get(&map, "x-trace"),
            get(&map, "X-TRACE"),
            get(&map, ":METHOD"),
        ];

        assert_eq!(read.map(|v| v.unwrap()), ["abc", "abc", "GET"]);
    }

    #[test]
    fn header_writes_ignore_ascii_case() {
        let mut map = request("GET", b"/", None);
        map.set(b"X-NEW", b"1").unwrap();
        map.add(b"x-new", b"2").unwrap();
        let before = map.header.headers.get_all("x-new").iter().count();

        map.remove(b"X-New").unwrap();

        assert_eq!(before, 2);
        assert!(map.header.headers.get("x-new").is_none());
    }

    #[test]
    fn writes_keep_header_case_map_in_sync() {
        let mut map = request("GET", b"/", Some("example.test"));

        map.set(b"Wasm-Context", b"7").unwrap();
        map.remove(b"x-trace").unwrap();

        let mut wire = BytesMut::new();
        map.header.header_to_h1_wire(&mut wire);
        let wire = String::from_utf8(wire.to_vec()).unwrap();
        assert!(wire.contains("Wasm-Context: 7\r\n"));
        assert!(!wire.to_ascii_lowercase().contains("x-trace"));
    }

    #[test]
    fn replace_all_keeps_omitted_pseudo_headers_and_host() {
        let mut map = request("GET", b"/keep", Some("example.test"));

        map.replace_all(&[(b"x-a", b"1"), (b"x-a", b"2"), (b":method", b"POST")])
            .unwrap();

        assert_eq!(map.header.method, Method::POST);
        assert_eq!(map.header.raw_path(), b"/keep");
        assert_eq!(map.header.headers[HOST], "example.test");
        assert!(map.header.headers.get("x-trace").is_none());
        let values: Vec<_> = map.header.headers.get_all("x-a").iter().collect();
        assert_eq!(values, ["1", "2"]);
    }

    #[test]
    fn replace_all_with_rejected_pair_changes_nothing() {
        let mut map = request("GET", b"/", Some("example.test"));

        let refused = map.replace_all(&[(b"x-a", b"1"), (b":scheme", b"https")]);

        assert_eq!(refused, Err(NotAllowed));
        assert_eq!(map.header.headers["x-trace"], "abc");
        assert!(map.header.headers.get("x-a").is_none());
    }

    #[test]
    fn replace_all_round_trips_listed_pairs() {
        let mut map = request("POST", b"/a?b=1", Some("example.test"));
        let before = pairs(&map);
        let owned: Vec<(Vec<u8>, Vec<u8>)> = before
            .iter()
            .map(|(k, v)| (k.clone().into_bytes(), v.clone().into_bytes()))
            .collect();
        let borrowed: Vec<(&[u8], &[u8])> = owned
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();

        map.replace_all(&borrowed).unwrap();

        assert_eq!(pairs(&map), before);
    }
}
