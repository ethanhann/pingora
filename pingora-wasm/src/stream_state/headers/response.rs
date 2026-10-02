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

//! Response header map

use super::{
    classify, framing_header_values, framing_headers_differ, visit_headers, Name, Regular,
    WriteResult,
};
use http::header::HeaderValue;
use pingora_http::ResponseHeader;
use proxy_wasm_host::{HeaderMap, NotAllowed, PairVisitor};
use std::borrow::Cow;
use std::ops::ControlFlow;

/// The response header map exposed to a guest.
pub(crate) struct ResponseHeaders {
    pub(crate) header: ResponseHeader,
    /// Whether a guest write through this map changed the value of `content-length` or
    /// `transfer-encoding`. A write that left both as they were does not set it.
    pub(crate) length_changed: bool,
}

impl ResponseHeaders {
    pub(crate) fn new(header: ResponseHeader) -> Self {
        ResponseHeaders {
            header,
            length_changed: false,
        }
    }
}

fn set_status(header: &mut ResponseHeader, value: &[u8]) -> WriteResult {
    let code: u16 = std::str::from_utf8(value)
        .ok()
        .and_then(|v| v.parse().ok())
        .ok_or(NotAllowed)?;
    header.set_status(code).map_err(|_| NotAllowed)
}

/// Classify a key for a response map, where `host` is an ordinary header.
fn classify_response(key: &[u8]) -> Option<Name<'_>> {
    match classify(key)? {
        Name::Host => std::str::from_utf8(key).ok().map(Name::Regular),
        name => Some(name),
    }
}

impl HeaderMap for ResponseHeaders {
    fn get(&self, key: &[u8]) -> Option<Cow<'_, [u8]>> {
        let value = match classify_response(key)? {
            Name::Status => Some(self.header.status.as_str().as_bytes()),
            Name::Regular(name) => self.header.headers.get(name).map(HeaderValue::as_bytes),
            _ => None,
        };
        value.map(Cow::Borrowed)
    }

    fn for_each_pair(&self, f: &mut PairVisitor<'_>) -> ControlFlow<()> {
        if f(b":status", self.header.status.as_str().as_bytes()).is_break() {
            return ControlFlow::Break(());
        }
        visit_headers(&self.header.headers, false, f)
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> WriteResult {
        match classify_response(key).ok_or(NotAllowed)? {
            Name::Status => set_status(&mut self.header, value),
            Name::Regular(_) => {
                let before = framing_header_values(&self.header.headers, key);
                self.header.insert(key, value)?;
                self.length_changed |= before != framing_header_values(&self.header.headers, key);
                Ok(())
            }
            _ => Err(NotAllowed),
        }
    }

    fn add(&mut self, key: &[u8], value: &[u8]) -> WriteResult {
        match classify_response(key).ok_or(NotAllowed)? {
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
        match classify_response(key).ok_or(NotAllowed)? {
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
        next.strip(false);
        for (key, value) in pairs {
            match classify_response(key).ok_or(NotAllowed)? {
                Name::Status => set_status(&mut next, value)?,
                Name::Regular(_) => next.append(key, value)?,
                _ => return Err(NotAllowed),
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

    fn response() -> ResponseHeaders {
        let mut header = ResponseHeader::build(200, None).unwrap();
        header.insert_header("X-Up", "1").unwrap();
        ResponseHeaders::new(header)
    }

    #[test]
    fn response_reads_status() {
        let map = response();

        let status = get(&map, ":status");

        assert_eq!(status.as_deref(), Some("200"));
    }

    #[test]
    fn response_writes_status() {
        let mut map = response();

        map.set(b":status", b"404").unwrap();

        assert_eq!(map.header.status, 404);
        assert_eq!(
            pairs(&map),
            [
                (":status".to_string(), "404".to_string()),
                ("x-up".to_string(), "1".to_string()),
            ]
        );
    }

    #[test]
    fn response_rejects_invalid_pseudo_writes() {
        let mut map = response();

        let refused = [
            map.set(b":status", b"abc"),
            map.set(b":status", b"99999"),
            map.set(b":path", b"/"),
            map.add(b":status", b"200"),
            map.remove(b":status"),
        ];

        assert!(refused.iter().all(|r| *r == Err(NotAllowed)));
        assert_eq!(map.header.status, 200);
    }

    #[test]
    fn replace_all_keeps_status() {
        let mut map = response();

        map.replace_all(&[(b"x-new", b"1")]).unwrap();

        assert_eq!(map.header.status, 200);
        assert!(map.header.headers.get("x-up").is_none());
        assert_eq!(map.header.headers["x-new"], "1");
    }

    #[test]
    fn host_is_ordinary_response_header() {
        let mut map = response();

        map.set(b"Host", b"origin.test").unwrap();

        assert_eq!(get(&map, "host").as_deref(), Some("origin.test"));
    }
}
