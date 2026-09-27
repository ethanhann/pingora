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

//! Header names and values, shared by the request map and the response map.

use http::header::{HeaderName, HeaderValue, HOST};
use pingora_http::{RequestHeader, ResponseHeader};
use proxy_wasm_host::{NotAllowed, PairVisitor};
use std::ops::ControlFlow;

pub(super) type WriteResult = Result<(), NotAllowed>;

/// What a key refers to, found without an allocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Name<'a> {
    Method,
    Path,
    Authority,
    Scheme,
    Status,
    OtherPseudo,
    Host,
    Regular(&'a str),
}

const PSEUDO: [(&[u8], Name<'static>); 5] = [
    (b":method", Name::Method),
    (b":path", Name::Path),
    (b":authority", Name::Authority),
    (b":scheme", Name::Scheme),
    (b":status", Name::Status),
];

pub(super) fn classify(key: &[u8]) -> Option<Name<'_>> {
    if key.first() == Some(&b':') {
        let found = PSEUDO
            .iter()
            .find(|(name, _)| key.eq_ignore_ascii_case(name));
        return Some(found.map_or(Name::OtherPseudo, |(_, name)| *name));
    }
    if key.eq_ignore_ascii_case(b"host") {
        return Some(Name::Host);
    }
    std::str::from_utf8(key).ok().map(Name::Regular)
}

fn name_of(key: &[u8]) -> Result<String, NotAllowed> {
    let name = std::str::from_utf8(key).map_err(|_| NotAllowed)?;
    HeaderName::from_bytes(name.as_bytes()).map_err(|_| NotAllowed)?;
    Ok(name.to_string())
}

pub(super) fn value_of(value: &[u8]) -> Result<HeaderValue, NotAllowed> {
    HeaderValue::from_bytes(value).map_err(|_| NotAllowed)
}

pub(super) fn visit_headers(
    map: &http::HeaderMap,
    skip_host: bool,
    f: &mut PairVisitor<'_>,
) -> ControlFlow<()> {
    for (name, value) in map {
        if skip_host && name == HOST {
            continue;
        }
        if f(name.as_str().as_bytes(), value.as_bytes()).is_break() {
            return ControlFlow::Break(());
        }
    }
    ControlFlow::Continue(())
}

pub(super) trait Regular {
    fn map(&self) -> &http::HeaderMap;
    fn insert(&mut self, key: &[u8], value: &[u8]) -> WriteResult;
    fn append(&mut self, key: &[u8], value: &[u8]) -> WriteResult;
    fn remove(&mut self, key: &str);
    fn strip(&mut self, keep_host: bool) {
        let names: Vec<HeaderName> = self
            .map()
            .keys()
            .filter(|name| !(keep_host && *name == HOST))
            .cloned()
            .collect();
        for name in names {
            self.remove(name.as_str());
        }
    }
}

macro_rules! impl_regular {
    ($type:ty) => {
        impl Regular for $type {
            fn map(&self) -> &http::HeaderMap {
                &self.headers
            }

            fn insert(&mut self, key: &[u8], value: &[u8]) -> WriteResult {
                self.insert_header(name_of(key)?, value_of(value)?)
                    .map_err(|_| NotAllowed)
            }

            fn append(&mut self, key: &[u8], value: &[u8]) -> WriteResult {
                self.append_header(name_of(key)?, value_of(value)?)
                    .map(|_| ())
                    .map_err(|_| NotAllowed)
            }

            fn remove(&mut self, key: &str) {
                self.remove_header(key);
            }
        }
    };
}

impl_regular!(RequestHeader);
impl_regular!(ResponseHeader);
