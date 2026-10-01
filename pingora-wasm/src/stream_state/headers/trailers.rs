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

//! The response trailer map that a guest sees.

use super::{header_name, value_of, visit_headers, WriteResult};
use http::header::HeaderValue;
use proxy_wasm_host::{HeaderMap, PairVisitor};
use std::borrow::Cow;
use std::ops::ControlFlow;

/// The response trailer map of a guest.
///
/// Trailers have no pseudo headers.
#[derive(Default)]
pub(crate) struct ResponseTrailers {
    pub(crate) trailers: http::HeaderMap,
}

impl ResponseTrailers {
    pub(crate) fn new(trailers: http::HeaderMap) -> Self {
        ResponseTrailers { trailers }
    }
}

impl HeaderMap for ResponseTrailers {
    fn get(&self, key: &[u8]) -> Option<Cow<'_, [u8]>> {
        let name = header_name(key).ok()?;
        self.trailers
            .get(name)
            .map(HeaderValue::as_bytes)
            .map(Cow::Borrowed)
    }

    fn for_each_pair(&self, f: &mut PairVisitor<'_>) -> ControlFlow<()> {
        visit_headers(&self.trailers, false, f)
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> WriteResult {
        self.trailers.insert(header_name(key)?, value_of(value)?);
        Ok(())
    }

    fn add(&mut self, key: &[u8], value: &[u8]) -> WriteResult {
        self.trailers.append(header_name(key)?, value_of(value)?);
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> WriteResult {
        self.trailers.remove(header_name(key)?);
        Ok(())
    }

    fn replace_all(&mut self, pairs: &[(&[u8], &[u8])]) -> WriteResult {
        let mut next = http::HeaderMap::with_capacity(pairs.len());
        for (key, value) in pairs {
            next.append(header_name(key)?, value_of(value)?);
        }
        self.trailers = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{get, pairs};
    use proxy_wasm_host::NotAllowed;

    fn trailers() -> ResponseTrailers {
        let mut map = ResponseTrailers::new(http::HeaderMap::new());
        map.add(b"grpc-status", b"0").unwrap();
        map.add(b"x-count", b"1").unwrap();
        map
    }

    fn text(list: &[(&str, &str)]) -> Vec<(String, String)> {
        list.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_guest_reads_and_changes_trailers() {
        let mut map = trailers();

        let results = [
            map.set(b"X-Count", b"2"),
            map.add(b"x-count", b"3"),
            map.remove(b"grpc-status"),
        ];

        assert_eq!(results, [Ok(()); 3]);
        assert_eq!(get(&map, "x-COUNT").as_deref(), Some("2"));
        assert_eq!(get(&map, "grpc-status"), None);
        assert_eq!(pairs(&map), text(&[("x-count", "2"), ("x-count", "3")]));
    }

    #[test]
    fn replace_all_sets_the_pairs_it_receives() {
        let mut map = trailers();

        let result = map.replace_all(&[(b"a", b"1"), (b"a", b"2")]);

        assert_eq!(result, Ok(()));
        assert_eq!(pairs(&map), text(&[("a", "1"), ("a", "2")]));
    }

    #[test]
    fn a_refused_name_or_value_changes_nothing() {
        let mut map = trailers();

        let results = [
            map.set(b":status", b"200"),
            map.add(b"bad name", b"v"),
            map.set(b"x-ok", b"bad\nvalue"),
            map.replace_all(&[(b"a", b"1"), (b"bad name", b"v")]),
        ];

        assert_eq!(results, [Err(NotAllowed); 4]);
        assert_eq!(pairs(&map), text(&[("grpc-status", "0"), ("x-count", "1")]));
    }
}
