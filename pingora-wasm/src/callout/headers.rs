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

//! The request header of a callout.

use http::header::{HeaderName, CONNECTION, CONTENT_LENGTH, HOST, TE, TRANSFER_ENCODING, UPGRADE};
use http::Method;
use log::debug;
use pingora_http::RequestHeader;
use proxy_wasm_host::abi::v0_2_1::HeaderPairs;

const PSEUDO_PREFIX: &[u8] = b":";
const PSEUDO_AUTHORITY: &[u8] = b":authority";
const PSEUDO_METHOD: &[u8] = b":method";
const PSEUDO_PATH: &[u8] = b":path";
const KEEP_ALIVE: &str = "keep-alive";
const PROXY_CONNECTION: &str = "proxy-connection";

/// Return `true` for a hop-by-hop header and for a header that frames the body.
fn is_framing_or_hop_header(name: &HeaderName) -> bool {
    [CONTENT_LENGTH, TRANSFER_ENCODING, CONNECTION, UPGRADE, TE].contains(name)
        || name == KEEP_ALIVE
        || name == PROXY_CONNECTION
}

/// Build the request header of a callout from the headers that `plugin` passed.
///
/// The method, the path, and the `host` come from the pseudo headers. The crate sets
/// `content-length` itself, so a plugin cannot frame the body in another way. Return `None` when
/// a header is not valid.
pub(crate) fn callout_request_header(
    plugin: &str,
    pairs: &HeaderPairs<'_>,
    body_len: usize,
) -> Option<RequestHeader> {
    let pseudo_header = |name: &[u8]| {
        pairs
            .iter()
            .find(|(key, _)| key.as_ref() == name)
            .map(|(_, value)| value.as_ref())
    };
    let method = Method::from_bytes(pseudo_header(PSEUDO_METHOD)?).ok()?;
    let path = pseudo_header(PSEUDO_PATH)?;
    let mut request = RequestHeader::build(method, path, Some(pairs.len())).ok()?;
    request
        .insert_header(HOST, pseudo_header(PSEUDO_AUTHORITY)?)
        .ok()?;
    for (key, value) in pairs {
        if key.starts_with(PSEUDO_PREFIX) {
            continue;
        }
        let name = HeaderName::from_bytes(key).ok()?;
        if name == HOST || is_framing_or_hop_header(&name) {
            debug!("wasm plugin {plugin} passed the callout header {name}, which is not sent");
            continue;
        }
        request.append_header(name, value.as_ref()).ok()?;
    }
    if body_len > 0 {
        request.insert_header(CONTENT_LENGTH, body_len).ok()?;
    }
    Some(request)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::borrow::Cow;

    pub(crate) fn pairs(list: &[(&'static str, &'static str)]) -> HeaderPairs<'static> {
        list.iter()
            .map(|(name, value)| {
                (
                    Cow::Borrowed(name.as_bytes()),
                    Cow::Borrowed(value.as_bytes()),
                )
            })
            .collect()
    }

    pub(crate) fn post_to_authz() -> Vec<(&'static str, &'static str)> {
        vec![
            (":method", "POST"),
            (":path", "/check?dry=1"),
            (":authority", "authz.test"),
        ]
    }

    #[test]
    fn a_callout_request_drops_hop_by_hop_headers_and_sets_its_length() {
        let mut headers = post_to_authz();
        headers.extend([
            (":scheme", "https"),
            ("x-token", "one"),
            ("x-token", "two"),
            ("host", "other.test"),
            ("content-length", "100"),
            ("transfer-encoding", "chunked"),
            ("connection", "upgrade"),
            ("keep-alive", "timeout=5"),
            ("upgrade", "websocket"),
            ("te", "trailers"),
            ("proxy-connection", "keep-alive"),
        ]);

        let request = callout_request_header("a", &pairs(&headers), 4).unwrap();
        let no_body = callout_request_header("a", &pairs(&headers), 0).unwrap();

        assert_eq!(request.method, Method::POST);
        assert_eq!(request.raw_path(), b"/check?dry=1");
        let sent: Vec<_> = request
            .headers
            .iter()
            .map(|(name, value)| (name.as_str(), value.to_str().unwrap()))
            .collect();
        let want = [
            ("host", "authz.test"),
            ("x-token", "one"),
            ("x-token", "two"),
            ("content-length", "4"),
        ];
        assert_eq!(sent, want);
        assert!(!no_body.headers.contains_key(CONTENT_LENGTH));
    }

    #[test]
    fn a_callout_request_needs_valid_headers() {
        let mut bad_name = post_to_authz();
        bad_name.push(("bad name", "value"));
        let mut bad_method = post_to_authz();
        bad_method[0] = (":method", "NOT A TOKEN");
        let cases = [bad_name, bad_method];

        let built = cases.map(|headers| callout_request_header("a", &pairs(&headers), 0));

        assert!(built.iter().all(Option::is_none));
    }
}
