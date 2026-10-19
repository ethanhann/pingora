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

//! Callout request header

use http::header::{HeaderName, CONNECTION, CONTENT_LENGTH, HOST, TE, TRANSFER_ENCODING, UPGRADE};
use http::Method;
use log::debug;
use pingora_http::RequestHeader;
use proxy_wasm_host::abi::v0_2_1::HeaderPairs;
use std::fmt;

const PSEUDO_PREFIX: &str = ":";
const PSEUDO_AUTHORITY: &str = ":authority";
const PSEUDO_METHOD: &str = ":method";
const PSEUDO_PATH: &str = ":path";
const KEEP_ALIVE: &str = "keep-alive";
const PROXY_CONNECTION: &str = "proxy-connection";

fn is_framing_or_hop_header(name: &HeaderName) -> bool {
    [CONTENT_LENGTH, TRANSFER_ENCODING, CONNECTION, UPGRADE, TE].contains(name)
        || name == KEEP_ALIVE
        || name == PROXY_CONNECTION
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RejectedCalloutHeader {
    MissingPseudo(&'static str),
    InvalidPseudo(&'static str),
    InvalidRegular(String),
}

impl fmt::Display for RejectedCalloutHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RejectedCalloutHeader::MissingPseudo(name) => {
                write!(f, "pseudo-header {name} is missing")
            }
            RejectedCalloutHeader::InvalidPseudo(name) => {
                write!(f, "pseudo-header {name} is invalid")
            }
            RejectedCalloutHeader::InvalidRegular(name) => write!(f, "header {name} is invalid"),
        }
    }
}

pub(crate) fn callout_request_header(
    plugin: &str,
    pairs: &HeaderPairs<'_>,
    body_len: usize,
) -> Result<RequestHeader, RejectedCalloutHeader> {
    use RejectedCalloutHeader::{InvalidPseudo, InvalidRegular, MissingPseudo};
    let pseudo_header = |name: &'static str| {
        pairs
            .iter()
            .find(|(key, _)| key.as_ref() == name.as_bytes())
            .map(|(_, value)| value.as_ref())
            .ok_or(MissingPseudo(name))
    };
    let method = Method::from_bytes(pseudo_header(PSEUDO_METHOD)?)
        .map_err(|_| InvalidPseudo(PSEUDO_METHOD))?;
    let path = pseudo_header(PSEUDO_PATH)?;
    let mut request = RequestHeader::build(method, path, Some(pairs.len()))
        .map_err(|_| InvalidPseudo(PSEUDO_PATH))?;
    request
        .insert_header(HOST, pseudo_header(PSEUDO_AUTHORITY)?)
        .map_err(|_| InvalidPseudo(PSEUDO_AUTHORITY))?;
    for (key, value) in pairs {
        if key.starts_with(PSEUDO_PREFIX.as_bytes()) {
            continue;
        }
        // A header value is never part of the error, since it can be a credential
        let invalid = || InvalidRegular(String::from_utf8_lossy(key).into_owned());
        let name = HeaderName::from_bytes(key).map_err(|_| invalid())?;
        if name == HOST || is_framing_or_hop_header(&name) {
            debug!("wasm plugin {plugin}: callout header {name} dropped, host, framing, and hop-by-hop headers are not forwarded");
            continue;
        }
        request
            .append_header(name, value.as_ref())
            .map_err(|_| invalid())?;
    }
    // `content-length` is set here and dropped from the pairs above, so a plugin cannot frame
    // the body any other way
    if body_len > 0 {
        request
            .insert_header(CONTENT_LENGTH, body_len)
            .map_err(|_| InvalidRegular(CONTENT_LENGTH.to_string()))?;
    }
    Ok(request)
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
    fn callout_request_drops_hop_headers_and_sets_content_length() {
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
    fn callout_request_rejects_invalid_headers() {
        let mut bad_name = post_to_authz();
        bad_name.push(("bad name", "value"));
        let mut bad_value = post_to_authz();
        bad_value.push(("x-token", "line\nbreak"));
        let mut bad_method = post_to_authz();
        bad_method[0] = (":method", "NOT A TOKEN");
        let no_authority = post_to_authz()[..2].to_vec();
        let cases = [
            (bad_name, "header bad name is invalid"),
            (bad_value, "header x-token is invalid"),
            (bad_method, "pseudo-header :method is invalid"),
            (no_authority, "pseudo-header :authority is missing"),
        ];

        for (headers, rejected) in cases {
            let built = callout_request_header("a", &pairs(&headers), 0);

            assert_eq!(built.unwrap_err().to_string(), rejected);
        }
    }
}
