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

use bytes::Bytes;
use http::header::{HeaderValue, CONTENT_LENGTH, TRANSFER_ENCODING};
use http::{Method, StatusCode};
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_http::ResponseHeader;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::LocalResponse;

/// A response that a plugin sent in place of the upstream response, with
/// `proxy_send_local_response`.
#[derive(Debug)]
pub(crate) struct PluginResponse {
    pub(crate) header: ResponseHeader,
    pub(crate) body: Bytes,
}

impl PluginResponse {
    /// Builds the response, or `None` for a status or a header that is not valid.
    pub(crate) fn build(response: &LocalResponse<'_>) -> Option<Self> {
        let status = u16::try_from(response.status_code)
            .ok()
            .and_then(|code| StatusCode::from_u16(code).ok())
            .filter(is_final)?;
        let mut header = ResponseHeader::build(status, Some(response.headers.len() + 1)).ok()?;
        for (key, value) in &response.headers {
            let name = std::str::from_utf8(key).ok()?;
            if name.starts_with(':') {
                return None;
            }
            if name.eq_ignore_ascii_case(CONTENT_LENGTH.as_str())
                || name.eq_ignore_ascii_case(TRANSFER_ENCODING.as_str())
            {
                continue;
            }
            let value = HeaderValue::from_bytes(value).ok()?;
            header.append_header(name.to_string(), value).ok()?;
        }
        header
            .insert_header(CONTENT_LENGTH, response.body.len())
            .ok()?;
        Some(PluginResponse {
            header,
            body: Bytes::copy_from_slice(&response.body),
        })
    }
}

/// Writes a plugin response that a [WasmCtx](crate::WasmCtx) returned.
///
/// A proxy that writes responses with its own code can use that code instead.
pub async fn write_plugin_response<DS: DownstreamSession>(
    session: &mut Session<DS>,
    header: Box<ResponseHeader>,
    body: Bytes,
) -> Result<()> {
    if session.req_header().method == Method::HEAD || body.is_empty() {
        return session.write_response_header(header, true).await;
    }
    session.write_response_header(header, false).await?;
    session.write_response_body(Some(body), true).await
}

/// A status that ends a response, from 200 to 599.
fn is_final(status: &StatusCode) -> bool {
    status.is_success()
        || status.is_redirection()
        || status.is_client_error()
        || status.is_server_error()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    fn response(status: u32, headers: &[(&'static str, &'static str)]) -> LocalResponse<'static> {
        let headers = headers
            .iter()
            .map(|(k, v)| (Cow::Borrowed(k.as_bytes()), Cow::Borrowed(v.as_bytes())))
            .collect();
        LocalResponse::new(status)
            .with_headers(headers)
            .with_body(&b"denied"[..])
    }

    #[test]
    fn build_sets_the_status_the_headers_and_the_length() {
        let local = response(403, &[("x-denied", "yes"), ("X-Case", "Kept")]);

        let built = PluginResponse::build(&local).unwrap();

        assert_eq!(built.header.status, 403);
        assert_eq!(built.header.headers["x-denied"], "yes");
        assert_eq!(built.header.headers["x-case"], "Kept");
        assert_eq!(built.header.headers[CONTENT_LENGTH], "6");
        assert_eq!(built.body, Bytes::from_static(b"denied"));
    }

    #[test]
    fn build_drops_the_guest_framing_headers() {
        let local = response(
            200,
            &[("Content-Length", "999"), ("transfer-encoding", "chunked")],
        );

        let built = PluginResponse::build(&local).unwrap();

        assert_eq!(
            built.header.headers.get_all(CONTENT_LENGTH).iter().count(),
            1
        );
        assert_eq!(built.header.headers[CONTENT_LENGTH], "6");
        assert!(built.header.headers.get(TRANSFER_ENCODING).is_none());
    }

    #[test]
    fn build_refuses_a_status_outside_200_to_599() {
        let statuses = [0, 100, 199, 600, 999];

        let built: Vec<_> = statuses
            .iter()
            .map(|s| PluginResponse::build(&response(*s, &[])).is_some())
            .collect();

        assert_eq!(built, [false; 5]);
    }

    #[test]
    fn build_refuses_a_bad_header() {
        let bad = [
            response(200, &[("bad name", "v")]),
            response(200, &[("x-ok", "bad\nvalue")]),
            response(200, &[(":status", "200")]),
        ];

        let built: Vec<_> = bad
            .iter()
            .map(|r| PluginResponse::build(r).is_some())
            .collect();

        assert_eq!(built, [false; 3]);
    }
}
