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

use crate::callout::grpc::status;
use crate::callout::grpc::APPLICATION_GRPC;
use bytes::Bytes;
use http::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE, TRANSFER_ENCODING};
use http::StatusCode;
use pingora_http::ResponseHeader;
use proxy_wasm_host::abi::v0_2_1::LocalResponse;

/// A response a plugin sent with `proxy_send_local_response`, to be used in place of the
/// upstream response.
#[derive(Debug)]
pub(crate) struct PluginResponse {
    pub(crate) header: ResponseHeader,
    pub(crate) body: Bytes,
}

impl PluginResponse {
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
                // `content-length` is always set from the body
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

impl PluginResponse {
    /// Return the response in the form a gRPC client reads, with HTTP status 200 and the gRPC
    /// status and the body in `grpc-status` and `grpc-message`.
    pub(crate) fn into_grpc(self, grpc_status: Option<u32>) -> Option<Self> {
        let code =
            grpc_status.unwrap_or_else(|| status::from_http_status(self.header.status.as_u16()));
        let mut header =
            ResponseHeader::build(StatusCode::OK, Some(self.header.headers.len() + 2)).ok()?;
        for (name, value) in &self.header.headers {
            if name != CONTENT_LENGTH && name != CONTENT_TYPE {
                header.append_header(name.clone(), value.clone()).ok()?;
            }
        }
        header.insert_header(CONTENT_TYPE, APPLICATION_GRPC).ok()?;
        header.insert_header(status::GRPC_STATUS, code).ok()?;
        if !self.body.is_empty() {
            let message = status::percent_encode(&String::from_utf8_lossy(&self.body));
            header.insert_header(status::GRPC_MESSAGE, message).ok()?;
        }
        Some(PluginResponse {
            header,
            body: Bytes::new(),
        })
    }
}

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
    fn build_sets_status_headers_and_content_length() {
        let local = response(403, &[("x-denied", "yes"), ("X-Case", "Kept")]);

        let built = PluginResponse::build(&local).unwrap();

        assert_eq!(built.header.status, 403);
        assert_eq!(built.header.headers["x-denied"], "yes");
        assert_eq!(built.header.headers["x-case"], "Kept");
        assert_eq!(built.header.headers[CONTENT_LENGTH], "6");
        assert_eq!(built.body, Bytes::from_static(b"denied"));
    }

    #[test]
    fn build_drops_guest_framing_headers() {
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
    fn build_rejects_invalid_status_or_header() {
        let bad = [
            response(0, &[]),
            response(100, &[]),
            response(199, &[]),
            response(600, &[]),
            response(999, &[]),
            response(200, &[("bad name", "v")]),
            response(200, &[("x-ok", "bad\nvalue")]),
            response(200, &[(":status", "200")]),
        ];

        let built: Vec<_> = bad
            .iter()
            .map(|r| PluginResponse::build(r).is_some())
            .collect();

        assert_eq!(built, [false; 8]);
    }
}
