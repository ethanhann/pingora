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

//! Callout results
//!
//! A callout that gets no response header from its peer is given a synthetic response instead.
//! These responses use the status codes and bodies that plugins are written against.

use crate::observability::CalloutFailure;
use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::StatusCode;
use pingora_error::{Error, ErrorType};
use proxy_wasm_host::abi::v0_2_1::{HeaderPairs, HttpCallResponse};
use std::borrow::Cow;

pub(super) const PSEUDO_STATUS: &[u8] = b":status";
const TEXT_PLAIN: &[u8] = b"text/plain";
const TIMEOUT_BODY: &str = "upstream request timeout";
const NO_HEALTHY_UPSTREAM_BODY: &str = "no healthy upstream";
const RESET_BODY_PREFIX: &str =
    "upstream connect error or disconnect/reset before headers. reset reason: ";
const RESET_REASON_OVERFLOW: &str = "overflow";
const RESET_REASON_CONNECT_TIMEOUT: &str = "connection timeout";
const RESET_REASON_CONNECT_FAILURE: &str = "remote connection failure";
const RESET_REASON_PROTOCOL_ERROR: &str = "protocol error";
const RESET_REASON_TERMINATION: &str = "connection termination";

pub(crate) type OwnedHeaderPairs = Vec<(Vec<u8>, Vec<u8>)>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HttpCalloutResult {
    /// A response from the peer, or a synthetic one if no response header was received.
    Response {
        headers: OwnedHeaderPairs,
        body: Bytes,
        trailers: OwnedHeaderPairs,
    },
    /// The callout failed after its response header was received, its response body exceeded
    /// the limit, or the task sending it did not finish.
    Failed,
}

impl HttpCalloutResult {
    pub(crate) fn as_http_call_response(&self) -> HttpCallResponse<'_> {
        match self {
            HttpCalloutResult::Response {
                headers,
                body,
                trailers,
            } => HttpCallResponse::received(borrowed_header_pairs(headers))
                .with_body(Cow::Borrowed(&body[..]))
                .with_trailers(borrowed_header_pairs(trailers)),
            HttpCalloutResult::Failed => HttpCallResponse::failed(),
        }
    }

    fn synthetic_response(status: StatusCode, body: String) -> Self {
        let length = body.len().to_string().into_bytes();
        let headers = vec![
            (PSEUDO_STATUS.to_vec(), status.as_str().as_bytes().to_vec()),
            (CONTENT_LENGTH.as_str().as_bytes().to_vec(), length),
            (
                CONTENT_TYPE.as_str().as_bytes().to_vec(),
                TEXT_PLAIN.to_vec(),
            ),
        ];
        HttpCalloutResult::Response {
            headers,
            body: Bytes::from(body),
            trailers: Vec::new(),
        }
    }

    fn reset_response(status: StatusCode, reason: &str) -> Self {
        Self::synthetic_response(status, format!("{RESET_BODY_PREFIX}{reason}"))
    }

    pub(crate) fn timeout_response() -> Self {
        Self::synthetic_response(StatusCode::GATEWAY_TIMEOUT, TIMEOUT_BODY.to_string())
    }

    pub(crate) fn no_healthy_upstream_response() -> Self {
        let body = NO_HEALTHY_UPSTREAM_BODY.to_string();
        Self::synthetic_response(StatusCode::SERVICE_UNAVAILABLE, body)
    }

    pub(crate) fn overflow_response() -> Self {
        Self::reset_response(StatusCode::SERVICE_UNAVAILABLE, RESET_REASON_OVERFLOW)
    }

    pub(crate) fn connect_failure_response(e: &Error) -> Self {
        let reason = match e.etype() {
            ErrorType::ConnectTimedout | ErrorType::TLSHandshakeTimedout => {
                RESET_REASON_CONNECT_TIMEOUT
            }
            _ => RESET_REASON_CONNECT_FAILURE,
        };
        Self::reset_response(StatusCode::SERVICE_UNAVAILABLE, reason)
    }

    pub(crate) fn response_for_session_error(e: &Error) -> Self {
        match e.etype() {
            ErrorType::ReadTimedout | ErrorType::WriteTimedout => Self::timeout_response(),
            ErrorType::InvalidHTTPHeader
            | ErrorType::H1Error
            | ErrorType::H2Error
            | ErrorType::InvalidH2
            | ErrorType::H2Downgrade => {
                Self::reset_response(StatusCode::BAD_GATEWAY, RESET_REASON_PROTOCOL_ERROR)
            }
            _ => Self::reset_response(StatusCode::SERVICE_UNAVAILABLE, RESET_REASON_TERMINATION),
        }
    }
}

pub(super) fn borrowed_header_pairs(pairs: &[(Vec<u8>, Vec<u8>)]) -> HeaderPairs<'_> {
    pairs
        .iter()
        .map(|(name, value)| (Cow::Borrowed(&name[..]), Cow::Borrowed(&value[..])))
        .collect()
}

pub(crate) fn connect_failure(e: &Error) -> CalloutFailure {
    match e.etype() {
        ErrorType::ConnectTimedout | ErrorType::TLSHandshakeTimedout => {
            CalloutFailure::ConnectTimeout
        }
        _ => CalloutFailure::ConnectFailed,
    }
}

pub(crate) fn session_failure(e: &Error) -> CalloutFailure {
    match e.etype() {
        ErrorType::ReadTimedout | ErrorType::WriteTimedout => CalloutFailure::Timeout,
        ErrorType::InvalidHTTPHeader
        | ErrorType::H1Error
        | ErrorType::H2Error
        | ErrorType::InvalidH2
        | ErrorType::H2Downgrade => CalloutFailure::ProtocolError,
        _ => CalloutFailure::ConnectionClosed,
    }
}
