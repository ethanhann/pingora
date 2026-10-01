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

//! The responses that plugins send after the request headers.

use super::response::{frame_if_length_removed, ResponseSource};
use super::{ResponseProgress, WasmCtx};
use crate::stream_state::PluginResponse;
use bytes::Bytes;
use http::header::CONTENT_LENGTH;
use http::Method;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::{Error, ErrorType, Result};
use pingora_http::ResponseHeader;
use pingora_proxy::Session;

impl WasmCtx {
    /// Return `true` after a plugin sent its own response.
    ///
    /// [WasmCtx::request_body_filter] and [WasmCtx::response_filter] write the response of a plugin
    /// and return an error to stop the request. Use this in `fail_to_proxy` and `logging` to tell
    /// that error from a failure. Pingora logs the error unless `suppress_error_log` returns
    /// `true`, so you can return this from `suppress_error_log`.
    ///
    /// This is also `true` after [WasmCtx::request_filter] returned
    /// [RequestOutcome::Respond](crate::RequestOutcome::Respond).
    pub fn plugin_responded(&self) -> bool {
        self.response_progress == ResponseProgress::FromPlugin
    }

    /// Run `proxy_on_response_headers` on the response that the plugin at `position` sent.
    ///
    /// That plugin and the plugins before it in the chain run, in reverse chain order.
    pub(super) fn pass_plugin_response<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        header: &mut ResponseHeader,
        no_body: bool,
    ) -> Result<()> {
        let end_of_stream = no_body || session.req_header().method == Method::HEAD;
        let had_length = header.headers.contains_key(CONTENT_LENGTH);
        let positions = (0..=position).rev();
        self.response_pass(
            session,
            header,
            positions,
            end_of_stream,
            ResponseSource::Plugin,
        )?;
        frame_if_length_removed(header, had_length, end_of_stream)
    }

    /// Write the response that a plugin sent to a request body, and return the error that stops the
    /// request.
    ///
    /// Return a failure and write nothing when the plugins already ran on the upstream response.
    pub(super) async fn respond_to_request_body<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        response: PluginResponse,
    ) -> Box<Error> {
        if self.response_progress != ResponseProgress::NotStarted {
            return self.late_response_error(position);
        }
        let mut header = response.header;
        let no_body = response.body.is_empty();
        if let Err(e) = self.pass_plugin_response(session, position, &mut header, no_body) {
            return e;
        }
        let response = PluginResponse {
            header,
            body: response.body,
        };
        self.write_response(session, position, response).await
    }

    /// Write the response that a plugin sent in place of the upstream response, and return the
    /// error that stops the request.
    pub(super) async fn respond_in_place_of_upstream<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        response: PluginResponse,
    ) -> Box<Error> {
        self.write_response(session, position, response).await
    }

    async fn write_response<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        position: usize,
        response: PluginResponse,
    ) -> Box<Error> {
        let status = response.header.status.as_u16();
        // The request stops here with its body unread, so the connection cannot be reused
        session.as_downstream_mut().set_keepalive(None);
        let header = Box::new(response.header);
        if let Err(e) = write_plugin_response(session, header, response.body).await {
            return e;
        }
        self.response_progress = ResponseProgress::FromPlugin;
        let plugin = &self.pool_at(position).name;
        Error::explain(
            ErrorType::HTTPStatus(status),
            format!("wasm plugin {plugin} sent its own response"),
        )
    }

    /// Return the error for a plugin response that came after the response header.
    pub(super) fn late_response_error(&self, position: usize) -> Box<Error> {
        self.plugin_error(position, "sent a response after the response header")
    }
}

/// Write the response that a plugin sent to the downstream.
///
/// Use it for the header and the body of [RequestOutcome::Respond](crate::RequestOutcome). If
/// your proxy writes its responses with its own code, for example to add headers or record
/// metrics, write the header and the body with that code instead.
pub async fn write_plugin_response<DS: DownstreamSession>(
    session: &mut Session<DS>,
    header: Box<ResponseHeader>,
    body: Bytes,
) -> Result<()> {
    if session.req_header().method == Method::HEAD || body.is_empty() {
        session.write_response_header(header, true).await?;
        // An HTTP/2 stream needs an end of stream after the header
        return session.write_response_body(None, true).await;
    }
    session.write_response_header(header, false).await?;
    session.write_response_body(Some(body), true).await
}
