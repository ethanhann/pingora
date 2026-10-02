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

//! Plugin responses
//!
//! A plugin may send its own response instead of letting a request or an upstream response
//! through. The helpers here run that response past the plugin that sent it and the plugins ahead
//! of it in the chain, and write it to the downstream.

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
    /// Return `true` if a plugin sent its own response to this request.
    ///
    /// [WasmCtx::request_body_filter] and [WasmCtx::response_filter] write a plugin's response to
    /// the downstream themselves and then return an error to stop the request. Check this in
    /// `fail_to_proxy` and `logging` to tell that error apart from a real failure. You can also
    /// return it from `suppress_error_log` to keep Pingora from logging the error.
    ///
    /// It is `true` as well once [WasmCtx::request_filter] has returned
    /// [RequestOutcome::Respond](crate::RequestOutcome::Respond). In the other phases it only
    /// becomes `true` after the response has been written, so it stays `false` if that write
    /// fails.
    pub fn plugin_responded(&self) -> bool {
        self.response_progress == ResponseProgress::FromPlugin
    }

    /// Run `proxy_on_response_headers` on a response sent by the plugin at `position`.
    ///
    /// The callback runs for that plugin and every plugin ahead of it, in reverse chain order.
    /// With `no_body` set, the callback runs with end of stream set.
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

    /// Write a response a plugin sent from the request body phase and return the error that stops
    /// the request.
    ///
    /// If the plugins have already run on a response header, nothing is written and a plugin
    /// failure is returned instead.
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

    /// Write a response a plugin sent in place of the upstream response and return the error that
    /// stops the request.
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
        // The request is stopped without draining its body, so the connection can't be reused
        session.as_downstream_mut().set_keepalive(None);
        let header = Box::new(response.header);
        if let Err(e) = write_plugin_response(session, header, response.body).await {
            return e;
        }
        self.response_progress = ResponseProgress::FromPlugin;
        let plugin = &self.pool_at(position).name;
        Error::explain(
            ErrorType::HTTPStatus(status),
            format!("wasm plugin {plugin}: sent its own response"),
        )
    }

    /// Build the error for a plugin response sent too late to replace the response header.
    pub(super) fn late_response_error(&self, position: usize) -> Box<Error> {
        self.plugin_error(
            position,
            "response rejected, sent after the response header",
        )
    }
}

/// Write a plugin's response to the downstream.
///
/// Pass it the header and body from [RequestOutcome::Respond](crate::RequestOutcome). The body
/// is left out for a `HEAD` request. If your proxy has its own way of writing responses, e.g. to
/// add headers or record metrics, you can use that instead of this function.
///
/// # Errors
///
/// Returns the error from the session if writing the header or the body fails.
pub async fn write_plugin_response<DS: DownstreamSession>(
    session: &mut Session<DS>,
    header: Box<ResponseHeader>,
    body: Bytes,
) -> Result<()> {
    if session.req_header().method == Method::HEAD || body.is_empty() {
        session.write_response_header(header, true).await?;
        // The header alone does not end an HTTP/2 stream, so finish the empty body explicitly
        return session.write_response_body(None, true).await;
    }
    session.write_response_header(header, false).await?;
    session.write_response_body(Some(body), true).await
}
