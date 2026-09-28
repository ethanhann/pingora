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

use super::slot::LockedSlot;
use super::{ResponseProgress, WasmCtx};
use crate::plugin_unavailable;
use crate::stream::PluginResponse;
use http::header::{CONTENT_LENGTH, TRANSFER_ENCODING};
use http::{Method, StatusCode, Version};
use log::warn;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_http::ResponseHeader;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::Action;

const CHUNKED: &str = "chunked";

/// The source of the response header in a response pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResponseSource {
    Upstream,
    /// A plugin. A response that another plugin sends in its place is dropped.
    Plugin,
}

impl WasmCtx {
    /// Run `proxy_on_response_headers` of each plugin that saw the request, in reverse chain order.
    ///
    /// Call it from `response_filter`. Plugins can read the request headers, and can read and
    /// change the response headers before they go to the downstream. Informational (1xx) responses
    /// other than 101 do not run the plugins.
    ///
    /// A plugin that changes the length of the body removes `content-length` here. This phase then
    /// adds `transfer-encoding: chunked`, as Pingora does for a response with no length, so that
    /// the downstream connection stays open after the response.
    ///
    /// A plugin can send its own response in place of the upstream response. The plugins that did
    /// not run yet are skipped, and this phase writes the response to the downstream. The phase
    /// then returns an error with the status of the response to stop the request, and
    /// [WasmCtx::plugin_responded] returns `true`. The downstream connection closes after the
    /// response.
    ///
    /// # Errors
    ///
    /// An error of type [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) when a plugin traps, fails,
    /// or pauses the response, or when the guest that held this request was replaced after a
    /// failure.
    pub async fn response_filter<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        resp: &mut ResponseHeader,
    ) -> Result<()> {
        if session.subrequest_ctx.is_some()
            || skips_response(resp.status)
            || self.response_progress == ResponseProgress::FromPlugin
        {
            return Ok(());
        }
        self.chain.runtime.start_ticker()?;
        self.response_progress = ResponseProgress::FromUpstream;
        let end_of_stream = response_ends(&session.req_header().method, resp);
        let positions = (0..self.records.len()).rev();
        match self.response_pass(
            session,
            resp,
            positions,
            end_of_stream,
            ResponseSource::Upstream,
        )? {
            Some((position, response)) => Err(self
                .respond_in_place_of_upstream(session, position, response)
                .await),
            None => Ok(()),
        }
    }

    /// Run `proxy_on_response_headers` of the plugins at `positions`, in that order, on `resp`.
    ///
    /// Return the response that a plugin sent in place of the upstream response, with the position
    /// of that plugin. Add chunked framing to `resp` when a plugin removed its `content-length`.
    pub(super) fn response_pass<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        resp: &mut ResponseHeader,
        positions: impl Iterator<Item = usize>,
        end_of_stream: bool,
        origin: ResponseSource,
    ) -> Result<Option<(usize, PluginResponse)>> {
        let runtime = self.chain.runtime.clone();
        let had_length = resp.headers.contains_key(CONTENT_LENGTH);
        for position in positions {
            let Some(record) = self.records[position] else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            let mut locked = LockedSlot::of_request(pool, &record)?;
            let guest = &mut locked.loaded()?.guest;
            self.request_in(session.req_header_mut());
            self.response_in(resp);
            let count = self.response_count();
            let action = self.run(guest, |scope| {
                scope.on_response_headers(record.context, count, end_of_stream)
            });
            self.response_out(resp);
            self.request_out(session.req_header_mut());
            let sent = self.stream().plugin_response.take();
            let action = match action {
                Ok(action) => action,
                Err(e) => return Err(locked.guest_failure("failed in on_response_headers", e)),
            };
            match (sent, origin) {
                (Some(response), ResponseSource::Upstream) => {
                    return Ok(Some((position, response)))
                }
                (Some(_), ResponseSource::Plugin) => warn!(
                    "wasm plugin {} sent a response in place of a plugin response",
                    pool.name
                ),
                (None, _) if action == Action::Pause => {
                    return Err(plugin_unavailable(&pool.name, "paused a response"))
                }
                (None, _) => {}
            }
        }
        if had_length && !end_of_stream {
            frame_as_chunked(resp)?;
        }
        Ok(None)
    }
}

/// Add `transfer-encoding: chunked` to a response with no length, as Pingora does before
/// `response_filter`.
fn frame_as_chunked(resp: &mut ResponseHeader) -> Result<()> {
    let framed =
        resp.headers.contains_key(CONTENT_LENGTH) || resp.headers.contains_key(TRANSFER_ENCODING);
    if framed || resp.status.is_informational() {
        return Ok(());
    }
    resp.set_version(Version::HTTP_11);
    resp.insert_header(TRANSFER_ENCODING, CHUNKED)
}

/// Return `true` for an informational response other than 101.
fn skips_response(status: StatusCode) -> bool {
    status.is_informational() && status != StatusCode::SWITCHING_PROTOCOLS
}

/// Return `true` for a response that has no body.
fn response_ends(method: &Method, resp: &ResponseHeader) -> bool {
    *method == Method::HEAD
        || resp.status == StatusCode::NO_CONTENT
        || resp.status == StatusCode::NOT_MODIFIED
        || resp
            .headers
            .get(CONTENT_LENGTH)
            .is_some_and(|len| len.as_bytes() == b"0")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        add_request_header, body_chunk, body_plugin, one_plugin, read_downstream, session,
        start_request, Wat, GET, HEAD, POST, REMOVE_LENGTH, TEAPOT, TRAP,
    };
    use crate::ERR_PLUGIN_FAILED;
    use pingora_error::ErrorType;

    fn response(status: u16, length: Option<&str>) -> ResponseHeader {
        let mut resp = ResponseHeader::build(status, None).unwrap();
        if let Some(length) = length {
            resp.insert_header(CONTENT_LENGTH, length).unwrap();
        }
        resp
    }

    #[tokio::test]
    async fn a_response_on_a_replaced_guest_fails() {
        let (runtime, mut ctx) = one_plugin(add_request_header());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        runtime.inner.pools[0].replace_slot(0);
        let mut resp = ResponseHeader::build(200, None).unwrap();

        let err = ctx
            .response_filter(&mut session, &mut resp)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        assert!(err.to_string().contains("lost the guest of this request"));
    }

    #[tokio::test]
    async fn a_plugin_response_replaces_the_upstream_response() {
        let plugins = vec![
            body_plugin("first", Wat::response_headers(TRAP)),
            body_plugin("last", Wat::response_headers(TEAPOT)),
        ];
        let (_runtime, mut ctx, mut session, mut client) = start_request(plugins, GET).await;
        let mut upstream = response(200, Some("6"));

        let err = ctx
            .response_filter(&mut session, &mut upstream)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(418));
        assert!(ctx.plugin_responded());
        let written = read_downstream(&mut client).await;
        assert!(written.starts_with("HTTP/1.1 418"), "{written}");
        assert!(written.ends_with("\r\n\r\nteapot"), "{written}");
        assert!(written.contains("\r\nConnection: close\r\n"), "{written}");
    }

    #[tokio::test]
    async fn a_plugin_response_that_is_not_written_is_a_failure() {
        let plugins = vec![body_plugin("a", Wat::response_headers(TEAPOT))];
        let (_runtime, mut ctx, mut session, client) = start_request(plugins, GET).await;
        drop(client);
        let mut upstream = response(200, Some("6"));

        let err = ctx
            .response_filter(&mut session, &mut upstream)
            .await
            .unwrap_err();

        assert_ne!(err.etype(), &ErrorType::HTTPStatus(418));
        assert!(!ctx.plugin_responded());
    }

    #[tokio::test]
    async fn a_plugin_response_to_a_head_request_has_no_body() {
        let plugins = vec![body_plugin("a", Wat::response_headers(TEAPOT))];
        let (_runtime, mut ctx, mut session, mut client) = start_request(plugins, HEAD).await;
        let mut upstream = response(200, Some("6"));

        let err = ctx
            .response_filter(&mut session, &mut upstream)
            .await
            .unwrap_err();

        assert_eq!(err.etype(), &ErrorType::HTTPStatus(418));
        let written = read_downstream(&mut client).await;
        assert!(written.starts_with("HTTP/1.1 418"), "{written}");
        assert!(written.contains("\r\nContent-Length: 6\r\n"), "{written}");
        assert!(written.ends_with("\r\n\r\n"), "{written}");
    }

    #[tokio::test]
    async fn a_plugin_response_with_no_length_is_framed_as_chunked() {
        let plugins = vec![
            body_plugin("first", Wat::response_headers(REMOVE_LENGTH)),
            body_plugin("last", Wat::request_body(TEAPOT)),
        ];
        let (_runtime, mut ctx, mut session, mut client) = start_request(plugins, POST).await;

        ctx.request_body_filter(&mut session, &mut body_chunk("attack"), false)
            .await
            .unwrap_err();

        let written = read_downstream(&mut client).await;
        assert!(
            written.contains("\r\nTransfer-Encoding: chunked\r\n"),
            "{written}"
        );
        assert!(written.ends_with("6\r\nteapot\r\n0\r\n\r\n"), "{written}");
    }

    #[tokio::test]
    async fn a_removed_length_is_framed_as_chunked() {
        let cases = [
            (GET, 200, Some(CHUNKED)),
            (HEAD, 200, None),
            (GET, 204, None),
            (GET, 304, None),
        ];

        for (request, status, encoding) in cases {
            let plugins = vec![body_plugin("a", Wat::response_headers(REMOVE_LENGTH))];
            let (_runtime, mut ctx, mut session, _client) = start_request(plugins, request).await;
            let mut upstream = response(status, Some("6"));

            ctx.response_filter(&mut session, &mut upstream)
                .await
                .unwrap();

            let framing = upstream.headers.get(TRANSFER_ENCODING);
            assert_eq!(framing.map(|v| v.to_str().unwrap()), encoding, "{status}");
            assert!(upstream.headers.get(CONTENT_LENGTH).is_none());
        }
    }

    #[tokio::test]
    async fn a_length_that_stays_adds_no_framing() {
        let (_runtime, mut ctx) = one_plugin(add_request_header());
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        let mut upstream = response(200, Some("6"));

        ctx.response_filter(&mut session, &mut upstream)
            .await
            .unwrap();

        assert_eq!(upstream.headers[CONTENT_LENGTH], "6");
        assert!(upstream.headers.get(TRANSFER_ENCODING).is_none());
    }

    #[test]
    fn skips_response_for_informational_other_than_101() {
        let statuses = [100, 101, 103, 199, 200, 404];

        let skipped: Vec<_> = statuses
            .iter()
            .map(|s| skips_response(StatusCode::from_u16(*s).unwrap()))
            .collect();

        assert_eq!(skipped, [true, false, true, true, false, false]);
    }

    #[test]
    fn response_ends_follows_the_rule() {
        let cases = [
            (Method::GET, response(200, None), false),
            (Method::GET, response(200, Some("10")), false),
            (Method::GET, response(200, Some("0")), true),
            (Method::GET, response(204, None), true),
            (Method::GET, response(304, None), true),
            (Method::HEAD, response(200, Some("10")), true),
        ];

        let ends: Vec<_> = cases
            .iter()
            .map(|(method, resp, _)| response_ends(method, resp))
            .collect();

        let want: Vec<_> = cases.iter().map(|c| c.2).collect();
        assert_eq!(ends, want);
    }
}
