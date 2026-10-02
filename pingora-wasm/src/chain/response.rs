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

use super::failure::FilterFailure;
use super::wait::{CalloutWaitOutcome, PausedPhase};
use super::{ResponseProgress, WasmCtx};
use crate::stream_state::PluginResponse;
use http::header::{CONTENT_LENGTH, TRANSFER_ENCODING};
use http::{Method, StatusCode, Version};
use log::warn;
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_http::ResponseHeader;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::StreamType;
use proxy_wasm_host::abi::v0_2_1::Callback;

const CHUNKED: &str = "chunked";

/// Build the failure for a plugin that paused on response headers with nothing to wait for.
///
/// On an upstream response the plugin has no callout pending. On a plugin's response its callouts
/// are never started, so the pause could not end.
fn response_pause_failure(origin: ResponseSource) -> FilterFailure {
    let what = match origin {
        ResponseSource::Upstream => "paused on response headers with no callout pending",
        ResponseSource::Plugin => "paused on plugin response headers, callouts not started",
    };
    FilterFailure::paused(Callback::ResponseHeaders, what)
}

/// Where the response header given to [WasmCtx::response_pass] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResponseSource {
    Upstream,
    /// A plugin sent it. A response another plugin sends during the pass is dropped.
    Plugin,
}

impl WasmCtx {
    /// Run `proxy_on_response_headers` for each plugin that saw the request, in reverse chain order.
    ///
    /// Call this from your `response_filter`. Plugins can read the request headers, and can read
    /// and change the response headers before they are sent downstream. This filter does nothing
    /// for a subrequest, for an informational (1xx) response other than 101, and once a plugin has
    /// sent its own response.
    ///
    /// A plugin that changes the body length must remove `content-length` here. The response is
    /// then sent with `transfer-encoding: chunked`, as Pingora does for any response without a
    /// length, so that the downstream connection can be reused.
    ///
    /// A plugin may pause the response while waiting for a callout, in which case this filter
    /// waits until the plugin continues or sends its own response.
    ///
    /// A plugin may send its own response in place of the upstream's. The remaining plugins are
    /// skipped and the response is written to the downstream, which is closed afterwards. This
    /// filter then returns an error with the response status to stop the request, and
    /// [WasmCtx::plugin_responded] returns `true`.
    ///
    /// # Errors
    ///
    /// For a plugin with [FailPolicy::Closed](crate::FailPolicy::Closed), returns
    /// [ERR_PLUGIN_FAILED](crate::ERR_PLUGIN_FAILED) if the plugin traps or otherwise fails,
    /// pauses the response with no callout pending, waits for callouts longer than its
    /// [callout_wait_limit](crate::WasmPluginConf::callout_wait_limit), or lost the guest holding
    /// this request. A plugin with [FailPolicy::Open](crate::FailPolicy::Open) is skipped
    /// instead, unless it has already changed a body that can still have bytes to come, or that
    /// body's length, e.g. a request body that is still being sent upstream. See
    /// [fail_policy](crate::WasmPluginConf::fail_policy) for the full rule.
    ///
    /// Under both policies, the same error is returned if an earlier filter of this request was
    /// cancelled while a plugin was waiting for a callout, or if the runtime's threads cannot be
    /// started.
    pub async fn response_filter<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        resp: &mut ResponseHeader,
    ) -> Result<()> {
        self.stream().request_facts.response_code = Some(resp.status.as_u16());
        if session.subrequest_ctx.is_some()
            || skips_response(resp.status)
            || self.response_progress == ResponseProgress::FromPlugin
        {
            return Ok(());
        }
        self.refuse_after_cancelled_wait()?;
        self.chain.runtime.start_threads()?;
        self.response_progress = ResponseProgress::FromUpstream;
        let end_of_stream = response_ends(&session.req_header().method, resp);
        self.failures.response_body_ended = end_of_stream;
        let had_length = resp.headers.contains_key(CONTENT_LENGTH);
        let mut remaining = self.records.len();
        loop {
            let positions = (0..remaining).rev();
            let origin = ResponseSource::Upstream;
            let outcome = self.response_pass(session, resp, positions, end_of_stream, origin)?;
            let position = match outcome {
                ResponsePassOutcome::Finished => break,
                ResponsePassOutcome::Respond(position, response) => {
                    return Err(self
                        .respond_in_place_of_upstream(session, position, *response)
                        .await)
                }
                ResponsePassOutcome::WaitsForCallout(position) => position,
            };
            let phase = PausedPhase::ResponseHeaders(&mut *resp);
            match self.wait_for_callouts(session, position, phase).await? {
                CalloutWaitOutcome::Continued | CalloutWaitOutcome::PluginSkipped => {}
                CalloutWaitOutcome::StillPaused => {
                    self.skip_plugin_or_fail_request(position, response_pause_failure(origin))?;
                }
                CalloutWaitOutcome::Respond(response) => {
                    return Err(self
                        .respond_in_place_of_upstream(session, position, *response)
                        .await)
                }
            }
            remaining = position;
        }
        frame_if_length_removed(resp, had_length, end_of_stream)
    }

    /// Run `proxy_on_response_headers` on `resp` for the plugins at `positions`, in that order.
    ///
    /// Positions without a context for this request are passed over, and so are plugins already
    /// skipped. The pass stops early at a plugin that pauses with a callout pending, or that
    /// sends its own response while `origin` is the upstream. Callouts are only started for an
    /// upstream response, because the filters that pass a plugin's response along cannot wait on
    /// one.
    ///
    /// A plugin that fails, whose guest is gone, or that pauses with no callout pending is
    /// skipped if its fail policy allows it, and the pass continues with the next plugin.
    ///
    /// # Errors
    ///
    /// Returns the failure's error if the plugin failed and was not skipped.
    pub(super) fn response_pass<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        resp: &mut ResponseHeader,
        positions: impl Iterator<Item = usize>,
        end_of_stream: bool,
        origin: ResponseSource,
    ) -> Result<ResponsePassOutcome> {
        let runtime = self.chain.runtime.clone();
        for position in positions {
            let Some(record) = self.records[position] else {
                continue;
            };
            if self.is_skipped(position) {
                continue;
            }
            let pool = &runtime.pools[self.chain.plugins[position]];
            let callback = Callback::ResponseHeaders;
            let Some(mut locked) =
                self.lock_slot_or_skip_plugin(pool, position, &record, callback)?
            else {
                continue;
            };
            let loaded = locked.loaded()?;
            self.request_in(session.req_header_mut());
            self.response_in(resp);
            let count = self.response_count();
            let action = self.run_for_context(loaded, record.context, |scope| {
                scope.on_response_headers(record.context, count, end_of_stream)
            });
            self.response_out(position, resp);
            self.request_out(position, session.req_header_mut());
            let sent = self.stream().plugin_response.take();
            let action = match action {
                Ok(action) => action,
                Err(e) => {
                    self.guest_call_failed(position, locked, Callback::ResponseHeaders, e)?;
                    continue;
                }
            };
            drop(locked);
            let paused =
                sent.is_none() && self.plugin_stays_paused(action, StreamType::HttpResponse);
            if origin == ResponseSource::Upstream {
                self.start_callouts(position, paused);
            }
            match (sent, origin) {
                (Some(response), ResponseSource::Upstream) => {
                    return Ok(ResponsePassOutcome::Respond(position, Box::new(response)))
                }
                (Some(_), ResponseSource::Plugin) => warn!(
                    "wasm plugin {}: response dropped, another plugin already responded",
                    pool.name
                ),
                (None, _) if paused && self.waits_for_callout(position) => {
                    return Ok(ResponsePassOutcome::WaitsForCallout(position))
                }
                (None, _) if paused => {
                    self.skip_plugin_or_fail_request(position, response_pause_failure(origin))?;
                }
                (None, _) => {}
            }
        }
        Ok(ResponsePassOutcome::Finished)
    }
}

/// How a [WasmCtx::response_pass] ended.
pub(super) enum ResponsePassOutcome {
    /// Every plugin in the pass ran and none of them stopped it.
    Finished,
    /// The plugin at this position sent a response in place of the upstream response.
    Respond(usize, Box<PluginResponse>),
    /// The plugin at this position paused with a callout pending.
    WaitsForCallout(usize),
}

/// Frame the response as chunked if a plugin removed its `content-length` and a body follows.
pub(super) fn frame_if_length_removed(
    resp: &mut ResponseHeader,
    had_length: bool,
    end_of_stream: bool,
) -> Result<()> {
    if had_length && !end_of_stream {
        frame_as_chunked(resp)?;
    }
    Ok(())
}

/// Add `transfer-encoding: chunked` to a response that has no framing header.
///
/// Pingora does the same for an upstream response without a length before `response_filter`
/// runs, which keeps the downstream connection reusable.
fn frame_as_chunked(resp: &mut ResponseHeader) -> Result<()> {
    let framed =
        resp.headers.contains_key(CONTENT_LENGTH) || resp.headers.contains_key(TRANSFER_ENCODING);
    if framed || resp.status.is_informational() {
        return Ok(());
    }
    resp.set_version(Version::HTTP_11);
    resp.insert_header(TRANSFER_ENCODING, CHUNKED)
}

/// Return `true` for an informational (1xx) status other than 101, which plugins never see.
fn skips_response(status: StatusCode) -> bool {
    status.is_informational() && status != StatusCode::SWITCHING_PROTOCOLS
}

/// Return `true` if no body will follow the response header.
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
        start_request, Wat, GET, HEAD, PAUSE, POST, REMOVE_LENGTH, TEAPOT, TRAP,
    };
    use crate::{WasmRuntime, ERR_PLUGIN_FAILED};
    use pingora_error::ErrorType;

    fn response(status: u16, length: Option<&str>) -> ResponseHeader {
        let mut resp = ResponseHeader::build(status, None).unwrap();
        if let Some(length) = length {
            resp.insert_header(CONTENT_LENGTH, length).unwrap();
        }
        resp
    }

    #[tokio::test]
    async fn response_fails_after_guest_is_replaced() {
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
        let message = "guest in slot 0 lost before proxy_on_response_headers";
        assert!(err.to_string().contains(message), "{err}");
    }

    #[tokio::test]
    async fn pause_on_plugin_response_headers_fails() {
        let teapot = Wat {
            request_headers: TEAPOT,
            ..Wat::default()
        };
        let plugins = vec![
            body_plugin("first", Wat::response_headers(PAUSE)),
            body_plugin("last", teapot),
        ];
        let runtime = WasmRuntime::new(plugins).unwrap();
        let mut ctx = runtime.chain(&["first", "last"]).unwrap().new_ctx();
        let (mut session, _client) = session(GET).await;

        let err = ctx.request_filter(&mut session).await.unwrap_err();

        assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
        let message = "wasm plugin first: paused on plugin response headers";
        assert!(err.to_string().contains(message), "{err}");
    }

    #[tokio::test]
    async fn plugin_response_replaces_upstream_response() {
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
    async fn unwritten_plugin_response_is_a_failure() {
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
    async fn plugin_response_to_head_request_has_no_body() {
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
    async fn plugin_response_without_length_is_chunked() {
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
    async fn removed_length_is_chunked_only_when_body_follows() {
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
    async fn kept_length_adds_no_framing() {
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
    fn informational_responses_except_101_are_skipped() {
        let statuses = [100, 101, 103, 199, 200, 404];

        let skipped: Vec<_> = statuses
            .iter()
            .map(|s| skips_response(StatusCode::from_u16(*s).unwrap()))
            .collect();

        assert_eq!(skipped, [true, false, true, true, false, false]);
    }

    #[test]
    fn response_ends_for_bodiless_responses() {
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
