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

use super::WasmCtx;
use crate::{plugin_failure, plugin_unavailable};
use http::header::CONTENT_LENGTH;
use http::{Method, StatusCode};
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_error::Result;
use pingora_http::ResponseHeader;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::types::Action;

impl WasmCtx {
    /// Runs `on_response_headers` of each plugin that saw the request, in reverse order.
    ///
    /// Call it from `response_filter`.
    pub async fn response_filter<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        resp: &mut ResponseHeader,
    ) -> Result<()> {
        if session.subrequest_ctx.is_some() || skips_response(resp.status) {
            return Ok(());
        }
        self.chain.runtime.start_ticker()?;
        let end_of_stream = response_ends(&session.req_header().method, resp);
        self.response_pass(session, resp, (0..self.records.len()).rev(), end_of_stream)
    }

    /// Runs `on_response_headers` of the plugins at `positions` on `resp`.
    pub(super) fn response_pass<DS: DownstreamSession>(
        &mut self,
        session: &mut Session<DS>,
        resp: &mut ResponseHeader,
        positions: impl Iterator<Item = usize>,
        end_of_stream: bool,
    ) -> Result<()> {
        let runtime = self.chain.runtime.clone();
        for position in positions {
            let Some(record) = self.records[position] else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            let Some(mut guard) = pool.lock(record.slot, record.guest) else {
                return Err(plugin_unavailable(
                    &pool.name,
                    "lost the guest of this request",
                ));
            };
            let Some(loaded) = guard.as_mut() else {
                return Err(plugin_unavailable(&pool.name, "has no guest"));
            };
            self.request_in(session.req_header_mut());
            self.response_in(resp);
            let count = self.response_count();
            let action = self.run(&mut loaded.guest, |scope| {
                scope.on_response_headers(record.context, count, end_of_stream)
            });
            self.response_out(resp);
            self.request_out(session.req_header_mut());
            self.stream().plugin_response = None;
            match action {
                Ok(Action::Pause) => {
                    return Err(plugin_unavailable(&pool.name, "paused a response"))
                }
                Ok(_) => {}
                Err(e) => {
                    pool.check(record.slot, guard, &e);
                    return Err(plugin_failure(
                        &pool.name,
                        "failed in on_response_headers",
                        e,
                    ));
                }
            }
        }
        Ok(())
    }
}

fn skips_response(status: StatusCode) -> bool {
    status.is_informational() && status != StatusCode::SWITCHING_PROTOCOLS
}

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
    use crate::test_support::{add_request_header, one_plugin, session, GET};
    use crate::ERR_PLUGIN_FAILED;

    fn response(status: u16, length: Option<&str>) -> ResponseHeader {
        let mut resp = ResponseHeader::build(status, None).unwrap();
        if let Some(length) = length {
            resp.insert_header(CONTENT_LENGTH, length).unwrap();
        }
        resp
    }

    #[tokio::test]
    async fn a_response_on_a_replaced_guest_answers_503() {
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
