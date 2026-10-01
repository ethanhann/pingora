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
use super::WasmCtx;
use crate::properties::built_in::LoggingFacts;
use log::{debug, error, warn};
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::{CallScope, ContextId, GuestError, StreamState};

impl WasmCtx {
    /// End the request in each plugin that saw it, in reverse chain order.
    ///
    /// Call it from `logging`, for every request that created this `WasmCtx`. Each plugin runs
    /// `proxy_on_done`, `proxy_on_log`, and `proxy_on_delete`, and can read the request headers
    /// and the response headers. A plugin failure here is logged and not returned, because the
    /// response is already sent.
    ///
    /// A plugin whose `proxy_on_done` returns `false` holds its context, and it runs
    /// `proxy_on_log` later, after it calls `proxy_done`, with empty header maps.
    pub async fn logging<DS: DownstreamSession>(&mut self, session: &mut Session<DS>) {
        let runtime = self.chain.runtime.clone();
        self.callouts.clear();
        let mut response = session.response_written().cloned();
        let facts = &mut self.stream().facts;
        facts.request_body_bytes = session.body_bytes_read();
        let start = facts.start.map(|start| start.monotonic);
        facts.logging = Some(LoggingFacts {
            duration: start.map(|at| at.elapsed()),
            response_body_bytes: session.body_bytes_sent(),
        });
        let held = self.held.request_len();
        if held > 0 {
            debug!("the request ended while wasm plugins held {held} request body bytes");
        }
        let held = self.held.response_len();
        if held > 0 {
            warn!("the request ended while wasm plugins held {held} response body bytes");
        }
        for position in (0..self.records.len()).rev() {
            let Some(record) = self.records[position].take() else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            let Ok(mut locked) = LockedSlot::of_request(pool, &record) else {
                continue;
            };
            let Ok(loaded) = locked.loaded() else {
                continue;
            };
            self.request_in(session.req_header_mut());
            if let Some(header) = response.as_mut() {
                self.response_in(header);
            }
            let result = self.run_for_context(loaded, record.context, |scope| {
                finish(scope, record.context, true)
            });
            if let Some(header) = response.as_mut() {
                self.response_out(header);
            }
            self.request_out(session.req_header_mut());
            self.after_finish(position, locked, record.context, result, true);
        }
    }

    /// Record how the context of the plugin at `position` ended, and start the callouts that
    /// the plugin sent while it ended.
    ///
    /// A context that the guest holds gets the results of its callouts on the root callback
    /// thread, and it still owes `proxy_on_log` when `log_owed` is `true`.
    pub(super) fn after_finish(
        &mut self,
        position: usize,
        mut locked: LockedSlot<'_>,
        context: ContextId,
        result: Result<bool, GuestError>,
        log_owed: bool,
    ) {
        match result {
            Ok(true) => {
                locked.pool.deleted(locked.slot);
                self.start_callouts(position, false);
            }
            Ok(false) => {
                debug!(
                    "wasm plugin {} holds a context after the request ended, until it calls proxy_done",
                    locked.pool.name
                );
                locked.pool.deleted(locked.slot);
                if let Ok(loaded) = locked.loaded() {
                    loaded.hold_context(context, log_owed, self.callouts.take_accepted());
                }
            }
            Err(e) => error!("{}", locked.guest_failure("failed to end a context", e)),
        }
    }
}

/// End a context with `on_done`, then `on_log` when `log` is set, then `on_delete`.
///
/// Return `false` when the guest holds the context.
pub(super) fn finish<H: StreamState>(
    scope: &mut CallScope<'_, H>,
    context: ContextId,
    log: bool,
) -> Result<bool, GuestError> {
    if !scope.on_done(context)? {
        return Ok(false);
    }
    if log {
        scope.on_log(context)?;
    }
    scope.on_delete(context)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use crate::test_support::{add_request_header, one_plugin, session, wat_plugin, GET};

    #[tokio::test]
    async fn logging_skips_a_replaced_guest() {
        let (runtime, mut ctx) = one_plugin(add_request_header());
        let (mut other_session, _other_client) = session(GET).await;
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();
        runtime.inner.pools[0].replace_slot(0);
        let mut other = runtime.chain(&["a"]).unwrap().new_ctx();
        other.request_filter(&mut other_session).await.unwrap();
        let stale = ctx.records[0].unwrap().context;
        let live = other.records[0].unwrap().context;

        ctx.logging(&mut session).await;

        assert_eq!(stale, live);
        assert_eq!(runtime.open_contexts(), 1);
        let slot = runtime.inner.pools[0].lock_slot(0);
        assert!(slot.as_ref().unwrap().guest.context_state(live).is_some());
    }

    #[tokio::test]
    async fn logging_closes_a_context_after_a_guest_error() {
        let (runtime, mut ctx) = one_plugin(wat_plugin("bad-log", "i32.const 7"));
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap_err();
        let open = runtime.open_contexts();

        ctx.logging(&mut session).await;

        assert_eq!(open, 1);
        assert_eq!(runtime.open_contexts(), 0);
    }
}
