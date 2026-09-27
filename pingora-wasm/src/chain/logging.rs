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
use crate::runtime::pool::{GuestPool, SlotGuard};
use log::{error, warn};
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::{CallScope, ContextId, GuestError, StreamState};

impl WasmCtx {
    /// Ends the request in each plugin that saw it, in reverse chain order, with
    /// `proxy_on_done`, `proxy_on_log`, and `proxy_on_delete`.
    ///
    /// Call it from `logging`, for every request that created this `WasmCtx`. A plugin failure
    /// here is logged and not returned, because the response is already sent.
    pub async fn logging<DS: DownstreamSession>(&mut self, session: &mut Session<DS>) {
        let runtime = self.chain.runtime.clone();
        let mut response = session.response_written().cloned();
        for position in (0..self.records.len()).rev() {
            let Some(record) = self.records[position].take() else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            let Some(mut guard) = pool.lock(record.slot, record.guest) else {
                continue;
            };
            let Some(loaded) = guard.as_mut() else {
                continue;
            };
            self.request_in(session.req_header_mut());
            if let Some(header) = response.as_mut() {
                self.response_in(header);
            }
            let result = self.run(&mut loaded.guest, |scope| {
                finish(scope, record.context, true)
            });
            if let Some(header) = response.as_mut() {
                self.response_out(header);
            }
            self.request_out(session.req_header_mut());
            finished(pool, record.slot, guard, result);
        }
    }
}

/// Ends a context: `on_done`, then `on_log` when `log` is set, then `on_delete`.
///
/// Answers `false` when the guest holds the context.
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

pub(super) fn finished(
    pool: &GuestPool,
    slot: usize,
    guard: SlotGuard<'_>,
    result: Result<bool, GuestError>,
) {
    match result {
        Ok(true) => pool.deleted(slot),
        Ok(false) => {
            warn!(
                "wasm plugin {} holds a context after the request ended",
                pool.name
            );
            pool.held(slot);
        }
        Err(e) => {
            error!("wasm plugin {} failed to end a context: {e}", pool.name);
            pool.check(slot, guard, &e);
        }
    }
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
