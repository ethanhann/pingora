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

use super::body::BodyDirection;
use super::slot::LockedSlot;
use super::WasmCtx;
use crate::observability::{PluginFailure, PluginFailureOutcome};
use crate::properties::built_in::LoggingFacts;
use log::{debug, error, warn};
use pingora_core::protocols::http::custom::server::Session as DownstreamSession;
use pingora_proxy::Session;
use proxy_wasm_host::abi::v0_2_1::{CallScope, Callback, ContextId, GuestError, StreamState};

impl WasmCtx {
    /// Run the end-of-request callbacks for each plugin that saw the request, in reverse chain order.
    ///
    /// Call this from your `logging` for every request that created a `WasmCtx`. Each plugin runs
    /// `proxy_on_done`, `proxy_on_log`, and `proxy_on_delete`, and can read the request headers
    /// and the headers of the response that was written. A plugin that failed open earlier in the
    /// request still runs them, as long as its guest is usable. A plugin whose guest was replaced
    /// or lost during the request does not, because its context was in that guest.
    ///
    /// A plugin failure here is logged rather than returned, under both fail policies, since the
    /// response has already been sent. It is also reported to the metric sink, unless that plugin
    /// already had a failure reported for this request.
    ///
    /// A plugin whose `proxy_on_done` returns `false` keeps its context. Its `proxy_on_log` runs
    /// later, once it has called `proxy_done`, and sees empty header maps.
    pub async fn logging<DS: DownstreamSession>(&mut self, session: &mut Session<DS>) {
        let runtime = self.chain.runtime.clone();
        self.callouts.clear();
        let mut response = session.response_written().cloned();
        let facts = &mut self.stream().request_facts;
        facts.request_body_bytes = session.body_bytes_read();
        let start = facts.start.map(|start| start.monotonic_time);
        facts.logging = Some(LoggingFacts {
            duration: start.map(|at| at.elapsed()),
            response_body_bytes: session.body_bytes_sent(),
        });
        self.log_bytes_still_held();
        for position in (0..self.records.len()).rev() {
            let Some(record) = self.records[position].take() else {
                continue;
            };
            let pool = &runtime.pools[self.chain.plugins[position]];
            let Some(mut locked) = LockedSlot::of_request(pool, &record) else {
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
                self.response_out(position, header);
            }
            self.request_out(position, session.req_header_mut());
            self.end_or_hold_context(position, locked, record.context, result, true);
        }
    }

    /// Log the body bytes each plugin is still holding at the end of the request.
    ///
    /// Request bytes are logged at debug level, since a client that disconnects mid-upload
    /// routinely leaves some held. Response bytes are logged as a warning because they were never
    /// sent downstream.
    pub(super) fn log_bytes_still_held(&self) {
        for position in 0..self.records.len() {
            let plugin = &self.pool_at(position).name;
            let held = self.held.len(BodyDirection::Request, position);
            if held > 0 {
                debug!(
                    "wasm plugin {plugin}: request ended with {held} request body bytes still held"
                );
            }
            let held = self.held.len(BodyDirection::Response, position);
            if held > 0 {
                warn!("wasm plugin {plugin}: request ended with {held} response body bytes still held, never sent downstream");
            }
        }
    }

    /// Finish the bookkeeping for the context of the plugin at `position` once [finish] has run.
    ///
    /// If the context was deleted, the callouts it sent while ending are started and their
    /// results are discarded. A context the guest kept is passed to the root callback thread
    /// along with those callouts, and `needs_on_log` records whether it still needs
    /// `proxy_on_log`. A guest failure is logged and reported to the metric sink, and the guest
    /// is replaced if the failure left it unusable.
    pub(super) fn end_or_hold_context(
        &mut self,
        position: usize,
        mut locked: LockedSlot<'_>,
        context: ContextId,
        result: Result<bool, (Callback, GuestError)>,
        needs_on_log: bool,
    ) {
        match result {
            Ok(true) => {
                locked.pool.deleted(locked.slot);
                self.start_callouts(position, false);
            }
            Ok(false) => {
                debug!(
                    "wasm plugin {}: context kept after the request ended, until the plugin calls proxy_done",
                    locked.pool.name
                );
                locked.pool.deleted(locked.slot);
                if let Ok(loaded) = locked.loaded() {
                    loaded.hold_context(context, needs_on_log, self.callouts.take_accepted());
                }
            }
            Err((callback, e)) => {
                error!("wasm plugin {}: {callback} failed: {e}", locked.pool.name);
                locked.replace_guest_if_unusable(&e);
                let (failure, outcome) = (PluginFailure::GuestError, PluginFailureOutcome::Failed);
                self.report_failure(position, failure, outcome, Some(callback));
            }
        }
    }
}

/// Run the end-of-request callbacks for `context`.
///
/// `proxy_on_done` runs first, then `proxy_on_log` if `log` is set, then `proxy_on_delete`.
/// Returns `false` without running the last two if `proxy_on_done` returned `false`, which means
/// the guest is keeping the context.
///
/// # Errors
///
/// Returns the failed callback with its error. The callbacks after it are not run.
pub(super) fn finish<H: StreamState>(
    scope: &mut CallScope<'_, H>,
    context: ContextId,
    log: bool,
) -> Result<bool, (Callback, GuestError)> {
    let failed_in = |callback| move |e| (callback, e);
    if !scope.on_done(context).map_err(failed_in(Callback::Done))? {
        return Ok(false);
    }
    if log {
        scope.on_log(context).map_err(failed_in(Callback::Log))?;
    }
    scope
        .on_delete(context)
        .map_err(failed_in(Callback::Delete))?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use crate::test_support::{add_request_header, one_plugin, session, wat_plugin, GET};

    #[tokio::test]
    async fn logging_skips_replaced_guest() {
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
    async fn logging_closes_context_after_guest_error() {
        let (runtime, mut ctx) = one_plugin(wat_plugin("bad-log", "i32.const 7"));
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap_err();
        let open = runtime.open_contexts();

        ctx.logging(&mut session).await;

        assert_eq!(open, 1);
        assert_eq!(runtime.open_contexts(), 0);
    }
}
