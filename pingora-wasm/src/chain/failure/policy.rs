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

//! Fail policy enforcement
//!
//! The per-request record the fail policy works from, and the `WasmCtx` functions that apply the
//! policy to a failure, skip a plugin, and report to the metric sink.

use super::FilterFailure;
use crate::chain::body::BodyDirection;
use crate::chain::ctx::PluginRecord;
use crate::chain::slot::LockedSlot;
use crate::chain::WasmCtx;
use crate::observability::{FailureOutcome, PluginFailure, PluginFailureReport};
use crate::runtime::pool::GuestPool;
use crate::runtime::FailPolicy;
use log::{debug, warn};
use pingora_error::{Error, Result};
use proxy_wasm_host::abi::v0_2_1::{Callback, GuestError};
use std::time::Instant;

/// Per-request record of plugin failures and body changes, kept as chain positions.
///
/// An empty `Vec` does not allocate, so these lists cost nothing for a request without a failure
/// or a body change.
#[derive(Debug, Default)]
pub(crate) struct FailPolicyRecord {
    /// Plugins that failed open, in the order they failed. They are skipped for the rest of the
    /// request.
    skipped: Vec<usize>,
    /// Plugins with a failure already reported to the metric sink.
    reported: Vec<usize>,
    /// Plugins that wrote to the request body or changed one of its length headers.
    changed_request_body: Vec<usize>,
    /// Plugins that wrote to the response body or changed one of its length headers.
    changed_response_body: Vec<usize>,
    /// Whether the response has ended. That is the case for a response without a body, once
    /// its last body chunk has run through the plugins, and once its trailers have arrived.
    pub(in crate::chain) response_body_ended: bool,
    /// Whether a plugin has sent its own response, after which no body is proxied.
    pub(in crate::chain) plugin_response_started: bool,
}

impl FailPolicyRecord {
    fn changed_body(&self, direction: BodyDirection) -> &Vec<usize> {
        match direction {
            BodyDirection::Request => &self.changed_request_body,
            BodyDirection::Response => &self.changed_response_body,
        }
    }
}

impl WasmCtx {
    /// Return the names of the plugins that failed open on this request.
    ///
    /// A plugin with [FailPolicy::Open] that fails is skipped for the rest of the request, and
    /// the request continues without it. Use this to apply a rule of your own when that happens,
    /// e.g. deny the request if a plugin that authorizes requests is in the list, add a header,
    /// or tag your access log. The names are in the order the plugins failed, and the iterator is
    /// empty if none did.
    ///
    /// A plugin can still be skipped in a later filter, after your `request_filter` has looked at
    /// this list. A rule that denies the request therefore has to check the list again in the
    /// later filters.
    pub fn skipped_plugins(&self) -> impl Iterator<Item = &str> {
        let skipped = self.failures.skipped.iter();
        skipped.map(|position| &*self.pool_at(*position).name)
    }

    /// Return `true` if the plugin at `position` has been skipped on this request.
    pub(in crate::chain) fn is_skipped(&self, position: usize) -> bool {
        self.failures.skipped.contains(&position)
    }

    /// Record that the plugin at `position` changed the body of `direction` or its length headers.
    pub(crate) fn record_body_change(&mut self, direction: BodyDirection, position: usize) {
        let changed = match direction {
            BodyDirection::Request => &mut self.failures.changed_request_body,
            BodyDirection::Response => &mut self.failures.changed_response_body,
        };
        if !changed.contains(&position) {
            changed.push(position);
        }
    }

    /// Apply the fail policy of the plugin at `position` to `failure`.
    ///
    /// Returns `Ok` once the plugin has been skipped, and the caller then continues the pass with
    /// the next plugin. Skipping drops the response the plugin recorded and the callouts it sent
    /// in the failing call, stops the wait for its pending callouts, and clears its continue
    /// requests. Its context is kept, so `logging` still runs its end-of-request callbacks if its
    /// guest is usable.
    ///
    /// # Errors
    ///
    /// Returns the failure's error if the plugin's fail policy is `Closed`. A plugin with `Open`
    /// also fails the request while a body it changed can still have bytes to come. The failure
    /// is then reported as [PluginFailure::BodyChanged], and the error message says so.
    pub(in crate::chain) fn skip_plugin_or_fail_request(
        &mut self,
        position: usize,
        mut failure: FilterFailure,
    ) -> Result<()> {
        let pool = self.pool_at(position);
        if pool.fail_policy != FailPolicy::Open {
            return Err(self.failed_request_error(position, failure));
        }
        if let Some(direction) = self.changed_body_with_bytes_to_come(position) {
            let (body, _) = direction.body_and_limit_names();
            failure.kind = PluginFailure::BodyChanged;
            failure.detail = format!(
                "{}, not skipped, {body} body or its length already changed",
                failure.detail
            );
            return Err(self.failed_request_error(position, failure));
        }
        self.report_failure(
            position,
            failure.kind,
            FailureOutcome::Skipped,
            failure.callback,
        );
        self.log_skipped_plugin(position, &failure);
        self.failures.skipped.push(position);
        self.callouts.take_accepted();
        self.callouts.forget_pending(position);
        let stream = self.stream();
        stream.plugin_response = None;
        stream.clear_continue_requests();
        Ok(())
    }

    /// Log that the plugin at `position` was skipped after `failure`.
    ///
    /// The warning is rate limited per plugin, and other skips are logged at debug level. It gives
    /// the number of requests that skipped the plugin since the last warning if more than one did.
    fn log_skipped_plugin(&self, position: usize, failure: &FilterFailure) {
        let pool = self.pool_at(position);
        let plugin = &pool.name;
        let detail = match &failure.cause {
            Some(cause) => format!("{}: {cause}", failure.detail),
            None => failure.detail.clone(),
        };
        match pool.skipped_plugin_warnings.count_event(Instant::now()) {
            Some(1) => warn!("wasm plugin {plugin}: {detail}, continuing without the plugin"),
            Some(skips) => warn!(
                "wasm plugin {plugin}: {detail}, continuing without the plugin, {skips} requests skipped it since the last warning"
            ),
            None => debug!("wasm plugin {plugin}: {detail}, continuing without the plugin"),
        }
    }

    /// Replace the guest in `locked` if `error` left it unusable, then apply the fail policy of
    /// the plugin at `position` to the failed `callback`.
    ///
    /// Returns `Ok` if the plugin was skipped.
    ///
    /// # Errors
    ///
    /// Returns the failure's error if the plugin was not skipped.
    pub(in crate::chain) fn guest_call_failed(
        &mut self,
        position: usize,
        locked: LockedSlot<'_>,
        callback: Callback,
        error: GuestError,
    ) -> Result<()> {
        locked.replace_guest_if_unusable(&error);
        let failure = FilterFailure::guest_error(callback, error);
        self.skip_plugin_or_fail_request(position, failure)
    }

    /// Lock the slot whose guest holds the context of the plugin at `position`.
    ///
    /// `callback` is the callback the filter is about to run, and is only used in the failure
    /// message. Returns `None` if the guest is gone and the plugin was skipped.
    ///
    /// # Errors
    ///
    /// Returns the failure's error if the guest is gone and the plugin was not skipped.
    pub(in crate::chain) fn lock_slot_or_skip_plugin<'a>(
        &mut self,
        pool: &'a GuestPool,
        position: usize,
        record: &PluginRecord,
        callback: Callback,
    ) -> Result<Option<LockedSlot<'a>>> {
        if let Some(locked) = LockedSlot::of_request(pool, record) {
            return Ok(Some(locked));
        }
        let failure = FilterFailure::guest_lost(record.slot, callback);
        self.skip_plugin_or_fail_request(position, failure)?;
        Ok(None)
    }

    /// Return the direction of a body the plugin at `position` changed that can still have bytes
    /// run through the plugin.
    ///
    /// Such a plugin cannot be skipped, because the rest of that body would go out without its
    /// changes. Returns `None` if the plugin can be skipped.
    ///
    /// A plugin changed a body if it wrote to the body bytes, or changed the value of
    /// `content-length` or `transfer-encoding` of that message. A body only counts if the plugin
    /// runs on it. The request body counts until its last chunk has run through the plugins, and
    /// the response body until the response has ended. Neither counts once a plugin has sent its
    /// own response, since no body is proxied after that.
    fn changed_body_with_bytes_to_come(&self, position: usize) -> Option<BodyDirection> {
        if self.failures.plugin_response_started {
            return None;
        }
        let phases = &self.pool_at(position).phases;
        let changed = |direction| self.failures.changed_body(direction).contains(&position);
        let request = BodyDirection::Request;
        if changed(request) && phases.request && self.request_body.is_unfinished() {
            return Some(request);
        }
        let response = BodyDirection::Response;
        if changed(response) && phases.response && !self.failures.response_body_ended {
            return Some(response);
        }
        None
    }

    /// Report `failure` as having failed the request and return its error.
    ///
    /// Called directly for the failures that fail the request under both policies.
    pub(in crate::chain) fn failed_request_error(
        &mut self,
        position: usize,
        failure: FilterFailure,
    ) -> Box<Error> {
        self.report_failure(
            position,
            failure.kind,
            FailureOutcome::Failed,
            failure.callback,
        );
        failure.into_error(&self.pool_at(position).name)
    }

    /// Send a failure report for the plugin at `position` to the metric sink.
    ///
    /// Later failures of the same plugin on this request are not reported.
    pub(in crate::chain) fn report_failure(
        &mut self,
        position: usize,
        failure: PluginFailure,
        outcome: FailureOutcome,
        callback: Option<Callback>,
    ) {
        if self.failures.reported.contains(&position) {
            return;
        }
        self.failures.reported.push(position);
        let report = PluginFailureReport {
            plugin_name: &self.pool_at(position).name,
            failure,
            outcome,
            callback: callback.map(Callback::export_name),
        };
        self.chain.runtime.metric_sink.plugin_failed(&report);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::callouts::{
        authz_services, callout_ctx_with_services, FixedSender, CALL_AND_PAUSE,
        CALL_TWICE_AND_PAUSE, CALL_WITHOUT_PAUSE, STAY_PAUSED,
    };
    use crate::test_support::phases::{
        cancel_a_wait_in, plugin_with_callback_in, run_phase, run_request_headers, Phase,
        PhaseInputs,
    };
    use crate::test_support::{
        body_chunk, body_plugin, crate_log_lines_with, record_crate_logs, session, RecordedFailure,
        RecordedFailures, RecordedGuestLogs, Wat, CONTINUE, GET, HEAD, HOLD, HOLD_THEN_TRAP,
        MARK_A_REQUEST, MARK_A_RESPONSE, MARK_B_REQUEST, MARK_B_RESPONSE, PAUSE, POST,
        REMOVE_LENGTH, SET_TRAILER, TEAPOT, TRAP,
    };
    use crate::ERR_PLUGIN_FAILED;
    use crate::{RequestOutcome, WasmPluginConf, WasmRuntime, WasmServices};
    use futures::poll;
    use http::header::CONTENT_LENGTH;
    use pingora_proxy::Session;
    use proxy_wasm_host::abi::v0_2_1::types::StreamType;
    use std::pin::pin;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    const MARK_ASKED: &str = "(call $mark_asked) i32.const 0";
    /// Body for `proxy_on_request_headers` that stores its context id at address 608, sends a
    /// callout, and pauses.
    const STORE_CONTEXT_AND_CALL: &str =
        "(i32.store (i32.const 608) (local.get 0)) (call $call_authz_and_pause)";
    /// Body for `proxy_on_http_call_response` that sends another callout from the context whose
    /// id is stored at address 608.
    const CALL_AGAIN: &str = "(drop (call $set_effective_context (i32.load (i32.const 608))))
        (drop (call $call_authz_and_pause))";
    const RETURN_INVALID_ACTION: &str = "i32.const 7";
    const LOG_TICK: &str = "(call $log_tick)";
    /// Body callbacks that prepend `a` to every chunk except the last, where they trap.
    const MARK_REQUEST_THEN_TRAP: &str =
        "(if (result i32) (local.get 2) (then unreachable) (else (call $mark_a (i32.const 0))))";
    const MARK_RESPONSE_THEN_TRAP: &str =
        "(if (result i32) (local.get 2) (then unreachable) (else (call $mark_a (i32.const 1))))";
    const REMOVE_REQUEST_LENGTH: &str =
        "(drop (call $remove (i32.const 0) (i32.const 96) (i32.const 14))) i32.const 0";
    const REMOVE_RESPONSE_LENGTH_THEN_TRAP: &str =
        "(drop (call $remove (i32.const 2) (i32.const 96) (i32.const 14))) unreachable";

    const EVERY_PHASE: [Phase; 5] = [
        Phase::RequestHeaders,
        Phase::RequestBody,
        Phase::ResponseHeaders,
        Phase::ResponseBody,
        Phase::ResponseTrailers,
    ];

    fn with_open_policy(mut conf: WasmPluginConf) -> WasmPluginConf {
        conf.fail_policy = FailPolicy::Open;
        conf
    }

    fn callback_of(phase: Phase) -> Callback {
        match phase {
            Phase::RequestHeaders => Callback::RequestHeaders,
            Phase::RequestBody => Callback::RequestBody,
            Phase::ResponseHeaders => Callback::ResponseHeaders,
            Phase::ResponseBody => Callback::ResponseBody,
            Phase::ResponseTrailers => Callback::ResponseTrailers,
        }
    }

    /// Return a chain of `optional` and a plugin named `next` that runs after it in `phase`
    /// and leaves a mark there.
    fn chain_with_next(optional: WasmPluginConf, phase: Phase) -> Vec<WasmPluginConf> {
        let (mark, runs_in_reverse) = match phase {
            Phase::RequestHeaders => (MARK_ASKED, false),
            Phase::RequestBody => (MARK_B_REQUEST, false),
            Phase::ResponseHeaders => (REMOVE_LENGTH, true),
            Phase::ResponseBody => (MARK_B_RESPONSE, true),
            Phase::ResponseTrailers => (SET_TRAILER, true),
        };
        let next = plugin_with_callback_in("next", phase, mark, "");
        if runs_in_reverse {
            vec![next, optional]
        } else {
            vec![optional, next]
        }
    }

    /// Return the chain position of the plugin `optional` in a chain of [chain_with_next].
    fn position_of_optional(phase: Phase) -> usize {
        match phase {
            Phase::RequestHeaders | Phase::RequestBody => 0,
            _ => 1,
        }
    }

    /// Return `true` if the plugin `next` of [chain_with_next] left its mark in `phase`.
    fn next_plugin_ran(phase: Phase, session: &Session, inputs: &PhaseInputs) -> bool {
        match phase {
            Phase::RequestHeaders => session.req_header().headers.contains_key("x-asked"),
            Phase::RequestBody | Phase::ResponseBody => inputs.body == body_chunk("bx"),
            Phase::ResponseHeaders => !inputs.response.headers.contains_key(CONTENT_LENGTH),
            Phase::ResponseTrailers => inputs.trailers.contains_key("x-trailer"),
        }
    }

    /// Return services with a sink that records failure reports, and that sink.
    fn services_with_reports() -> (WasmServices, Arc<RecordedFailures>) {
        let reports = Arc::new(RecordedFailures::default());
        let services = WasmServices {
            metric_sink: reports.clone(),
            ..authz_services()
        };
        (services, reports)
    }

    /// Build a context for a chain of `plugins`, with a sink that records failure reports and a
    /// sender that responds to every callout.
    fn ctx_with_reports(
        plugins: Vec<WasmPluginConf>,
    ) -> (
        WasmRuntime,
        WasmCtx,
        Arc<RecordedFailures>,
        Arc<FixedSender>,
    ) {
        let sender = FixedSender::responds("allowed");
        let (runtime, ctx, reports) = ctx_with_reports_and_sender(plugins, sender.clone());
        (runtime, ctx, reports, sender)
    }

    fn ctx_with_reports_and_sender(
        plugins: Vec<WasmPluginConf>,
        sender: Arc<FixedSender>,
    ) -> (WasmRuntime, WasmCtx, Arc<RecordedFailures>) {
        let (services, reports) = services_with_reports();
        let (runtime, ctx) = callout_ctx_with_services(plugins, sender, services);
        (runtime, ctx, reports)
    }

    /// Run the phases ahead of `phase`, then `phase` itself, and return its result.
    async fn run_up_to(
        ctx: &mut WasmCtx,
        session: &mut Session,
        phase: Phase,
        inputs: &mut PhaseInputs,
    ) -> Result<()> {
        if phase != Phase::RequestHeaders {
            run_request_headers(ctx, session).await;
        }
        run_phase(ctx, session, phase, inputs).await
    }

    fn expected_report(
        failure: PluginFailure,
        outcome: FailureOutcome,
        callback: Option<Callback>,
    ) -> RecordedFailure {
        let callback = callback.map(|callback| callback.export_name().to_string());
        ("optional".to_string(), failure, outcome, callback)
    }

    fn skipped_plugin_names(ctx: &WasmCtx) -> Vec<&str> {
        ctx.skipped_plugins().collect()
    }

    #[tokio::test]
    async fn open_plugin_is_skipped_after_trap_or_pause() {
        let cases = EVERY_PHASE.into_iter().flat_map(|phase| {
            [
                (phase, TRAP, PluginFailure::GuestError),
                (phase, PAUSE, PluginFailure::Paused),
            ]
        });

        for (phase, callback, failure) in cases {
            let optional = plugin_with_callback_in("optional", phase, callback, "");
            let plugins = chain_with_next(with_open_policy(optional), phase);
            let (_runtime, mut ctx, reports, _) = ctx_with_reports(plugins);
            let (mut session, _client) = session(POST).await;
            let mut inputs = PhaseInputs::new();

            let result = run_up_to(&mut ctx, &mut session, phase, &mut inputs).await;

            assert!(result.is_ok(), "{phase:?} {callback}: {result:?}");
            assert_eq!(
                skipped_plugin_names(&ctx),
                ["optional"],
                "{phase:?} {callback}"
            );
            assert!(
                next_plugin_ran(phase, &session, &inputs),
                "{phase:?} {callback}"
            );
            let want = expected_report(failure, FailureOutcome::Skipped, Some(callback_of(phase)));
            assert_eq!(reports.failures(), [want]);
        }
    }

    #[tokio::test]
    async fn open_plugin_with_replaced_guest_is_skipped() {
        for phase in EVERY_PHASE.into_iter().skip(1) {
            let optional = plugin_with_callback_in("optional", phase, CONTINUE, "");
            let plugins = chain_with_next(with_open_policy(optional), phase);
            let (runtime, mut ctx, reports, _) = ctx_with_reports(plugins);
            let (mut session, _client) = session(POST).await;
            let mut inputs = PhaseInputs::new();
            run_request_headers(&mut ctx, &mut session).await;
            runtime.inner.pools[position_of_optional(phase)].replace_slot(0);

            let result = run_phase(&mut ctx, &mut session, phase, &mut inputs).await;

            assert!(result.is_ok(), "{phase:?}: {result:?}");
            assert_eq!(skipped_plugin_names(&ctx), ["optional"], "{phase:?}");
            assert!(next_plugin_ran(phase, &session, &inputs), "{phase:?}");
            let want = expected_report(PluginFailure::GuestLost, FailureOutcome::Skipped, None);
            assert_eq!(reports.failures(), [want]);
        }
    }

    #[tokio::test]
    async fn plugin_with_no_guest_fails_or_is_skipped_by_policy() {
        let cases = [
            (FailPolicy::Closed, FailureOutcome::Failed),
            (FailPolicy::Open, FailureOutcome::Skipped),
        ];

        for (policy, outcome) in cases {
            let phase = Phase::RequestHeaders;
            let mut optional = plugin_with_callback_in("optional", phase, CONTINUE, "");
            optional.fail_policy = policy;
            let (runtime, mut ctx, reports, _) = ctx_with_reports(chain_with_next(optional, phase));
            runtime.inner.pools[0].fail_slot(0);
            let (mut session, _client) = session(GET).await;

            let result = ctx.request_filter(&mut session).await;

            let open = policy == FailPolicy::Open;
            assert_eq!(result.is_ok(), open);
            assert_eq!(session.req_header().headers.contains_key("x-asked"), open);
            assert_eq!(skipped_plugin_names(&ctx).len(), usize::from(open));
            assert_eq!(
                reports.failures(),
                [expected_report(PluginFailure::Unavailable, outcome, None)]
            );
            if let Err(err) = result {
                assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
                let message = "wasm plugin optional: no slot has a guest";
                assert!(err.to_string().contains(message), "{err}");
            }
        }
    }

    #[tokio::test]
    async fn open_plugin_is_skipped_after_failure_in_callout_wait() {
        let cases = EVERY_PHASE.into_iter().flat_map(|phase| {
            [
                (
                    phase,
                    TRAP,
                    PluginFailure::GuestError,
                    Callback::HttpCallResponse,
                ),
                (
                    phase,
                    STAY_PAUSED,
                    PluginFailure::Paused,
                    callback_of(phase),
                ),
            ]
        });

        for (phase, delivery, failure, callback) in cases {
            let optional =
                plugin_with_callback_in("optional", phase, CALL_TWICE_AND_PAUSE, delivery);
            let plugins = chain_with_next(with_open_policy(optional), phase);
            let (_runtime, mut ctx, reports, _) = ctx_with_reports(plugins);
            let (mut session, _client) = session(POST).await;
            let mut inputs = PhaseInputs::new();

            let result = run_up_to(&mut ctx, &mut session, phase, &mut inputs).await;

            assert!(result.is_ok(), "{phase:?} {delivery}: {result:?}");
            assert_eq!(skipped_plugin_names(&ctx), ["optional"], "{phase:?}");
            assert!(
                next_plugin_ran(phase, &session, &inputs),
                "{phase:?} {delivery}"
            );
            assert!(!ctx.waits_for_callout(position_of_optional(phase)));
            let want = expected_report(failure, FailureOutcome::Skipped, Some(callback));
            assert_eq!(reports.failures(), [want]);
        }
    }

    #[tokio::test]
    async fn open_plugin_is_skipped_when_guest_is_lost_during_callout_wait() {
        for phase in EVERY_PHASE {
            let gate = Arc::new(Notify::new());
            let sender = FixedSender::responds_after("late", gate.clone());
            let optional = plugin_with_callback_in("optional", phase, CALL_AND_PAUSE, "");
            let plugins = chain_with_next(with_open_policy(optional), phase);
            let (runtime, mut ctx, reports) = ctx_with_reports_and_sender(plugins, sender);
            let (mut session, _client) = session(POST).await;
            let mut inputs = PhaseInputs::new();
            let pool = &runtime.inner.pools[position_of_optional(phase)];

            let result = {
                let mut waits = pin!(run_up_to(&mut ctx, &mut session, phase, &mut inputs));
                assert!(poll!(waits.as_mut()).is_pending());
                pool.replace_slot(0);
                gate.notify_one();
                waits.await
            };

            assert!(result.is_ok(), "{phase:?}: {result:?}");
            assert_eq!(skipped_plugin_names(&ctx), ["optional"], "{phase:?}");
            assert!(next_plugin_ran(phase, &session, &inputs), "{phase:?}");
            let want = expected_report(PluginFailure::GuestLost, FailureOutcome::Skipped, None);
            assert_eq!(reports.failures(), [want]);
        }
    }

    #[tokio::test]
    async fn callout_wait_past_limit_fails_or_skips_by_policy() {
        let closed = (Phase::RequestHeaders, FailPolicy::Closed);
        let open = EVERY_PHASE.map(|phase| (phase, FailPolicy::Open));
        let cases = [closed].into_iter().chain(open);

        for (phase, policy) in cases {
            let mut optional = plugin_with_callback_in("optional", phase, CALL_AND_PAUSE, "");
            optional.fail_policy = policy;
            optional.callout_timeout_limit = Duration::from_millis(40);
            optional.callout_wait_limit = Duration::from_millis(50);
            let never_responds = FixedSender::responds_after("late", Arc::new(Notify::new()));
            let plugins = chain_with_next(optional, phase);
            let (_runtime, mut ctx, reports) = ctx_with_reports_and_sender(plugins, never_responds);
            let (mut session, _client) = session(POST).await;
            let mut inputs = PhaseInputs::new();

            let result = run_up_to(&mut ctx, &mut session, phase, &mut inputs).await;

            let open = policy == FailPolicy::Open;
            assert_eq!(result.is_ok(), open, "{phase:?}: {result:?}");
            assert_eq!(next_plugin_ran(phase, &session, &inputs), open, "{phase:?}");
            assert!(!ctx.waits_for_callout(position_of_optional(phase)));
            let outcome = match policy {
                FailPolicy::Open => FailureOutcome::Skipped,
                _ => FailureOutcome::Failed,
            };
            let callback = Some(callback_of(phase));
            let want = expected_report(PluginFailure::WaitLimit, outcome, callback);
            assert_eq!(reports.failures(), [want]);
            if let Err(err) = result {
                assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
                let message =
                    "wasm plugin optional: callout wait in proxy_on_request_headers exceeded \
                     callout_wait_limit 50ms";
                assert!(err.to_string().contains(message), "{err}");
            }
        }
    }

    #[tokio::test]
    async fn wait_limit_ends_chain_of_callouts_over_in_flight_limit() {
        let holds_the_permit = Wat {
            request_headers: CALL_WITHOUT_PAUSE,
            ..Wat::default()
        };
        let mut chained = plugin_with_callback_in(
            "optional",
            Phase::RequestHeaders,
            STORE_CONTEXT_AND_CALL,
            CALL_AGAIN,
        );
        chained.callout_timeout_limit = Duration::from_millis(40);
        chained.callout_wait_limit = Duration::from_millis(50);
        let (mut services, reports) = services_with_reports();
        services.max_callouts_in_flight = 1;
        let never_responds = FixedSender::responds_after("late", Arc::new(Notify::new()));
        let plugins = vec![body_plugin("holder", holds_the_permit), chained];
        let (_runtime, mut ctx) = callout_ctx_with_services(plugins, never_responds, services);
        let (mut session, _client) = session(GET).await;

        let result = ctx.request_filter(&mut session).await;

        assert_eq!(result.unwrap_err().etype(), &ERR_PLUGIN_FAILED);
        let callback = Some(Callback::RequestHeaders);
        let want = expected_report(PluginFailure::WaitLimit, FailureOutcome::Failed, callback);
        assert_eq!(reports.failures(), [want]);
    }

    #[tokio::test]
    async fn open_plugin_fails_request_after_body_or_length_change() {
        let removes_length_then_traps = Wat {
            request_headers: REMOVE_REQUEST_LENGTH,
            request_body: Some(HOLD_THEN_TRAP),
            ..Wat::default()
        };
        let cases = [
            (
                Wat::request_body(MARK_REQUEST_THEN_TRAP),
                BodyDirection::Request,
            ),
            (
                Wat::response_body(MARK_RESPONSE_THEN_TRAP),
                BodyDirection::Response,
            ),
            (removes_length_then_traps, BodyDirection::Request),
        ];

        for (wat, direction) in cases {
            let plugins = vec![with_open_policy(body_plugin("optional", wat))];
            let (_runtime, mut ctx, reports, _) = ctx_with_reports(plugins);
            let (mut session, _client) = session(POST).await;
            run_request_headers(&mut ctx, &mut session).await;
            let mut chunks = [(body_chunk("x"), false), (body_chunk("y"), true)];
            let mut results = Vec::new();

            for (body, end_of_stream) in &mut chunks {
                results.push(match direction {
                    BodyDirection::Request => {
                        ctx.request_body_filter(&mut session, body, *end_of_stream)
                            .await
                    }
                    BodyDirection::Response => {
                        ctx.response_body_filter(&mut session, body, *end_of_stream)
                            .await
                    }
                });
            }

            let [first, last] = &results[..] else {
                panic!("expected two results");
            };
            assert!(first.is_ok(), "{first:?}");
            let err = last.as_ref().unwrap_err();
            assert_eq!(err.etype(), &ERR_PLUGIN_FAILED);
            let (body, _) = direction.body_and_limit_names();
            let message = format!("not skipped, {body} body or its length already changed");
            assert!(err.to_string().contains(&message), "{err}");
            assert!(skipped_plugin_names(&ctx).is_empty());
            let callback = Some(direction.callback());
            let want =
                expected_report(PluginFailure::BodyChanged, FailureOutcome::Failed, callback);
            assert_eq!(reports.failures(), [want]);
        }
    }

    #[tokio::test]
    async fn open_plugin_fails_request_while_changed_body_is_open() {
        let wat = Wat {
            request_body: Some(MARK_REQUEST_THEN_TRAP),
            response_headers: Some(TRAP),
            ..Wat::default()
        };
        let plugins = vec![with_open_policy(body_plugin("optional", wat))];
        let (_runtime, mut ctx, reports, _) = ctx_with_reports(plugins);
        let (mut session, _client) = session(POST).await;
        let mut inputs = PhaseInputs::new();
        run_request_headers(&mut ctx, &mut session).await;
        ctx.request_body_filter(&mut session, &mut body_chunk("x"), false)
            .await
            .unwrap();

        let result = run_phase(&mut ctx, &mut session, Phase::ResponseHeaders, &mut inputs).await;

        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("not skipped, request body"),
            "{err}"
        );
        assert!(skipped_plugin_names(&ctx).is_empty());
        let callback = Some(Callback::ResponseHeaders);
        let want = expected_report(PluginFailure::BodyChanged, FailureOutcome::Failed, callback);
        assert_eq!(reports.failures(), [want]);
    }

    /// One step of a request in [open_plugin_is_skipped_once_changed_body_has_ended].
    enum Step {
        RequestBody(&'static str, bool),
        ResponseBody(&'static str, bool),
        ReplaceGuest,
        Run(Phase),
    }

    async fn run_step(
        step: &Step,
        runtime: &WasmRuntime,
        ctx: &mut WasmCtx,
        session: &mut Session,
        inputs: &mut PhaseInputs,
    ) -> Result<()> {
        match step {
            Step::RequestBody(chunk, end) => {
                let mut body = body_chunk(chunk);
                ctx.request_body_filter(session, &mut body, *end).await
            }
            Step::ResponseBody(chunk, end) => {
                let mut body = body_chunk(chunk);
                ctx.response_body_filter(session, &mut body, *end).await
            }
            Step::ReplaceGuest => {
                runtime.inner.pools[0].replace_slot(0);
                Ok(())
            }
            Step::Run(phase) => run_phase(ctx, session, *phase, inputs).await,
        }
    }

    #[tokio::test]
    async fn open_plugin_is_skipped_once_changed_body_has_ended() {
        let request_body_ended = Wat {
            request_body: Some(MARK_A_REQUEST),
            response_headers: Some(TRAP),
            ..Wat::default()
        };
        let traps_on_trailers = Wat {
            response_body: Some(MARK_A_RESPONSE),
            response_trailers: Some(TRAP),
            ..Wat::default()
        };
        let lost_before_trailers = Wat {
            response_body: Some(MARK_A_RESPONSE),
            response_trailers: Some(CONTINUE),
            ..Wat::default()
        };
        let response_without_body = Wat {
            response_headers: Some(REMOVE_RESPONSE_LENGTH_THEN_TRAP),
            response_body: Some(CONTINUE),
            ..Wat::default()
        };
        let does_not_run_on_request_body = Wat {
            request_headers: REMOVE_REQUEST_LENGTH,
            response_headers: Some(TRAP),
            ..Wat::default()
        };
        let headers = Step::Run(Phase::ResponseHeaders);
        let trailers = Step::Run(Phase::ResponseTrailers);
        let cases = [
            (
                request_body_ended,
                POST,
                vec![Step::RequestBody("x", true), headers],
            ),
            (
                traps_on_trailers,
                POST,
                vec![Step::ResponseBody("x", false), trailers],
            ),
            (
                lost_before_trailers,
                POST,
                vec![
                    Step::ResponseBody("x", false),
                    Step::ReplaceGuest,
                    Step::Run(Phase::ResponseTrailers),
                ],
            ),
            (
                response_without_body,
                HEAD,
                vec![Step::Run(Phase::ResponseHeaders)],
            ),
            (
                does_not_run_on_request_body,
                POST,
                vec![Step::Run(Phase::ResponseHeaders)],
            ),
        ];

        for (case, (wat, request, steps)) in cases.into_iter().enumerate() {
            let plugins = vec![with_open_policy(body_plugin("optional", wat))];
            let (runtime, mut ctx, _reports, _) = ctx_with_reports(plugins);
            let (mut session, _client) = session(request).await;
            let mut inputs = PhaseInputs::new();
            run_request_headers(&mut ctx, &mut session).await;
            let mut results = Vec::new();

            for step in &steps {
                results.push(run_step(step, &runtime, &mut ctx, &mut session, &mut inputs).await);
            }

            assert!(
                results.iter().all(Result::is_ok),
                "case {case}: {results:?}"
            );
            assert_eq!(skipped_plugin_names(&ctx), ["optional"], "case {case}");
        }
    }

    #[tokio::test]
    async fn plugin_response_is_returned_when_open_plugin_changed_length_and_fails_on_it() {
        let changes_length_then_fails = Wat {
            request_headers: REMOVE_REQUEST_LENGTH,
            request_body: Some(CONTINUE),
            response_headers: Some(TRAP),
            ..Wat::default()
        };
        let teapot = Wat {
            request_headers: TEAPOT,
            ..Wat::default()
        };
        let plugins = vec![
            with_open_policy(body_plugin("optional", changes_length_then_fails)),
            body_plugin("last", teapot),
        ];
        let (_runtime, mut ctx, _reports, _) = ctx_with_reports(plugins);
        let (mut session, _client) = session(POST).await;

        let outcome = ctx.request_filter(&mut session).await;

        assert!(
            matches!(outcome, Ok(RequestOutcome::Respond(..))),
            "{outcome:?}"
        );
        assert_eq!(skipped_plugin_names(&ctx), ["optional"]);
    }

    #[tokio::test]
    async fn open_plugin_fails_request_for_late_response() {
        let plugins = vec![with_open_policy(body_plugin(
            "optional",
            Wat::response_body(TEAPOT),
        ))];
        let (_runtime, mut ctx, reports, _) = ctx_with_reports(plugins);
        let (mut session, _client) = session(POST).await;
        let mut inputs = PhaseInputs::new();
        run_request_headers(&mut ctx, &mut session).await;

        let result = run_phase(&mut ctx, &mut session, Phase::ResponseBody, &mut inputs).await;

        assert_eq!(result.unwrap_err().etype(), &ERR_PLUGIN_FAILED);
        assert!(skipped_plugin_names(&ctx).is_empty());
        let callback = Some(Callback::ResponseBody);
        let want = expected_report(
            PluginFailure::LateResponse,
            FailureOutcome::Failed,
            callback,
        );
        assert_eq!(reports.failures(), [want]);
    }

    #[tokio::test]
    async fn open_plugin_fails_request_after_cancelled_wait() {
        let phase = Phase::RequestBody;
        let optional = with_open_policy(plugin_with_callback_in(
            "optional",
            phase,
            CALL_AND_PAUSE,
            "",
        ));
        let sender = FixedSender::responds_after("late", Arc::new(Notify::new()));
        let (_runtime, mut ctx, reports) = ctx_with_reports_and_sender(vec![optional], sender);
        let (mut session, _client) = session(POST).await;
        cancel_a_wait_in(phase, &mut ctx, &mut session).await;
        let mut inputs = PhaseInputs::new();

        let result = run_phase(&mut ctx, &mut session, Phase::ResponseHeaders, &mut inputs).await;

        assert_eq!(result.unwrap_err().etype(), &ERR_PLUGIN_FAILED);
        assert!(skipped_plugin_names(&ctx).is_empty());
        let want = expected_report(PluginFailure::CancelledWait, FailureOutcome::Failed, None);
        assert_eq!(reports.failures(), [want]);
    }

    #[tokio::test]
    async fn skipped_plugin_keeps_changes_only() {
        let wat = Wat {
            data_segments: r#"(data (i32.const 700) "plugin\00note") (data (i32.const 720) "kept")"#,
            request_headers: "(call $mark_asked)
                (drop (call $set_property
                    (i32.const 700) (i32.const 11) (i32.const 720) (i32.const 4)))
                (call $continue (i32.const 0))
                (drop (call $call_authz_and_pause))
                (drop (call $respond (i32.const 418)))
                unreachable",
            ..Wat::default()
        };
        let plugins = vec![with_open_policy(body_plugin("optional", wat))];
        let (_runtime, mut ctx, _reports, sender) = ctx_with_reports(plugins);
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await;

        assert!(
            matches!(outcome, Ok(RequestOutcome::Continue)),
            "{outcome:?}"
        );
        assert_eq!(session.req_header().headers["x-asked"], "yes");
        assert_eq!(ctx.guest_property(&["plugin", "note"]), Some(&b"kept"[..]));
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert_eq!(sender.sent_count(), 0);
        assert!(!ctx.plugin_responded());
        let stream = ctx.stream();
        assert!(stream.plugin_response.is_none());
        assert!(!stream.continue_requested(StreamType::HttpRequest));
    }

    #[tokio::test]
    async fn held_bytes_of_skipped_plugin_move_to_next_plugin() {
        let plugins = vec![
            with_open_policy(body_plugin("optional", Wat::request_body(HOLD_THEN_TRAP))),
            body_plugin("next", Wat::request_body(MARK_B_REQUEST)),
        ];
        let (_runtime, mut ctx, _reports, _) = ctx_with_reports(plugins);
        let (mut session, _client) = session(POST).await;
        run_request_headers(&mut ctx, &mut session).await;
        let mut first = body_chunk("ab");
        let mut last = body_chunk("cd");
        ctx.request_body_filter(&mut session, &mut first, false)
            .await
            .unwrap();

        let result = ctx.request_body_filter(&mut session, &mut last, true).await;

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(first, body_chunk(""));
        assert_eq!(last, body_chunk("babcd"));
    }

    #[tokio::test]
    async fn held_bytes_of_skipped_plugin_precede_next_chunk() {
        let wat = Wat {
            request_body: Some(HOLD),
            response_headers: Some(TRAP),
            ..Wat::default()
        };
        let plugins = vec![with_open_policy(body_plugin("optional", wat))];
        let (_runtime, mut ctx, _reports, _) = ctx_with_reports(plugins);
        let (mut session, _client) = session(POST).await;
        let mut inputs = PhaseInputs::new();
        run_request_headers(&mut ctx, &mut session).await;
        ctx.request_body_filter(&mut session, &mut body_chunk("ab"), false)
            .await
            .unwrap();
        run_phase(&mut ctx, &mut session, Phase::ResponseHeaders, &mut inputs)
            .await
            .unwrap();
        let mut last = body_chunk("cd");

        let result = ctx
            .request_body_filter(&mut session, &mut last, false)
            .await;

        assert!(result.is_ok(), "{result:?}");
        assert_eq!(skipped_plugin_names(&ctx), ["optional"]);
        assert_eq!(last, body_chunk("abcd"));
    }

    #[tokio::test]
    async fn trailer_filter_releases_held_bytes_of_skipped_plugin() {
        let wat = Wat {
            response_body: Some(HOLD),
            response_trailers: Some(TRAP),
            ..Wat::default()
        };
        let plugins = vec![with_open_policy(body_plugin("optional", wat))];
        let (_runtime, mut ctx, _reports, _) = ctx_with_reports(plugins);
        let (mut session, _client) = session(POST).await;
        run_request_headers(&mut ctx, &mut session).await;
        ctx.response_body_filter(&mut session, &mut body_chunk("held"), false)
            .await
            .unwrap();

        let released = ctx
            .response_trailer_filter(&mut session, &mut http::HeaderMap::new())
            .await;

        assert_eq!(released.unwrap(), body_chunk("held"));
        assert_eq!(skipped_plugin_names(&ctx), ["optional"]);
    }

    #[tokio::test]
    async fn logging_reports_held_response_bytes_of_skipped_plugin() {
        record_crate_logs();
        let wat = Wat {
            request_body: Some(TRAP),
            response_body: Some(HOLD),
            ..Wat::default()
        };
        let conf = with_open_policy(body_plugin("held-and-skipped", wat));
        let (_runtime, mut ctx, _reports, _) = ctx_with_reports(vec![conf]);
        let (mut session, _client) = session(POST).await;
        run_request_headers(&mut ctx, &mut session).await;
        ctx.response_body_filter(&mut session, &mut body_chunk("held"), false)
            .await
            .unwrap();
        ctx.request_body_filter(&mut session, &mut body_chunk("x"), true)
            .await
            .unwrap();

        ctx.logging(&mut session).await;

        assert_eq!(skipped_plugin_names(&ctx), ["held-and-skipped"]);
        let lines = crate_log_lines_with("wasm plugin held-and-skipped: request ended");
        let want = "wasm plugin held-and-skipped: request ended with 4 response body bytes \
                    still held, never sent downstream";
        assert_eq!(lines, [want]);
    }

    #[tokio::test]
    async fn dropping_ctx_without_logging_reports_held_response_bytes() {
        record_crate_logs();
        let conf = body_plugin("held-at-drop", Wat::response_body(HOLD));
        let (_runtime, mut ctx, _reports, _) = ctx_with_reports(vec![conf]);
        let (mut session, _client) = session(POST).await;
        run_request_headers(&mut ctx, &mut session).await;
        ctx.response_body_filter(&mut session, &mut body_chunk("held"), false)
            .await
            .unwrap();

        drop(ctx);

        let lines = crate_log_lines_with("wasm plugin held-at-drop: request ended");
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].contains("4 response body bytes still held"),
            "{lines:?}"
        );
    }

    #[tokio::test]
    async fn skipped_plugin_gets_proxy_on_log_only_with_usable_guest() {
        let cases = [(RETURN_INVALID_ACTION, 1), (TRAP, 0)];

        for (request_headers, log_calls) in cases {
            let wat = Wat {
                request_headers,
                log: Some(LOG_TICK),
                ..Wat::default()
            };
            let logs = Arc::new(RecordedGuestLogs::default());
            let services = WasmServices {
                log_sink: logs.clone(),
                ..authz_services()
            };
            let plugins = vec![with_open_policy(body_plugin("optional", wat))];
            let sender = FixedSender::responds("unused");
            let (runtime, mut ctx) = callout_ctx_with_services(plugins, sender, services);
            let (mut session, _client) = session(GET).await;
            ctx.request_filter(&mut session).await.unwrap();

            ctx.logging(&mut session).await;

            assert_eq!(logs.0.lock().len(), log_calls, "{request_headers}");
            assert_eq!(runtime.open_contexts(), 0);
        }
    }

    #[tokio::test]
    async fn plugin_response_is_returned_when_open_plugin_fails_on_it() {
        let teapot = Wat {
            request_headers: TEAPOT,
            ..Wat::default()
        };
        let plugins = vec![
            with_open_policy(body_plugin("optional", Wat::response_headers(TRAP))),
            body_plugin("last", teapot),
        ];
        let (_runtime, mut ctx, _reports, _) = ctx_with_reports(plugins);
        let (mut session, _client) = session(GET).await;

        let outcome = ctx.request_filter(&mut session).await.unwrap();

        let RequestOutcome::Respond(header, _) = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(header.status, 418);
        assert_eq!(skipped_plugin_names(&ctx), ["optional"]);
    }

    #[tokio::test]
    async fn plugin_failure_is_reported_once_for_each_request() {
        let cases = [
            (Some(RETURN_INVALID_ACTION), Callback::ResponseTrailers),
            (None, Callback::Log),
        ];

        for (response_trailers, callback) in cases {
            let wat = Wat {
                response_trailers,
                log: Some(TRAP),
                ..Wat::default()
            };
            let (_runtime, mut ctx, reports, _) =
                ctx_with_reports(vec![body_plugin("optional", wat)]);
            let (mut session, _client) = session(POST).await;
            let mut inputs = PhaseInputs::new();
            run_request_headers(&mut ctx, &mut session).await;
            let trailer_result =
                run_phase(&mut ctx, &mut session, Phase::ResponseTrailers, &mut inputs).await;

            ctx.logging(&mut session).await;

            assert_eq!(trailer_result.is_err(), response_trailers.is_some());
            let want = expected_report(
                PluginFailure::GuestError,
                FailureOutcome::Failed,
                Some(callback),
            );
            assert_eq!(reports.failures(), [want]);
        }
    }
}
