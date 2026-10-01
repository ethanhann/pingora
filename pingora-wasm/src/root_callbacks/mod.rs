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

//! The thread that calls the guests when no request runs: ticks, queue wakes, the results of
//! the callouts that no request waits for, and the end of the contexts that guests held.
//!
//! The thread has its own single-thread tokio runtime, so a tick never blocks a thread of a
//! Pingora service. After each guest call, the thread that made the call sends an event on a
//! single channel. The root callback thread is the only reader, so it keeps its state without
//! locks.

mod callback_loop;
mod queue_registrations;
mod root_callouts;
mod root_stream;
mod tick_schedule;
mod work;

pub(crate) use root_stream::{RootCallbackPluginState, RootStream};

use crate::runtime::pool::events::{RootCallbackEvent, RootCallbackSender};
use crate::runtime::RuntimeInner;
use crate::ERR_PLUGIN_FAILED;
use callback_loop::RootCallbackLoop;
use parking_lot::Mutex;
use pingora_error::{OrErr, Result};
use std::sync::{mpsc, Arc, Weak};
use std::thread;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

// Linux truncates a thread name to 15 bytes
const THREAD_NAME: &str = "wasm-root-calls";

/// The event channel of the root callback thread, and the means to start the thread.
pub(crate) struct RootCallbackThread {
    sender: RootCallbackSender,
    receiver: Arc<Mutex<Option<UnboundedReceiver<RootCallbackEvent>>>>,
    #[cfg(test)]
    pub(crate) running: Arc<std::sync::atomic::AtomicBool>,
}

impl RootCallbackThread {
    pub(crate) fn new() -> Self {
        let (sender, receiver) = unbounded_channel();
        RootCallbackThread {
            sender,
            receiver: Arc::new(Mutex::new(Some(receiver))),
            #[cfg(test)]
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub(crate) fn sender(&self) -> RootCallbackSender {
        self.sender.clone()
    }

    /// Start the thread.
    ///
    /// The events that guests sent before the start wait in the channel. The thread ends when the
    /// `WasmRuntime` drops, because it holds every sender of the channel. When the thread cannot
    /// start, this returns an error, and the events stay in the channel for the next call.
    pub(crate) fn start(&self, runtime: Weak<RuntimeInner>) -> Result<()> {
        let receiver = self.receiver.clone();
        let (build_result_sender, build_result_receiver) = mpsc::channel();
        #[cfg(test)]
        let running = self.running.clone();
        thread::Builder::new()
            .name(THREAD_NAME.to_string())
            .spawn(move || {
                // The thread builds its own tokio runtime, because a tokio runtime that drops
                // inside an async context panics, and the calling thread can be inside the async
                // context of a request
                let tokio_runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(tokio_runtime) => tokio_runtime,
                    Err(e) => {
                        let _ = build_result_sender.send(Err(e));
                        return;
                    }
                };
                let Some(events) = receiver.lock().take() else {
                    let _ = build_result_sender.send(Ok(()));
                    return;
                };
                let _ = build_result_sender.send(Ok(()));
                #[cfg(test)]
                running.store(true, std::sync::atomic::Ordering::Relaxed);
                run_root_callback_loop(&tokio_runtime, &runtime, events);
                #[cfg(test)]
                running.store(false, std::sync::atomic::Ordering::Relaxed);
            })
            .or_err(
                ERR_PLUGIN_FAILED,
                "failed to start the wasm root callback thread",
            )?;
        let build_result = build_result_receiver.recv().or_err(
            ERR_PLUGIN_FAILED,
            "the wasm root callback thread stopped before it built its runtime",
        )?;
        build_result.or_err(
            ERR_PLUGIN_FAILED,
            "failed to build the runtime of the wasm root callback thread",
        )
    }
}

/// Run the loop of the root callback thread until the `WasmRuntime` drops.
///
/// The thread waits for work inside `block_on` and runs the work outside it. When the thread
/// holds the last reference to the `WasmRuntime`, the runtime drops outside `block_on`, where
/// a service of the proxy that owns a tokio runtime, such as a log sink, can drop without a
/// panic.
fn run_root_callback_loop(
    tokio_runtime: &Runtime,
    runtime: &Weak<RuntimeInner>,
    mut events: UnboundedReceiver<RootCallbackEvent>,
) {
    let mut callback_loop = RootCallbackLoop::default();
    while tokio_runtime.block_on(callback_loop.wait_for_work(&mut events)) {
        let Some(runtime) = runtime.upgrade() else {
            return;
        };
        let entered = tokio_runtime.enter();
        callback_loop.run_due_work(&runtime);
        drop(entered);
        drop(runtime);
    }
}

#[cfg(test)]
mod tests {
    use crate::callout::CalloutResult;
    use crate::observability::PrometheusMetricSink;
    use crate::runtime::pool::events::{GuestAddress, SlotIndex};
    use crate::test_support::callouts::{authz_services, callout_ctx_with_services, FixedSender};
    use crate::test_support::{
        crate_log_lines_with, plugin, record_crate_logs, session, wat_guest, RecordedGuestLogs,
        Wat, GET,
    };
    use crate::{WasmRuntime, WasmServices};
    use parking_lot::Mutex;
    use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
    use proxy_wasm_host::abi::v0_2_1::CalloutId;
    use proxy_wasm_host::abi::v0_2_1::{GuestId, LogContext, LogSink};
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};
    use tokio::sync::Notify;

    const TICK_EVERY_20_MS: &str = "(drop (call $set_tick_period (i32.const 20))) i32.const 1";
    const LOG_TICK: &str = "(call $log_tick)";

    /// The guest log lines, with the guest that wrote each one.
    #[derive(Default)]
    struct LinesByGuest(Mutex<Vec<(GuestId, String)>>);

    impl LogSink for LinesByGuest {
        fn log(&self, context: LogContext<'_>, _level: LogLevel, message: &[u8]) {
            let line = String::from_utf8_lossy(message).into_owned();
            self.0.lock().push((context.guest, line));
        }
    }

    fn runtime_with_logs<S: LogSink + Default + 'static>(
        label: &str,
        wat: Wat,
        slots: usize,
    ) -> (WasmRuntime, Arc<S>) {
        let logs = Arc::new(S::default());
        let services = WasmServices {
            log_sink: logs.clone(),
            ..WasmServices::default()
        };
        let plugin_conf = plugin(label, wat_guest(label, wat), slots);
        let runtime = WasmRuntime::new_with_services(vec![plugin_conf], services).unwrap();
        (runtime, logs)
    }

    fn count_lines_with(logs: &RecordedGuestLogs, text: &str) -> usize {
        logs.0
            .lock()
            .iter()
            .filter(|line| line.contains(text))
            .count()
    }

    fn wait_until(check: impl Fn() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !check() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        check()
    }

    #[test]
    fn each_slot_gets_its_own_ticks() {
        let wat = Wat {
            configure: TICK_EVERY_20_MS,
            tick: Some(LOG_TICK),
            ..Wat::default()
        };
        let (runtime, logs) = runtime_with_logs::<LinesByGuest>("two-slot-ticks", wat, 2);

        runtime.inner.start_threads().unwrap();

        let guests_that_ticked = || {
            logs.0
                .lock()
                .iter()
                .map(|(guest, _)| *guest)
                .collect::<HashSet<_>>()
        };
        assert!(wait_until(|| guests_that_ticked().len() == 2));
    }

    #[test]
    fn a_tick_waits_for_a_slot_that_a_request_holds() {
        let wat = Wat {
            configure: TICK_EVERY_20_MS,
            tick: Some(LOG_TICK),
            ..Wat::default()
        };
        let (runtime, logs) = runtime_with_logs::<RecordedGuestLogs>("busy-slot", wat, 1);
        runtime.inner.start_threads().unwrap();
        wait_until(|| count_lines_with(&logs, "tick") > 0);
        let guard = runtime.inner.pools[0].lock_slot(0);
        let ticks_when_locked = count_lines_with(&logs, "tick");
        thread::sleep(Duration::from_millis(100));
        let ticks_after_100_ms_locked = count_lines_with(&logs, "tick");

        drop(guard);

        assert_eq!(ticks_after_100_ms_locked, ticks_when_locked);
        assert!(wait_until(
            || count_lines_with(&logs, "tick") > ticks_when_locked
        ));
    }

    #[test]
    fn a_guest_that_traps_in_a_tick_is_replaced() {
        record_crate_logs();
        let wat = Wat {
            configure: TICK_EVERY_20_MS,
            tick: Some("unreachable"),
            ..Wat::default()
        };
        let (runtime, _logs) = runtime_with_logs::<RecordedGuestLogs>("trap-in-tick", wat, 1);

        runtime.inner.start_threads().unwrap();

        let replaced = "wasm plugin trap-in-tick replaced the guest of slot 0 after a failure";
        assert!(wait_until(|| !crate_log_lines_with(replaced).is_empty()));
    }

    #[test]
    fn a_tick_reads_empty_header_pairs_and_proxy_continue_stream_returns_ok() {
        let wat = Wat {
            data_segments: r#"(data (i32.const 700) "pairs ok") (data (i32.const 710) "continue ok")"#,
            configure: TICK_EVERY_20_MS,
            tick: Some(
                "(if (i32.eqz (call $get_header_pairs (i32.const 0) (i32.const 512) (i32.const 516)))
                    (then (drop (call $log (i32.const 2) (i32.const 700) (i32.const 8)))))
                (if (i32.eqz (call $continue_stream (i32.const 0)))
                    (then (drop (call $log (i32.const 2) (i32.const 710) (i32.const 11)))))",
            ),
            ..Wat::default()
        };
        let (runtime, logs) =
            runtime_with_logs::<RecordedGuestLogs>("tick-with-no-request", wat, 1);

        runtime.inner.start_threads().unwrap();

        assert!(wait_until(|| count_lines_with(&logs, "pairs ok") > 0));
        assert!(wait_until(|| count_lines_with(&logs, "continue ok") > 0));
    }

    #[test]
    fn the_thread_ends_when_the_runtime_drops() {
        let (runtime, _logs) =
            runtime_with_logs::<RecordedGuestLogs>("thread-end", Wat::default(), 1);
        runtime.inner.start_threads().unwrap();
        let running = runtime.inner.root_callback_thread.running.clone();
        assert!(wait_until(|| running.load(Ordering::Relaxed)));

        drop(runtime);

        assert!(wait_until(|| !running.load(Ordering::Relaxed)));
    }

    /// A log sink that owns a tokio runtime, and that keeps a guest call that logs from
    /// returning until the test releases it.
    struct BlockingSinkWithTokioRuntime {
        _tokio_runtime: tokio::runtime::Runtime,
        in_guest_call: Arc<AtomicBool>,
        released: Arc<AtomicBool>,
    }

    impl LogSink for BlockingSinkWithTokioRuntime {
        fn log(&self, _context: LogContext<'_>, _level: LogLevel, _message: &[u8]) {
            self.in_guest_call.store(true, Ordering::Relaxed);
            wait_until(|| self.released.load(Ordering::Relaxed));
        }
    }

    #[test]
    fn a_runtime_that_drops_during_a_tick_ends_the_thread_with_no_panic() {
        let in_guest_call = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicBool::new(false));
        let services = WasmServices {
            log_sink: Arc::new(BlockingSinkWithTokioRuntime {
                _tokio_runtime: tokio::runtime::Runtime::new().unwrap(),
                in_guest_call: in_guest_call.clone(),
                released: released.clone(),
            }),
            ..WasmServices::default()
        };
        let wat = Wat {
            configure: TICK_EVERY_20_MS,
            tick: Some(LOG_TICK),
            ..Wat::default()
        };
        let plugin_conf = plugin("drop-in-tick", wat_guest("drop-in-tick", wat), 1);
        let runtime = WasmRuntime::new_with_services(vec![plugin_conf], services).unwrap();
        runtime.inner.start_threads().unwrap();
        let running = runtime.inner.root_callback_thread.running.clone();
        assert!(wait_until(|| in_guest_call.load(Ordering::Relaxed)));
        drop(runtime);

        released.store(true, Ordering::Relaxed);

        assert!(wait_until(|| !running.load(Ordering::Relaxed)));
    }

    #[tokio::test]
    async fn a_held_context_whose_ctx_drops_gets_proxy_on_delete_and_no_proxy_on_log() {
        let wat = Wat {
            data_segments: r#"(data (i32.const 700) "logged") (data (i32.const 710) "deleted")"#,
            configure: TICK_EVERY_20_MS,
            done: "(i32.store (i32.const 608) (local.get 0)) i32.const 0",
            tick: Some(
                "(if (i32.load (i32.const 608)) (then
                    (drop (call $set_effective_context (i32.load (i32.const 608))))
                    (drop (call $proxy_done))
                    (i32.store (i32.const 608) (i32.const 0))))",
            ),
            log: Some("(drop (call $log (i32.const 2) (i32.const 700) (i32.const 6)))"),
            delete: "(drop (call $log (i32.const 2) (i32.const 710) (i32.const 7)))",
            ..Wat::default()
        };
        let (runtime, logs) = runtime_with_logs::<RecordedGuestLogs>("a", wat, 1);
        let mut ctx = runtime.chain(&["a"]).unwrap().new_ctx();
        let (mut session, _client) = session(GET).await;
        ctx.request_filter(&mut session).await.unwrap();

        drop(ctx);

        assert!(wait_until(|| count_lines_with(&logs, "deleted") == 1));
        assert_eq!(runtime.held_contexts(), 0);
        assert_eq!(count_lines_with(&logs, "logged"), 0);
    }

    #[test]
    fn a_root_callout_result_for_a_replaced_guest_is_dropped() {
        let gate = Arc::new(Notify::new());
        let sender = FixedSender::responds_after("ok", gate.clone());
        let wat = Wat {
            configure: "(call $call_and_log_status) i32.const 1",
            http_call_response: Some("(call $log_result (local.get 2))"),
            ..Wat::default()
        };
        let logs = Arc::new(RecordedGuestLogs::default());
        let mut services = authz_services();
        services.log_sink = logs.clone();
        let plugin_conf = plugin("replaced", wat_guest("replaced", wat), 1);
        let (runtime, _ctx) =
            callout_ctx_with_services(vec![plugin_conf], sender.clone(), services);
        runtime.inner.start_threads().unwrap();
        wait_until(|| sender.sent_count() == 1);
        runtime.inner.pools[0].replace_slot(0);
        wait_until(|| sender.sent_count() == 2);

        gate.notify_waiters();

        assert!(wait_until(|| count_lines_with(&logs, "response") == 1));
        thread::sleep(Duration::from_millis(100));
        assert_eq!(count_lines_with(&logs, "response"), 1);
    }

    #[tokio::test]
    async fn plugins_with_one_vm_id_share_a_queue_and_a_counter() {
        let configure = "(drop (call $register_queue (i32.const 700) (i32.const 1) (i32.const 640)))
            (drop (call $define_metric (i32.const 0) (i32.const 720) (i32.const 14) (i32.const 644)))
            i32.const 1";
        let data_segments = r#"(data (i32.const 700) "q") (data (i32.const 720) "shared_counter")
            (data (i32.const 740) "first ready") (data (i32.const 760) "second ready")"#;
        let first = Wat {
            data_segments,
            configure,
            request_headers:
                "(drop (call $increment_metric (i32.load (i32.const 644)) (i64.const 1)))
                (drop (call $enqueue (i32.load (i32.const 640)) (i32.const 700) (i32.const 1)))
                i32.const 0",
            queue_ready: Some("(drop (call $log (i32.const 2) (i32.const 740) (i32.const 11)))"),
            ..Wat::default()
        };
        let second = Wat {
            request_headers: "(drop (call $increment_metric (i32.load (i32.const 644)) (i64.const 1))) i32.const 0",
            queue_ready: Some("(drop (call $log (i32.const 2) (i32.const 760) (i32.const 12)))"),
            ..first
        };
        let registry = prometheus::Registry::new();
        let logs = Arc::new(RecordedGuestLogs::default());
        let services = WasmServices {
            log_sink: logs.clone(),
            metric_sink: Arc::new(PrometheusMetricSink::new(registry.clone()).unwrap()),
            ..WasmServices::default()
        };
        let mut plugins = vec![
            plugin("first", wat_guest("shared-first", first), 1),
            plugin("second", wat_guest("shared-second", second), 1),
        ];
        for plugin_conf in &mut plugins {
            plugin_conf.vm_id = "shared".to_string();
        }
        let runtime = WasmRuntime::new_with_services(plugins, services).unwrap();
        let mut ctx = runtime.chain(&["first", "second"]).unwrap().new_ctx();
        let (mut session, _client) = session(GET).await;

        ctx.request_filter(&mut session).await.unwrap();

        assert!(wait_until(|| count_lines_with(&logs, "second ready") == 1));
        assert_eq!(count_lines_with(&logs, "first ready"), 0);
        let mut output = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        prometheus::Encoder::encode(&encoder, &registry.gather(), &mut output).unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(
            output.contains("shared_counter{vm_id=\"shared\"} 2"),
            "{output}"
        );
    }

    #[test]
    fn a_result_for_a_callout_that_is_no_longer_open_is_dropped_with_no_warning() {
        record_crate_logs();
        let wat = Wat::default();
        let (runtime, _logs) = runtime_with_logs::<RecordedGuestLogs>("closed-callout", wat, 1);
        let (guest, root) = {
            let guard = runtime.inner.pools[0].lock_slot(0);
            let loaded = guard.as_ref().unwrap();
            (loaded.guest.id(), loaded.root)
        };
        let finished = super::root_callouts::FinishedCallout {
            address: GuestAddress {
                slot: SlotIndex {
                    pool_index: 0,
                    slot_index: 0,
                },
                guest,
            },
            context: root,
            id: CalloutId::try_from(7).unwrap(),
            result: CalloutResult::Failed,
        };

        let mut callback_loop = super::callback_loop::RootCallbackLoop::default();
        callback_loop.run_work(
            &runtime.inner,
            &super::work::Work::DeliverCalloutResult(finished),
        );

        let warnings = crate_log_lines_with("closed-callout failed in proxy_on_http_call_response");
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn a_slot_whose_guest_was_lost_gets_no_tick() {
        record_crate_logs();
        let wat = Wat {
            configure: TICK_EVERY_20_MS,
            tick: Some(LOG_TICK),
            ..Wat::default()
        };
        let (runtime, logs) = runtime_with_logs::<RecordedGuestLogs>("lost-guest-tick", wat, 1);
        runtime.inner.start_threads().unwrap();
        wait_until(|| count_lines_with(&logs, "tick") > 0);

        runtime.inner.pools[0].fail_slot(0);

        let when_lost = count_lines_with(&logs, "tick");
        thread::sleep(Duration::from_millis(100));
        assert_eq!(count_lines_with(&logs, "tick"), when_lost);
        assert!(crate_log_lines_with("lost-guest-tick failed in").is_empty());
    }

    #[tokio::test]
    async fn a_queue_item_skips_a_lost_registrant_for_the_one_that_registered_before_it() {
        let logs = Arc::new(RecordedGuestLogs::default());
        let services = WasmServices {
            log_sink: logs.clone(),
            ..WasmServices::default()
        };
        let plugin_conf = plugin("a", crate::test_support::fixture("http-example"), 2);
        let runtime = WasmRuntime::new_with_services(vec![plugin_conf], services).unwrap();
        runtime.inner.pools[0].fail_slot(1);
        let mut ctx = runtime.chain(&["a"]).unwrap().new_ctx();
        let (mut session, _client) = session(GET).await;

        ctx.request_filter(&mut session).await.unwrap();

        assert!(wait_until(|| count_lines_with(
            &logs,
            "path seen: /original"
        ) == 1));
    }
}
