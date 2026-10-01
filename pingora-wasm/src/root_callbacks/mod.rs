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
//! Pingora service. After each guest call, the thread that made it sends an event on one
//! channel, and the root callback thread keeps its own state without locks.

mod callback_loop;
mod queue_registrations;
mod root_callouts;
mod tick_schedule;
mod work;

use crate::runtime::pool::events::{RootCallbackEvent, RootCallbackSender};
use crate::runtime::RuntimeInner;
use crate::ERR_PLUGIN_FAILED;
use callback_loop::RootCallbackLoop;
use parking_lot::Mutex;
use pingora_error::{OrErr, Result};
use std::sync::{Arc, Weak};
use std::thread;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

// Linux keeps 15 bytes of a thread name
const THREAD_NAME: &str = "wasm-root-calls";

/// The channel to the root callback thread, and the start of the thread.
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
    /// The events that guests sent before the start wait in the channel. The thread ends when
    /// the runtime drops, because the runtime holds every sender of the channel. When the
    /// thread cannot start, this returns an error, and the events stay for the next try.
    pub(crate) fn start(&self, runtime: Weak<RuntimeInner>) -> Result<()> {
        // The tokio runtime is built here, as `OffloadRuntime` of pingora-core builds its own,
        // so that a failure returns to the caller
        let tokio_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .or_err(
                ERR_PLUGIN_FAILED,
                "failed to build the runtime of the wasm root callback thread",
            )?;
        let receiver = self.receiver.clone();
        #[cfg(test)]
        let running = self.running.clone();
        thread::Builder::new()
            .name(THREAD_NAME.to_string())
            .spawn(move || {
                let Some(events) = receiver.lock().take() else {
                    return;
                };
                #[cfg(test)]
                running.store(true, std::sync::atomic::Ordering::Relaxed);
                run(&tokio_runtime, &runtime, events);
                #[cfg(test)]
                running.store(false, std::sync::atomic::Ordering::Relaxed);
            })
            .or_err(
                ERR_PLUGIN_FAILED,
                "failed to start the wasm root callback thread",
            )?;
        Ok(())
    }
}

/// Wait for work inside the tokio runtime of the thread, and run it outside, until the runtime
/// of the plugins drops.
///
/// The work runs outside `block_on`, so when the thread holds the last reference to the
/// runtime of the plugins, it drops there, where a connector that owns a tokio runtime can
/// drop too.
fn run(
    tokio_runtime: &Runtime,
    runtime: &Weak<RuntimeInner>,
    mut events: UnboundedReceiver<RootCallbackEvent>,
) {
    let mut state = RootCallbackLoop::default();
    while tokio_runtime.block_on(state.wait_for_work(&mut events)) {
        let Some(runtime) = runtime.upgrade() else {
            return;
        };
        let entered = tokio_runtime.enter();
        state.run_due_work(&runtime);
        drop(entered);
        drop(runtime);
    }
}

#[cfg(test)]
mod tests {
    use crate::metrics::PrometheusMetricSink;
    use crate::test_support::callouts::{authz_services, callout_ctx_with_services, FixedSender};
    use crate::test_support::{
        crate_log_lines_with, plugin, record_crate_logs, session, wat_guest, RecordedGuestLogs,
        Wat, GET,
    };
    use crate::{WasmRuntime, WasmServices};
    use parking_lot::Mutex;
    use proxy_wasm_host::abi::v0_2_1::types::LogLevel;
    use proxy_wasm_host::abi::v0_2_1::{GuestId, LogContext, LogSink};
    use std::collections::HashSet;
    use std::sync::atomic::Ordering;
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
        let conf = plugin(label, wat_guest(label, wat), slots);
        let runtime = WasmRuntime::new_with_services(vec![conf], services).unwrap();
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
        assert!(wait_until(|| count_lines_with(&logs, "tick") > 0));
        let pool = &runtime.inner.pools[0];
        let guard = pool.lock_slot(0);
        let while_locked = count_lines_with(&logs, "tick");
        thread::sleep(Duration::from_millis(100));
        assert_eq!(count_lines_with(&logs, "tick"), while_locked);

        drop(guard);

        assert!(wait_until(|| count_lines_with(&logs, "tick") > while_locked));
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
    fn a_tick_reads_empty_header_pairs_and_continues_with_ok() {
        let wat = Wat {
            data: r#"(data (i32.const 700) "pairs ok") (data (i32.const 710) "continue ok")"#,
            configure: TICK_EVERY_20_MS,
            tick: Some(
                "(if (i32.eqz (call $get_pairs (i32.const 0) (i32.const 512) (i32.const 516)))
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
        let running = runtime.inner.root_callbacks.running.clone();
        assert!(wait_until(|| running.load(Ordering::Relaxed)));

        drop(runtime);

        assert!(wait_until(|| !running.load(Ordering::Relaxed)));
    }

    #[tokio::test]
    async fn a_context_held_after_a_drop_ends_with_no_proxy_on_log() {
        let wat = Wat {
            data: r#"(data (i32.const 700) "logged") (data (i32.const 710) "deleted")"#,
            configure: TICK_EVERY_20_MS,
            done: "(i32.store (i32.const 608) (local.get 0)) i32.const 0",
            tick: Some(
                "(if (i32.load (i32.const 608)) (then
                    (drop (call $set_effective_context (i32.load (i32.const 608))))
                    (drop (call $done))
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
        let conf = plugin("replaced", wat_guest("replaced", wat), 1);
        let (runtime, _ctx) = callout_ctx_with_services(vec![conf], sender.clone(), services);
        runtime.inner.start_threads().unwrap();
        assert!(wait_until(|| sender.sent_count() == 1));
        runtime.inner.pools[0].replace_slot(0);
        assert!(wait_until(|| sender.sent_count() == 2));

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
        let data = r#"(data (i32.const 700) "q") (data (i32.const 720) "shared_counter")
            (data (i32.const 740) "first ready") (data (i32.const 760) "second ready")"#;
        let first = Wat {
            data,
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
        for conf in &mut plugins {
            conf.vm_id = "shared".to_string();
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
}
