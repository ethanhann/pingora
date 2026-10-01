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
    use crate::test_support::{
        crate_log_lines_with, plugin, record_crate_logs, wat_guest, RecordedGuestLogs, Wat,
    };
    use crate::{WasmProperties, WasmRuntime, WasmServices};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    const TICK_EVERY_200_MS: &str = "(drop (call $set_tick_period (i32.const 200))) i32.const 1";
    const TICK_EVERY_20_MS: &str = "(drop (call $set_tick_period (i32.const 20))) i32.const 1";

    fn runtime_with_logs(
        label: &str,
        wat: Wat,
        slots: usize,
    ) -> (WasmRuntime, Arc<RecordedGuestLogs>) {
        runtime_with_services(label, wat, slots, WasmServices::default())
    }

    fn runtime_with_services(
        label: &str,
        wat: Wat,
        slots: usize,
        mut services: WasmServices,
    ) -> (WasmRuntime, Arc<RecordedGuestLogs>) {
        let logs = Arc::new(RecordedGuestLogs::default());
        services.log_sink = logs.clone();
        let conf = plugin(label, wat_guest(label, wat), slots);
        let runtime = WasmRuntime::new_with_services(vec![conf], services).unwrap();
        (runtime, logs)
    }

    fn lines(logs: &RecordedGuestLogs, text: &str) -> usize {
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
    fn each_slot_gets_its_own_tick_in_each_period() {
        let wat = Wat {
            configure: TICK_EVERY_200_MS,
            tick: Some("(call $log_tick)"),
            ..Wat::default()
        };
        let (runtime, logs) = runtime_with_logs("two-slot-ticks", wat, 2);

        runtime.inner.start_threads().unwrap();
        thread::sleep(Duration::from_millis(300));

        assert_eq!(lines(&logs, "tick"), 2);
    }

    #[test]
    fn a_guest_that_traps_in_a_tick_is_replaced() {
        record_crate_logs();
        let wat = Wat {
            configure: TICK_EVERY_20_MS,
            tick: Some("unreachable"),
            ..Wat::default()
        };
        let (runtime, _logs) = runtime_with_logs("trap-in-tick", wat, 1);

        runtime.inner.start_threads().unwrap();

        let replaced = "wasm plugin trap-in-tick replaced the guest of slot 0 after a failure";
        assert!(wait_until(|| !crate_log_lines_with(replaced).is_empty()));
    }

    #[test]
    fn the_thread_ends_when_the_runtime_drops() {
        let (runtime, _logs) = runtime_with_logs("thread-end", Wat::default(), 1);
        runtime.inner.start_threads().unwrap();
        let running = runtime.inner.root_callbacks.running.clone();
        assert!(wait_until(|| running.load(Ordering::Relaxed)));

        drop(runtime);

        assert!(wait_until(|| !running.load(Ordering::Relaxed)));
    }

    #[test]
    fn a_plugin_reads_a_fixed_property_when_it_is_configured() {
        let wat = Wat {
            data: r#"(data (i32.const 700) "node\00name")"#,
            configure: "(call $log_property (i32.const 700) (i32.const 9)) i32.const 1",
            ..Wat::default()
        };
        let mut services = WasmServices::default();
        let mut fixed = WasmProperties::new();
        fixed.insert(&["node", "name"], "edge-1");
        services.fixed_properties = fixed;

        let (_runtime, logs) = runtime_with_services("fixed-in-configure", wat, 1, services);

        assert_eq!(lines(&logs, "edge-1"), 1);
    }
}
