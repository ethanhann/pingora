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

//! The cost of the filters under load from several threads.
//!
//! Each thread runs its own single-threaded tokio runtime, as a Pingora service does with
//! `work_stealing: false`, and sends requests through in-memory sessions with no IO, so that
//! the time a request waits for its guest stays visible.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pingora_core::upstreams::peer::HttpPeer;
use pingora_http::ResponseHeader;
use pingora_proxy::Session;
use pingora_wasm::{
    LogContext, LogLevel, LogSink, StaticCalloutUpstreams, WasmChain, WasmPluginConf, WasmRuntime,
    WasmServices,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

const THREADS: usize = 8;
const REQUEST: &[u8] = b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
const MICROSECOND: u64 = 1_000;

/// What the callbacks of a benchmark guest do.
#[derive(Clone, Copy)]
enum Guest {
    /// Use `cost` nanoseconds in each header callback.
    Headers {
        cost: u64,
    },
    /// The same, with a tick every 10 ms that also uses `cost` nanoseconds.
    HeadersAndTicks {
        cost: u64,
    },
    /// Send a callout from the request headers, and continue when its result arrives.
    Callout,
    Trap,
}

const SHARED_WAT: &str = r#"
  (import "env" "proxy_get_current_time_nanoseconds" (func $now (param i32) (result i32)))
  (import "env" "proxy_set_tick_period_milliseconds" (func $tick_period (param i32) (result i32)))
  (import "env" "proxy_http_call"
    (func $http_call (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_set_effective_context" (func $set_effective (param i32) (result i32)))
  (import "env" "proxy_continue_stream" (func $continue (param i32) (result i32)))
  (import "env" "proxy_log" (func $log (param i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 112) "authz")
  (data (i32.const 128) "\03\00\00\00\07\00\00\00\03\00\00\00\05\00\00\00\06\00\00\00\0a\00\00\00\0a\00\00\00\3a\6d\65\74\68\6f\64\00\47\45\54\00\3a\70\61\74\68\00\2f\63\68\65\63\6b\00\3a\61\75\74\68\6f\72\69\74\79\00\61\75\74\68\7a\2e\74\65\73\74\00")
  (data (i32.const 296) "refused")
  (func $busy (param $ns i64)
    (local $until i64)
    (drop (call $now (i32.const 0)))
    (local.set $until (i64.add (i64.load (i32.const 0)) (local.get $ns)))
    (block $done
      (loop $spin
        (drop (call $now (i32.const 0)))
        (br_if $done (i64.ge_u (i64.load (i32.const 0)) (local.get $until)))
        (br $spin))))
  ;; Send a callout from `ctx`, remember `ctx` under the callout token, and return whether the
  ;; host refused it. Contexts are kept at 1024 + 4 * (token mod 4096).
  (func $call (param $ctx i32) (result i32)
    (if (result i32) (call $http_call
        (i32.const 112) (i32.const 5) (i32.const 128) (i32.const 75)
        (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 8))
      (then (drop (call $log (i32.const 2) (i32.const 296) (i32.const 7))) (i32.const 1))
      (else
        (i32.store (i32.add (i32.const 1024)
            (i32.shl (i32.and (i32.load (i32.const 8)) (i32.const 4095)) (i32.const 2)))
          (local.get $ctx))
        (i32.const 0))))
  (func (export "proxy_abi_version_0_2_1"))
  (func (export "proxy_on_memory_allocate") (param i32) (result i32) i32.const 40000)
  (func (export "proxy_on_context_create") (param i32 i32))
  (func (export "proxy_on_vm_start") (param i32 i32) (result i32) i32.const 1)
  (func (export "proxy_on_done") (param i32) (result i32) i32.const 1)
  (func (export "proxy_on_log") (param i32))
  (func (export "proxy_on_delete") (param i32))
"#;

fn callbacks(guest: Guest) -> String {
    let busy = |cost: u64| format!("(call $busy (i64.const {cost}))");
    let (configure, request, response, tick, delivery) = match guest {
        Guest::Headers { cost } => (String::new(), busy(cost), busy(cost), None, String::new()),
        Guest::HeadersAndTicks { cost } => (
            "(drop (call $tick_period (i32.const 10)))".to_string(),
            busy(cost),
            busy(cost),
            Some(busy(cost)),
            String::new(),
        ),
        Guest::Callout => {
            let request = "(if (call $call (local.get 0)) (then (return (i32.const 0))))
                (return (i32.const 1))"
                .to_string();
            let delivery = "(drop (call $set_effective (i32.load (i32.add (i32.const 1024)
                    (i32.shl (i32.and (local.get 1) (i32.const 4095)) (i32.const 2))))))
                (drop (call $continue (i32.const 0)))"
                .to_string();
            (String::new(), request, String::new(), None, delivery)
        }
        Guest::Trap => (
            String::new(),
            "unreachable".to_string(),
            String::new(),
            None,
            String::new(),
        ),
    };
    let tick = tick.unwrap_or_default();
    format!(
        r#"
  (func (export "proxy_on_configure") (param i32 i32) (result i32) {configure} i32.const 1)
  (func (export "proxy_on_request_headers") (param i32 i32 i32) (result i32) {request} i32.const 0)
  (func (export "proxy_on_response_headers") (param i32 i32 i32) (result i32) {response} i32.const 0)
  (func (export "proxy_on_tick") (param i32) {tick})
  (func (export "proxy_on_http_call_response") (param i32 i32 i32 i32 i32) {delivery})"#
    )
}

fn guest_file(label: &str, guest: Guest) -> PathBuf {
    let wat = format!("(module {SHARED_WAT} {})", callbacks(guest));
    let path = std::env::temp_dir().join(format!(
        "pingora-wasm-bench-{label}-{}.wasm",
        std::process::id()
    ));
    std::fs::write(&path, wat::parse_str(wat).unwrap()).unwrap();
    path
}

/// A log sink that counts the callouts the host refused.
#[derive(Default)]
struct RefusedCallouts(AtomicUsize);

impl LogSink for RefusedCallouts {
    fn log(&self, _context: LogContext<'_>, _level: LogLevel, message: &[u8]) {
        if message == b"refused" {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// An origin for callouts that responds after `delay_ms`.
struct CalloutOrigin {
    port: u16,
    delay_ms: Arc<AtomicU64>,
}

impl CalloutOrigin {
    fn start() -> Self {
        let delay_ms = Arc::new(AtomicU64::new(0));
        let delay = delay_ms.clone();
        let (port_sender, port_receiver) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let origin = pingora_test_utils::http_origin::HttpOrigin::bind(move |_request| {
                    let delay = delay.load(Ordering::Relaxed);
                    async move {
                        if delay > 0 {
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                        }
                        http::Response::builder()
                            .status(200)
                            .body(bytes::Bytes::from_static(b"0"))
                            .unwrap()
                    }
                })
                .await
                .unwrap();
                port_sender.send(origin.addr().port()).unwrap();
                std::future::pending::<()>().await;
            });
        });
        let port = port_receiver.recv().unwrap();
        CalloutOrigin { port, delay_ms }
    }
}

fn chain(label: &str, guest: Guest, slots: usize, services: WasmServices) -> WasmChain {
    let mut plugin = WasmPluginConf::new(label, guest_file(label, guest));
    plugin.slots = Some(slots);
    let runtime = WasmRuntime::new_with_services(vec![plugin], services).unwrap();
    runtime.chain(&[label]).unwrap()
}

async fn one_request(chain: Option<&WasmChain>) {
    let (mut client, server) = tokio::io::duplex(4096);
    client.write_all(REQUEST).await.unwrap();
    let mut session = Session::new_h1(Box::new(server));
    session.read_request().await.unwrap();
    let Some(chain) = chain else {
        return;
    };
    let mut ctx = chain.new_ctx();
    if ctx.request_filter(&mut session).await.is_ok() {
        let mut response = ResponseHeader::build(200, None).unwrap();
        let _ = ctx.response_filter(&mut session, &mut response).await;
    }
    ctx.logging(&mut session).await;
}

struct Job {
    chain: Option<WasmChain>,
    concurrency: usize,
    per_worker: u64,
    start: Arc<Barrier>,
    done: std::sync::mpsc::Sender<()>,
}

/// Threads that each keep one tokio runtime for the whole benchmark, as a Pingora service
/// does, so that a pooled callout connection always belongs to a runtime that is running.
static WORKERS: std::sync::LazyLock<Vec<tokio::sync::mpsc::UnboundedSender<Job>>> =
    std::sync::LazyLock::new(|| {
        (0..2 * THREADS)
            .map(|_| {
                let (sender, mut jobs) = tokio::sync::mpsc::unbounded_channel::<Job>();
                thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    while let Some(job) = runtime.block_on(jobs.recv()) {
                        job.start.wait();
                        runtime.block_on(async {
                            let chain = job.chain.as_ref();
                            let tasks = (0..job.concurrency).map(|_| async {
                                for _ in 0..job.per_worker {
                                    one_request(chain).await;
                                }
                            });
                            futures::future::join_all(tasks).await;
                        });
                        job.done.send(()).unwrap();
                    }
                });
                sender
            })
            .collect()
    });

/// Send `requests` requests from [THREADS] threads with `concurrency` at a time on each, and
/// return the time they took, scaled to exactly `requests`.
fn run(chain: Option<&WasmChain>, concurrency: usize, requests: u64) -> Duration {
    let threads = THREADS;
    let workers = (threads * concurrency) as u64;
    let per_worker = requests.div_ceil(workers).max(1);
    let start = Arc::new(Barrier::new(threads + 1));
    let (done, finished) = std::sync::mpsc::channel();
    for worker in &WORKERS[..threads] {
        let job = Job {
            chain: chain.cloned(),
            concurrency,
            per_worker,
            start: start.clone(),
            done: done.clone(),
        };
        worker.send(job).ok().unwrap();
    }
    start.wait();
    let started = Instant::now();
    for _ in 0..threads {
        finished.recv().unwrap();
    }
    let elapsed = started.elapsed();
    elapsed.mul_f64(requests as f64 / (per_worker * workers) as f64)
}

fn headers(c: &mut Criterion) {
    let mut group = c.benchmark_group("headers");
    group.throughput(Throughput::Elements(1));
    group.bench_function("no plugin", |b| b.iter_custom(|n| run(None, 1, n)));
    for cost_us in [1, 50, 500] {
        for slots in [1, THREADS, 2 * THREADS] {
            let guest = Guest::Headers {
                cost: cost_us * MICROSECOND,
            };
            let chain = chain("headers", guest, slots, WasmServices::default());
            let id = BenchmarkId::new(format!("{cost_us} us"), format!("{slots} slots"));
            group.bench_function(id, |b| b.iter_custom(|n| run(Some(&chain), 1, n)));
        }
    }
    group.finish();
}

fn ticks(c: &mut Criterion) {
    let mut group = c.benchmark_group("ticks every 10 ms");
    group.throughput(Throughput::Elements(1));
    for slots in [1, THREADS] {
        let guest = Guest::HeadersAndTicks {
            cost: 50 * MICROSECOND,
        };
        let chain = chain("ticks", guest, slots, WasmServices::default());
        let id = BenchmarkId::new("50 us", format!("{slots} slots"));
        group.bench_function(id, |b| b.iter_custom(|n| run(Some(&chain), 1, n)));
    }
    group.finish();
}

fn callout_services(origin: &CalloutOrigin, refused: Arc<RefusedCallouts>) -> WasmServices {
    let mut upstreams = StaticCalloutUpstreams::new();
    let peer = HttpPeer::new(("127.0.0.1", origin.port), false, String::new());
    upstreams.insert("authz", peer);
    let mut services = WasmServices::default();
    services.callout_upstreams = Arc::new(upstreams);
    services.log_sink = refused;
    services
}

fn callout_limit_at_one_slot(_: &mut Criterion) {
    callout_limit(&CalloutOrigin::start());
}

/// Hold more than 1024 callouts open at once at 1 slot, and print how many the host refused.
fn callout_limit(origin: &CalloutOrigin) {
    origin.delay_ms.store(20, Ordering::Relaxed);
    let refused = Arc::new(RefusedCallouts::default());
    let services = callout_services(origin, refused.clone());
    let chain = chain("callout-limit", Guest::Callout, 1, services);
    let concurrency = 160;
    let requests = (THREADS * concurrency * 4) as u64;
    let took = run(Some(&chain), concurrency, requests);
    let refused = refused.0.load(Ordering::Relaxed);
    origin.delay_ms.store(0, Ordering::Relaxed);
    println!(
        "callout limit: {requests} requests, {} at once, origin delay 20 ms, \
         {refused} callouts refused by the host, {took:?}",
        THREADS * concurrency
    );
}

fn traps(c: &mut Criterion) {
    let mut group = c.benchmark_group("trap on each request");
    group.throughput(Throughput::Elements(1));
    for slots in [1, THREADS] {
        let chain = chain("trap", Guest::Trap, slots, WasmServices::default());
        let id = BenchmarkId::new("trap", format!("{slots} slots"));
        group.bench_function(id, |b| b.iter_custom(|n| run(Some(&chain), 1, n)));
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(3));
    targets = headers, ticks, callout_limit_at_one_slot, traps
}
criterion_main!(benches);
