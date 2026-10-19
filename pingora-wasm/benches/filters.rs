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
use pingora_http::ResponseHeader;
use pingora_proxy::Session;
use pingora_wasm::{WasmChainHandle, WasmPluginConf, WasmPlugins, WasmRuntime};
use std::path::PathBuf;
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
    Trap,
}

const SHARED_WAT: &str = r#"
  (import "env" "proxy_get_current_time_nanoseconds" (func $now (param i32) (result i32)))
  (import "env" "proxy_set_tick_period_milliseconds" (func $tick_period (param i32) (result i32)))
  (memory (export "memory") 1)
  (func $busy (param $ns i64)
    (local $until i64)
    (drop (call $now (i32.const 0)))
    (local.set $until (i64.add (i64.load (i32.const 0)) (local.get $ns)))
    (block $done
      (loop $spin
        (drop (call $now (i32.const 0)))
        (br_if $done (i64.ge_u (i64.load (i32.const 0)) (local.get $until)))
        (br $spin))))
  (func (export "proxy_abi_version_0_2_1"))
  (func (export "proxy_on_memory_allocate") (param i32) (result i32) i32.const 1024)
  (func (export "proxy_on_context_create") (param i32 i32))
  (func (export "proxy_on_vm_start") (param i32 i32) (result i32) i32.const 1)
  (func (export "proxy_on_done") (param i32) (result i32) i32.const 1)
  (func (export "proxy_on_log") (param i32))
  (func (export "proxy_on_delete") (param i32))
"#;

fn callbacks(guest: Guest) -> String {
    let busy = |cost: u64| format!("(call $busy (i64.const {cost}))");
    let (configure, request, response, tick) = match guest {
        Guest::Headers { cost } => (String::new(), busy(cost), busy(cost), String::new()),
        Guest::HeadersAndTicks { cost } => (
            "(drop (call $tick_period (i32.const 10)))".to_string(),
            busy(cost),
            busy(cost),
            busy(cost),
        ),
        Guest::Trap => (
            String::new(),
            "unreachable".to_string(),
            String::new(),
            String::new(),
        ),
    };
    format!(
        r#"
  (func (export "proxy_on_configure") (param i32 i32) (result i32) {configure} i32.const 1)
  (func (export "proxy_on_request_headers") (param i32 i32 i32) (result i32) {request} i32.const 0)
  (func (export "proxy_on_response_headers") (param i32 i32 i32) (result i32) {response} i32.const 0)
  (func (export "proxy_on_tick") (param i32) {tick})"#
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

fn chain(label: &str, guest: Guest, slots: usize) -> WasmChainHandle {
    let mut plugin = WasmPluginConf::new(label, guest_file(label, guest));
    plugin.slots = Some(slots);
    let runtime = WasmRuntime::new(vec![plugin]).unwrap();
    let plugins = WasmPlugins::new(runtime, [("bench", [label])]).unwrap();
    plugins.chain("bench").unwrap()
}

async fn one_request(chain: Option<&WasmChainHandle>) {
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
    chain: Option<WasmChainHandle>,
    per_thread: u64,
    start: Arc<Barrier>,
    done: std::sync::mpsc::Sender<()>,
}

/// Threads that each keep one tokio runtime for the whole benchmark, as a Pingora service does.
static WORKERS: std::sync::LazyLock<Vec<tokio::sync::mpsc::UnboundedSender<Job>>> =
    std::sync::LazyLock::new(|| {
        (0..THREADS)
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
                            for _ in 0..job.per_thread {
                                one_request(job.chain.as_ref()).await;
                            }
                        });
                        job.done.send(()).unwrap();
                    }
                });
                sender
            })
            .collect()
    });

/// Send `requests` requests from [THREADS] threads, one at a time on each, and return the time
/// they took, scaled to exactly `requests`.
fn run(chain: Option<&WasmChainHandle>, requests: u64) -> Duration {
    let per_thread = requests.div_ceil(THREADS as u64).max(1);
    let start = Arc::new(Barrier::new(THREADS + 1));
    let (done, finished) = std::sync::mpsc::channel();
    for worker in WORKERS.iter() {
        let job = Job {
            chain: chain.cloned(),
            per_thread,
            start: start.clone(),
            done: done.clone(),
        };
        worker.send(job).ok().unwrap();
    }
    start.wait();
    let started = Instant::now();
    for _ in 0..THREADS {
        finished.recv().unwrap();
    }
    let elapsed = started.elapsed();
    elapsed.mul_f64(requests as f64 / (per_thread * THREADS as u64) as f64)
}

fn headers(c: &mut Criterion) {
    let mut group = c.benchmark_group("headers");
    group.throughput(Throughput::Elements(1));
    group.bench_function("no plugin", |b| b.iter_custom(|n| run(None, n)));
    for cost_us in [1, 50, 500] {
        for slots in [1, THREADS, 2 * THREADS] {
            let guest = Guest::Headers {
                cost: cost_us * MICROSECOND,
            };
            let chain = chain("headers", guest, slots);
            let id = BenchmarkId::new(format!("{cost_us} us"), format!("{slots} slots"));
            group.bench_function(id, |b| b.iter_custom(|n| run(Some(&chain), n)));
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
        let chain = chain("ticks", guest, slots);
        let id = BenchmarkId::new("50 us", format!("{slots} slots"));
        group.bench_function(id, |b| b.iter_custom(|n| run(Some(&chain), n)));
    }
    group.finish();
}

fn traps(c: &mut Criterion) {
    let mut group = c.benchmark_group("trap on each request");
    group.throughput(Throughput::Elements(1));
    for slots in [1, THREADS] {
        let chain = chain("trap", Guest::Trap, slots);
        let id = BenchmarkId::new("trap", format!("{slots} slots"));
        group.bench_function(id, |b| b.iter_custom(|n| run(Some(&chain), n)));
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10).measurement_time(Duration::from_secs(3));
    targets = headers, ticks, traps
}
criterion_main!(benches);
