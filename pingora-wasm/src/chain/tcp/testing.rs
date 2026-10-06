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

//! Test harness for TCP connections
//!
//! A test runs the copy loop of one connection over in-memory streams. It holds the client end
//! of the downstream stream and the server end of the upstream stream.

use super::connection::{run_connection, ConnectUpstream, ConnectionLimits};
use super::plugins::TcpPlugins;
use super::WasmTcpConnection;
use crate::properties::built_in::RequestFacts;
use crate::test_support::callouts::{authz_services, FixedSender};
use crate::test_support::{plugin, wat_guest, RecordedFailures, RecordedGuestLogs, Wat};
use crate::{WasmPluginConf, WasmRuntime, WasmServices};
use async_trait::async_trait;
use parking_lot::Mutex;
use pingora_core::protocols::Stream;
use pingora_error::{Error, ErrorType, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::task::JoinHandle;

/// WAT functions and strings for TCP test guests. Strings start at 700.
const TCP_DATA: &str = r#"
  (data (i32.const 700) "downstream local")
  (data (i32.const 720) "downstream remote")
  (data (i32.const 740) "upstream local")
  (data (i32.const 760) "upstream remote")
  (data (i32.const 780) "log")
  (data (i32.const 790) "paused")
  (data (i32.const 800) "end")
  (func $log_text (param $at i32) (param $size i32)
    (drop (call $log (i32.const 2) (local.get $at) (local.get $size))))
  (func $log_peer (param $peer i32) (param $local i32) (param $local_size i32)
      (param $remote i32) (param $remote_size i32)
    (if (i32.eq (local.get $peer) (i32.const 1))
      (then (call $log_text (local.get $local) (local.get $local_size)))
      (else (call $log_text (local.get $remote) (local.get $remote_size)))))
"#;

pub(super) const LOG_DOWNSTREAM_CLOSE: &str =
    "(call $log_peer (local.get 1) (i32.const 700) (i32.const 16) (i32.const 720) (i32.const 17))";
pub(super) const LOG_UPSTREAM_CLOSE: &str =
    "(call $log_peer (local.get 1) (i32.const 740) (i32.const 14) (i32.const 760) (i32.const 15))";
pub(super) const LOG_LOG: &str = "(call $log_text (i32.const 780) (i32.const 3))";
/// Log "paused" and pause until the data has at least 6 bytes.
pub(super) const PAUSE_UNDER_SIX: &str = "(if (result i32) (i32.lt_u (local.get 1) (i32.const 6))
      (then (call $log_text (i32.const 790) (i32.const 6)) (i32.const 1))
      (else (i32.const 0)))";
/// Log "end" when the data callback has the end of its direction, and continue.
pub(super) const LOG_END: &str =
    "(if (local.get 2) (then (call $log_text (i32.const 800) (i32.const 3)))) (i32.const 0)";

/// Build a plugin conf from `wat`, with the TCP test functions and strings added.
pub(super) fn tcp_plugin(name: &str, wat: Wat) -> WasmPluginConf {
    let data = format!("{TCP_DATA}{}", wat.data_segments);
    let wat = Wat {
        data_segments: Box::leak(data.into_boxed_str()),
        ..wat
    };
    plugin(name, wat_guest(name, wat), 1)
}

/// A runtime that records guest logs and plugin failures.
pub(super) struct TcpRuntime {
    pub(super) runtime: WasmRuntime,
    pub(super) logs: Arc<RecordedGuestLogs>,
    pub(super) failures: Arc<RecordedFailures>,
}

impl TcpRuntime {
    pub(super) fn new(plugins: Vec<WasmPluginConf>) -> Self {
        Self::with_sender(plugins, FixedSender::responds("allowed"))
    }

    pub(super) fn with_sender(plugins: Vec<WasmPluginConf>, sender: Arc<FixedSender>) -> Self {
        let logs = Arc::new(RecordedGuestLogs::default());
        let failures = Arc::new(RecordedFailures::default());
        let services = WasmServices {
            log_sink: logs.clone(),
            metric_sink: failures.clone(),
            ..authz_services()
        };
        let runtime = WasmRuntime::new_with_callout_sender(plugins, services, sender).unwrap();
        TcpRuntime {
            runtime,
            logs,
            failures,
        }
    }

    pub(super) fn logs(&self) -> Vec<String> {
        self.logs.0.lock().clone()
    }

    /// Start a connection through every plugin of the runtime, in the order they were given.
    pub(super) fn connect(&self, options: ConnectOptions) -> TestConnection {
        let names = self.runtime.inner.plugin_names();
        let ctx = self.runtime.chain(&names).unwrap().new_ctx();
        let (client, downstream) = duplex(options.stream_buffer);
        let (server, upstream) = duplex(options.stream_buffer);
        let connector = TestConnector {
            upstream: Mutex::new(options.connects.then_some(upstream)),
        };
        let plugins = TcpPlugins::new(ctx, test_facts());
        let connection = WasmTcpConnection {
            downstream: Box::new(downstream),
            id: 7,
        };
        let limits = options.limits;
        let task = tokio::spawn(async move {
            run_connection(plugins, connection, &connector, limits).await;
        });
        TestConnection {
            client,
            server,
            task,
        }
    }
}

pub(super) struct ConnectOptions {
    pub(super) limits: ConnectionLimits,
    pub(super) stream_buffer: usize,
    pub(super) connects: bool,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        ConnectOptions {
            limits: ConnectionLimits {
                buffer_limit: 1024 * 1024,
                idle_timeout: Some(Duration::from_secs(10)),
                drain_timeout: Duration::from_secs(10),
            },
            stream_buffer: 64 * 1024,
            connects: true,
        }
    }
}

pub(super) struct TestConnection {
    pub(super) client: DuplexStream,
    pub(super) server: DuplexStream,
    pub(super) task: JoinHandle<()>,
}

impl TestConnection {
    /// Wait for the copy loop to end, failing the test after five seconds.
    pub(super) async fn ended(self) {
        let ended = tokio::time::timeout(Duration::from_secs(5), self.task).await;
        assert!(matches!(ended, Ok(Ok(()))), "{ended:?}");
    }
}

pub(super) async fn send(stream: &mut DuplexStream, bytes: &str) {
    stream.write_all(bytes.as_bytes()).await.unwrap();
}

/// Read exactly `size` bytes from `stream`, failing the test after five seconds.
pub(super) async fn receive(stream: &mut DuplexStream, size: usize) -> String {
    let mut bytes = vec![0; size];
    let read = stream.read_exact(&mut bytes);
    tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

/// Read until the end of `stream`, failing the test after five seconds.
pub(super) async fn receive_to_end(stream: &mut DuplexStream) -> String {
    let mut bytes = Vec::new();
    let read = stream.read_to_end(&mut bytes);
    tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

/// Return whether `stream` stays silent for 100 ms.
pub(super) async fn stays_silent(stream: &mut DuplexStream) -> bool {
    let mut byte = [0; 1];
    let read = stream.read(&mut byte);
    tokio::time::timeout(Duration::from_millis(100), read)
        .await
        .is_err()
}

pub(super) fn test_facts() -> RequestFacts {
    RequestFacts {
        client_address: Some("127.0.0.1:5000".parse().unwrap()),
        server_address: Some("127.0.0.1:5433".parse().unwrap()),
        connection_id: Some(7),
        ..RequestFacts::default()
    }
}

pub(super) const UPSTREAM_ADDRESS: &str = "10.0.0.5:5432";

struct TestConnector {
    upstream: Mutex<Option<DuplexStream>>,
}

#[async_trait]
impl ConnectUpstream for TestConnector {
    async fn connect(
        &self,
        _connection: &WasmTcpConnection,
    ) -> Result<(Stream, Option<SocketAddr>)> {
        let address: SocketAddr = UPSTREAM_ADDRESS.parse().unwrap();
        match self.upstream.lock().take() {
            Some(upstream) => Ok((Box::new(upstream), Some(address))),
            None => Error::e_explain(ErrorType::ConnectRefused, "test upstream refused"),
        }
    }
}
