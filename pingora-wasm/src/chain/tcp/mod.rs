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

//! TCP filters

mod connection;
mod direction;
mod plugins;
#[cfg(test)]
mod testing;
mod writer;

use crate::properties::built_in::{RequestFacts, TlsFacts};
use crate::WasmChainHandle;
use async_trait::async_trait;
use connection::{run_connection, ConnectUpstream, ConnectionLimits};
use pingora_core::apps::ServerApp;
use pingora_core::connectors::TransportConnector;
use pingora_core::protocols::Stream;
use pingora_core::server::ShutdownWatch;
use pingora_core::upstreams::peer::{BasicPeer, Peer};
use pingora_error::Result;
use plugins::TcpPlugins;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

/// Upstream selection for the connections of a [WasmTcpProxy].
#[async_trait]
pub trait WasmTcpUpstream: Send + Sync {
    /// Define where the proxy should connect `connection` to.
    ///
    /// This runs once every plugin has continued `proxy_on_new_connection`. If it returns an
    /// error, the downstream connection is closed.
    async fn upstream_peer(&self, connection: &WasmTcpConnection) -> Result<Box<BasicPeer>>;
}

/// A downstream connection that a [WasmTcpProxy] accepted.
#[derive(Debug)]
pub struct WasmTcpConnection {
    downstream: Stream,
    id: u64,
}

impl WasmTcpConnection {
    /// Return the downstream stream, e.g. to read the client address from its socket digest.
    pub fn downstream(&self) -> &Stream {
        &self.downstream
    }

    /// Return the id that plugins read as `connection_id`, unique within the process.
    pub fn id(&self) -> u64 {
        self.id
    }
}

/// A TCP proxy that runs a chain of plugins on each connection.
///
/// Add it to a Pingora `Service` with a TCP or TLS listener. For each connection, every plugin
/// runs `proxy_on_new_connection`, then [WasmTcpUpstream::upstream_peer] picks the upstream, and
/// the bytes of both directions pass through the plugins in `proxy_on_downstream_data` and
/// `proxy_on_upstream_data` until the connection ends. A plugin can change the bytes, pause
/// either direction, or close the connection. A plugin failure closes the connection or skips
/// the plugin, as [FailPolicy](crate::FailPolicy) describes.
///
/// ```no_run
/// # use async_trait::async_trait;
/// # use pingora_core::services::listening::Service;
/// # use pingora_core::upstreams::peer::BasicPeer;
/// # use pingora_wasm::{WasmPlugins, WasmTcpConnection, WasmTcpProxy, WasmTcpUpstream};
/// struct Database;
///
/// #[async_trait]
/// impl WasmTcpUpstream for Database {
///     async fn upstream_peer(
///         &self,
///         _connection: &WasmTcpConnection,
///     ) -> pingora_error::Result<Box<BasicPeer>> {
///         Ok(Box::new(BasicPeer::new("10.0.0.5:5432")))
///     }
/// }
///
/// # fn build(plugins: &WasmPlugins) -> pingora_error::Result<()> {
/// let proxy = WasmTcpProxy::new(plugins.chain("db")?, Database);
/// let mut service = Service::new("db".to_string(), proxy);
/// service.add_tcp("0.0.0.0:5433");
/// # Ok(())
/// # }
/// ```
pub struct WasmTcpProxy<U> {
    chain: WasmChainHandle,
    upstream: U,
    connector: TransportConnector,
    limits: ConnectionLimits,
}

impl<U: WasmTcpUpstream> WasmTcpProxy<U> {
    /// Create a proxy that runs `chain` on each connection and connects it to the peer that
    /// `upstream` returns.
    pub fn new(chain: WasmChainHandle, upstream: U) -> Self {
        WasmTcpProxy {
            chain,
            upstream,
            connector: TransportConnector::new(None),
            limits: ConnectionLimits {
                buffer_limit: 1024 * 1024,
                idle_timeout: Some(Duration::from_secs(60 * 60)),
                drain_timeout: Duration::from_secs(60),
            },
        }
    }

    /// Set the connector for upstream connections, e.g. one built with
    /// `ConnectorOptions::from_server_conf` to use the server's CA file. Default
    /// `TransportConnector::new(None)`.
    pub fn set_connector(&mut self, connector: TransportConnector) {
        self.connector = connector;
    }

    /// Set how many bytes of one direction the plugins can hold paused, and how many can wait to
    /// be written to the receiving side, before the proxy stops reading that direction. Default
    /// 1 MiB.
    pub fn set_buffer_limit(&mut self, limit: usize) {
        self.limits.buffer_limit = limit;
    }

    /// Set how long a connection can go without moving any bytes before it is closed, or `None`
    /// for no limit. Default one hour.
    pub fn set_idle_timeout(&mut self, timeout: Option<Duration>) {
        self.limits.idle_timeout = timeout;
    }

    /// Set how long a connection keeps running after its runtime begins to end, at a shutdown or
    /// after [WasmPlugins::replace](crate::WasmPlugins::replace). Default 60 seconds.
    ///
    /// Keep it shorter than Pingora's `grace_period_seconds` minus
    /// [shutdown_wait_limit](crate::WasmServices::shutdown_wait_limit), so that each plugin gets
    /// `proxy_on_done` before the server stops.
    pub fn set_drain_timeout(&mut self, timeout: Duration) {
        self.limits.drain_timeout = timeout;
    }
}

#[async_trait]
impl<U: WasmTcpUpstream + 'static> ServerApp for WasmTcpProxy<U> {
    async fn process_new(
        self: &Arc<Self>,
        downstream: Stream,
        _shutdown: &ShutdownWatch,
    ) -> Option<Stream> {
        let id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        let facts = connection_facts(&downstream, id);
        let plugins = TcpPlugins::new(self.chain.new_ctx(), facts);
        let connection = WasmTcpConnection { downstream, id };
        run_connection(plugins, connection, self.as_ref(), self.limits).await;
        None
    }
}

#[async_trait]
impl<U: WasmTcpUpstream> ConnectUpstream for WasmTcpProxy<U> {
    async fn connect(
        &self,
        connection: &WasmTcpConnection,
    ) -> Result<(Stream, Option<SocketAddr>)> {
        let peer = self.upstream.upstream_peer(connection).await?;
        let address = peer.address().as_inet().copied();
        let upstream = self.connector.new_stream(&*peer).await?;
        Ok((upstream, address))
    }
}

fn connection_facts(downstream: &Stream, id: u64) -> RequestFacts {
    let socket = downstream.get_socket_digest();
    let socket = socket.as_deref();
    let tls = downstream.get_ssl_digest();
    RequestFacts {
        client_address: socket.and_then(|s| s.peer_addr()?.as_inet().copied()),
        server_address: socket.and_then(|s| s.local_addr()?.as_inet().copied()),
        tls: tls.as_deref().map(TlsFacts::new),
        connection_id: Some(id),
        ..RequestFacts::default()
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{
        receive, send, stays_silent, tcp_plugin, ConnectOptions, TcpRuntime, UPSTREAM_ADDRESS,
    };
    use crate::test_support::callouts::FixedSender;
    use crate::test_support::{eventually, Wat};
    use std::sync::Arc;
    use tokio::sync::Notify;

    const PROPERTY_PATHS: &str = r#"
      (data (i32.const 900) "connection_id")
      (data (i32.const 920) "source\00address")
      (data (i32.const 940) "upstream\00address")
      (data (i32.const 960) "request\00path")"#;

    #[tokio::test]
    async fn connection_reads_its_facts_and_no_request_property() {
        let wat = Wat {
            downstream_data: Some(
                "(call $log_property (i32.const 900) (i32.const 13))
                 (call $log_property (i32.const 920) (i32.const 14))
                 (call $log_property (i32.const 940) (i32.const 16))
                 (call $log_property (i32.const 960) (i32.const 12))
                 (i32.const 0)",
            ),
            data_segments: PROPERTY_PATHS,
            ..Wat::default()
        };
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", wat)]);
        let mut connection = tcp.connect(ConnectOptions::default());

        send(&mut connection.client, "x").await;

        assert_eq!(receive(&mut connection.server, 1).await, "x");
        let id = String::from_utf8_lossy(&7_i64.to_le_bytes()).into_owned();
        let want = [id.as_str(), "127.0.0.1:5000", UPSTREAM_ADDRESS, "missing"];
        assert_eq!(tcp.logs(), want);
    }

    #[tokio::test]
    async fn tick_reads_open_connection_facts() {
        let wat = Wat {
            configure: "(drop (call $set_tick_period (i32.const 10))) (i32.const 1)",
            done: "i32.const 0",
            new_connection: Some("(i32.store (i32.const 980) (local.get 0)) (i32.const 0)"),
            tick: Some(
                "(if (i32.load (i32.const 980)) (then
                   (drop (call $set_effective_context (i32.load (i32.const 980))))
                   (call $log_property (i32.const 920) (i32.const 14))))",
            ),
            data_segments: PROPERTY_PATHS,
            ..Wat::default()
        };
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", wat)]);
        tcp.runtime.inner.start_threads().unwrap();
        let connection = tcp.connect(ConnectOptions::default());
        assert!(eventually(|| tcp.logs().contains(&"127.0.0.1:5000".to_string())).await);

        drop((connection.client, connection.server));

        assert!(eventually(|| tcp.logs().last().is_some_and(|line| line == "missing")).await);
    }

    #[tokio::test]
    async fn paused_new_connection_resumes_only_for_downstream() {
        for (resume, connects) in [("(i32.const 2)", true), ("(i32.const 3)", false)] {
            let delivery = format!("(call $continue {resume})");
            let wat = Wat {
                new_connection: Some("(call $call_authz_and_pause)"),
                http_call_response: Some(Box::leak(delivery.into_boxed_str())),
                ..Wat::default()
            };
            let gate = Arc::new(Notify::new());
            let sender = FixedSender::responds_after("allowed", gate.clone());
            let tcp = TcpRuntime::with_sender(vec![tcp_plugin("a", wat)], sender.clone());
            let mut connection = tcp.connect(ConnectOptions::default());
            send(&mut connection.client, "x").await;
            assert!(eventually(|| sender.sent_count() == 1).await);
            assert!(stays_silent(&mut connection.server).await);

            gate.notify_one();

            match connects {
                true => assert_eq!(receive(&mut connection.server, 1).await, "x"),
                false => assert!(stays_silent(&mut connection.server).await),
            }
        }
    }
}
