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

//! Copy loop of a TCP connection

use super::direction::Direction;
use super::plugins::TcpPlugins;
use super::writer::SideWriter;
use super::WasmTcpConnection;
use crate::runtime::RuntimeInner;
use async_trait::async_trait;
use log::{debug, error};
use pingora_core::protocols::Stream;
use pingora_error::Result;
use proxy_wasm_host::abi::v0_2_1::types::PeerType;
use std::future::pending;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, ReadHalf};
use tokio::sync::Notify;

const READ_SIZE: usize = 16 * 1024;

/// How long each side has to write its last bytes after a close, when no idle timeout is set.
const CLOSE_WRITE_LIMIT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy)]
pub(super) struct ConnectionLimits {
    pub(super) buffer_limit: usize,
    pub(super) idle_timeout: Option<Duration>,
    pub(super) drain_timeout: Duration,
}

/// Upstream connector of the copy loop, a trait so that tests can connect to in-memory streams.
#[async_trait]
pub(super) trait ConnectUpstream: Send + Sync {
    async fn connect(&self, connection: &WasmTcpConnection)
        -> Result<(Stream, Option<SocketAddr>)>;
}

#[derive(Clone, Copy)]
struct Timers {
    limits: ConnectionLimits,
    last_activity: Instant,
    drain_deadline: Option<Instant>,
}

impl Timers {
    fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    fn idle_deadline(&self) -> Option<Instant> {
        let idle = self.limits.idle_timeout?;
        self.last_activity.checked_add(idle)
    }

    fn start_drain(&mut self) {
        self.drain_deadline = Instant::now().checked_add(self.limits.drain_timeout);
    }

    /// Wait until the connection must close for the drain or the idle timeout.
    async fn expired(&mut self, runtime: &RuntimeInner) {
        if self.drain_deadline.is_none() {
            tokio::select! {
                _ = runtime.lifecycle.wait_for_end_to_begin() => self.start_drain(),
                _ = sleep_until(self.idle_deadline()) => return,
            }
        }
        tokio::select! {
            _ = sleep_until(self.drain_deadline) => {}
            _ = sleep_until(self.idle_deadline()) => {}
        }
    }
}

/// Run the plugins and copy bytes in both directions until the connection ends, then run the
/// close callbacks and end each plugin's context.
pub(super) async fn run_connection<C: ConnectUpstream>(
    mut plugins: TcpPlugins,
    connection: WasmTcpConnection,
    connector: &C,
    limits: ConnectionLimits,
) {
    let mut timers = Timers {
        limits,
        last_activity: Instant::now(),
        drain_deadline: None,
    };
    let runtime = plugins.runtime().clone();
    let opened = match plugins.start() {
        Ok(()) => wait_for_new_connection(&mut plugins, &mut timers, &runtime).await,
        Err(e) => Err(e),
    };
    match opened {
        Ok(true) if !plugins.close_requested() => {}
        Ok(_) => return close_before_upstream(plugins, connection),
        Err(e) => {
            error!("wasm TCP connection: closed after plugin failure: {e}");
            return close_before_upstream(plugins, connection);
        }
    }
    let connected = tokio::select! {
        connected = connector.connect(&connection) => connected,
        _ = timers.expired(&runtime) => return close_before_upstream(plugins, connection),
    };
    match connected {
        Ok((upstream, address)) => {
            plugins.upstream_connected(address);
            copy(plugins, connection.downstream, upstream, timers, &runtime).await;
        }
        Err(e) => {
            error!("wasm TCP connection: failed to connect to upstream: {e}");
            close_before_upstream(plugins, connection);
        }
    }
}

fn close_before_upstream(mut plugins: TcpPlugins, connection: WasmTcpConnection) {
    drop(connection);
    if let Err(e) = plugins.run_close(Direction::Downstream, PeerType::Local) {
        error!("wasm TCP connection: plugin failure while closing: {e}");
    }
    plugins.end_contexts();
}

/// Deliver callout results until every plugin has continued `proxy_on_new_connection`, and
/// return `false` if the connection closes first.
async fn wait_for_new_connection(
    plugins: &mut TcpPlugins,
    timers: &mut Timers,
    runtime: &RuntimeInner,
) -> Result<bool> {
    while !plugins.new_connection_done() {
        if plugins.close_requested() {
            return Ok(false);
        }
        let wait_deadline = plugins.next_wait_deadline();
        let mut expiry = *timers;
        tokio::select! {
            (position, id, delivery) = plugins.next_callout() => {
                plugins.deliver(position, id, &delivery)?;
            }
            _ = sleep_until(wait_deadline) => plugins.expire_waits(Instant::now())?,
            _ = expiry.expired(runtime) => return Ok(false),
        }
        timers.drain_deadline = timers.drain_deadline.or(expiry.drain_deadline);
    }
    Ok(true)
}

struct Side {
    reader: ReadHalf<Stream>,
    writer: SideWriter,
    write_signal: Arc<Notify>,
    buffer: Vec<u8>,
    read_done: bool,
    /// Whether the peer ended or reset this side before the proxy closed it.
    ended_by_peer: bool,
    close_callback_ran: bool,
}

impl Side {
    fn new(stream: Stream) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        let writer = SideWriter::spawn(writer);
        Side {
            reader,
            write_signal: writer.write_signal(),
            writer,
            buffer: vec![0; READ_SIZE],
            read_done: false,
            ended_by_peer: false,
            close_callback_ran: false,
        }
    }

    fn peer(&self) -> PeerType {
        match self.ended_by_peer {
            true => PeerType::Remote,
            false => PeerType::Local,
        }
    }

    fn closed(&self) -> bool {
        self.read_done && self.writer.has_ended()
    }

    /// Pass a read result to the plugins, and return `true` if the loop should stop. A read of
    /// zero bytes ends the direction, and a read error ends the connection.
    fn on_read(
        &mut self,
        plugins: &mut TcpPlugins,
        timers: &mut Timers,
        direction: Direction,
        read: std::io::Result<usize>,
    ) -> Result<bool> {
        match read {
            Ok(0) => {
                self.read_done = true;
                self.ended_by_peer = true;
                plugins.push(direction, Vec::new(), true)?;
                Ok(false)
            }
            Ok(size) => {
                timers.touch();
                plugins.push(direction, self.buffer[..size].to_vec(), false)?;
                Ok(false)
            }
            Err(e) => {
                self.read_done = true;
                self.ended_by_peer = true;
                debug!(
                    "wasm TCP connection: failed to read from {}: {e}",
                    direction.name()
                );
                Ok(true)
            }
        }
    }
}

/// Why the copy loop stopped.
enum Stop {
    /// Both sides closed, or a plugin, a peer, or a failure closed the connection.
    Closed,
    /// The drain time or the idle timeout ran out, so the last bytes are not waited for.
    Expired,
}

async fn copy(
    mut plugins: TcpPlugins,
    downstream: Stream,
    upstream: Stream,
    mut timers: Timers,
    runtime: &RuntimeInner,
) {
    let limit = timers.limits.buffer_limit;
    let mut down = Side::new(downstream);
    let mut up = Side::new(upstream);
    let stop = loop {
        let mut outcome = Ok(false);
        if down.read_done && up.read_done {
            outcome = plugins.fail_stalled_pauses().map(|()| false);
        }
        let to_upstream = plugins.take_output(Direction::Downstream);
        let to_downstream = plugins.take_output(Direction::Upstream);
        if !to_upstream.is_empty() || !to_downstream.is_empty() {
            timers.touch();
        }
        up.writer.send(to_upstream);
        down.writer.send(to_downstream);
        if plugins.ended(Direction::Downstream) {
            up.writer.shutdown();
        }
        if plugins.ended(Direction::Upstream) {
            down.writer.shutdown();
        }
        if plugins.close_requested() {
            break Stop::Closed;
        }
        if outcome.is_ok() && up.closed() && !up.close_callback_ran {
            up.close_callback_ran = true;
            outcome = plugins
                .run_close(Direction::Upstream, up.peer())
                .map(|()| false);
        }
        if outcome.is_ok() && down.closed() && !down.close_callback_ran {
            down.close_callback_ran = true;
            outcome = plugins
                .run_close(Direction::Downstream, down.peer())
                .map(|()| false);
        }
        if up.close_callback_ran && down.close_callback_ran {
            break Stop::Closed;
        }
        let read_down = !down.read_done
            && plugins.held_bytes(Direction::Downstream) < limit
            && up.writer.queued() < limit;
        let read_up = !up.read_done
            && plugins.held_bytes(Direction::Upstream) < limit
            && down.writer.queued() < limit;
        let wait_deadline = plugins.next_wait_deadline();
        let mut expiry = timers;
        if outcome.is_ok() {
            outcome = tokio::select! {
                read = down.reader.read(&mut down.buffer), if read_down => {
                    down.on_read(&mut plugins, &mut timers, Direction::Downstream, read)
                }
                read = up.reader.read(&mut up.buffer), if read_up => {
                    up.on_read(&mut plugins, &mut timers, Direction::Upstream, read)
                }
                (position, id, delivery) = plugins.next_callout() => {
                    plugins.deliver(position, id, &delivery).map(|()| false)
                }
                _ = sleep_until(wait_deadline) => plugins.expire_waits(Instant::now()).map(|()| false),
                // A full write queue that drains lets the loop read that direction again
                _ = up.write_signal.notified(), if up.writer.queued() >= limit => Ok(false),
                _ = down.write_signal.notified(), if down.writer.queued() >= limit => Ok(false),
                failed = down.writer.wait_for_end() => {
                    down.ended_by_peer |= failed;
                    Ok(failed)
                }
                failed = up.writer.wait_for_end() => {
                    up.ended_by_peer |= failed;
                    Ok(failed)
                }
                _ = expiry.expired(runtime) => break Stop::Expired,
            };
            timers.drain_deadline = timers.drain_deadline.or(expiry.drain_deadline);
        }
        match outcome {
            Ok(false) => {}
            Ok(true) => break Stop::Closed,
            Err(e) => {
                error!("wasm TCP connection: closed after plugin failure: {e}");
                break Stop::Closed;
            }
        }
    };
    let write_limit = match stop {
        Stop::Expired => Duration::ZERO,
        Stop::Closed => timers.limits.idle_timeout.unwrap_or(CLOSE_WRITE_LIMIT),
    };
    // The stream of a side closes once both of its halves are dropped
    let (down_writer, up_writer) = (down.writer, up.writer);
    drop((down.reader, up.reader));
    futures::join!(down_writer.close(write_limit), up_writer.close(write_limit));
    for (side, peer, ran) in [
        (Direction::Upstream, up.ended_by_peer, up.close_callback_ran),
        (
            Direction::Downstream,
            down.ended_by_peer,
            down.close_callback_ran,
        ),
    ] {
        let peer = if peer {
            PeerType::Remote
        } else {
            PeerType::Local
        };
        if !ran {
            if let Err(e) = plugins.run_close(side, peer) {
                error!("wasm TCP connection: plugin failure while closing: {e}");
            }
        }
    }
    plugins.end_contexts();
}

async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => pending().await,
    }
}

#[cfg(test)]
mod tests {
    use crate::chain::tcp::testing::{
        receive, receive_to_end, send, tcp_plugin, ConnectOptions, TcpRuntime,
        LOG_DOWNSTREAM_CLOSE, LOG_END, LOG_LOG, LOG_UPSTREAM_CLOSE,
    };
    use crate::test_support::{eventually, Wat};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn logs_closes(wat: Wat) -> Wat {
        Wat {
            downstream_close: Some(LOG_DOWNSTREAM_CLOSE),
            upstream_close: Some(LOG_UPSTREAM_CLOSE),
            log: Some(LOG_LOG),
            ..wat
        }
    }

    #[tokio::test]
    async fn both_directions_move_large_payloads_at_once() {
        let wat = Wat {
            downstream_data: Some("i32.const 0"),
            upstream_data: Some("i32.const 0"),
            ..Wat::default()
        };
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", wat)]);
        let options = ConnectOptions {
            stream_buffer: 1024,
            ..ConnectOptions::default()
        };
        let connection = tcp.connect(options);
        let payload = vec![b'x'; 256 * 1024];
        let (mut client_reader, mut client_writer) = tokio::io::split(connection.client);
        let (mut server_reader, mut server_writer) = tokio::io::split(connection.server);
        let (mut at_client, mut at_server) = (vec![0; payload.len()], vec![0; payload.len()]);

        let moved = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::try_join!(
                client_writer.write_all(&payload),
                server_writer.write_all(&payload),
                client_reader.read_exact(&mut at_client),
                server_reader.read_exact(&mut at_server),
            )
        })
        .await;

        assert!(matches!(moved, Ok(Ok(_))), "{moved:?}");
        assert_eq!((at_client == payload, at_server == payload), (true, true));
    }

    #[tokio::test]
    async fn half_close_reaches_plugin_and_other_direction_still_flows() {
        for client_ends_first in [true, false] {
            let wat = logs_closes(Wat {
                downstream_data: Some(LOG_END),
                upstream_data: Some(LOG_END),
                ..Wat::default()
            });
            let tcp = TcpRuntime::new(vec![tcp_plugin("a", wat)]);
            let mut connection = tcp.connect(ConnectOptions::default());
            let (first, second) = match client_ends_first {
                true => (&mut connection.client, &mut connection.server),
                false => (&mut connection.server, &mut connection.client),
            };
            send(first, "q").await;
            first.shutdown().await.unwrap();
            assert_eq!(receive_to_end(second).await, "q");
            send(second, "r").await;

            second.shutdown().await.unwrap();

            assert_eq!(receive_to_end(first).await, "r");
            connection.ended().await;
            let (first_close, second_close) = match client_ends_first {
                true => ("upstream remote", "downstream remote"),
                false => ("downstream remote", "upstream remote"),
            };
            let want = ["end", "end", first_close, second_close, "log"];
            assert_eq!(tcp.logs(), want, "client ends first: {client_ends_first}");
        }
    }

    #[tokio::test]
    async fn close_stream_writes_callback_data_then_closes_both_sides() {
        let close_downstream = logs_closes(Wat {
            downstream_data: Some(
                "(drop (call $close_stream (i32.const 2))) (call $mark_a (i32.const 2))",
            ),
            ..Wat::default()
        });
        let close_upstream = logs_closes(Wat {
            upstream_data: Some(
                "(drop (call $close_stream (i32.const 3))) (call $mark_a (i32.const 3))",
            ),
            ..Wat::default()
        });

        for (wat, from_client) in [(close_downstream, true), (close_upstream, false)] {
            let tcp = TcpRuntime::new(vec![tcp_plugin("a", wat)]);
            let mut connection = tcp.connect(ConnectOptions::default());
            let (sender, receiver) = match from_client {
                true => (&mut connection.client, &mut connection.server),
                false => (&mut connection.server, &mut connection.client),
            };

            send(sender, "x").await;

            assert_eq!(receive_to_end(receiver).await, "ax");
            assert_eq!(receive_to_end(sender).await, "");
            connection.ended().await;
            let want = ["upstream local", "downstream local", "log"];
            assert_eq!(tcp.logs(), want, "close from client data: {from_client}");
        }
    }

    #[tokio::test]
    async fn idle_timeout_closes_both_sides() {
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", logs_closes(Wat::default()))]);
        let mut options = ConnectOptions::default();
        options.limits.idle_timeout = Some(Duration::from_millis(50));
        let mut connection = tcp.connect(options);

        let at_client = receive_to_end(&mut connection.client).await;

        assert_eq!(at_client, "");
        assert_eq!(receive_to_end(&mut connection.server).await, "");
        connection.ended().await;
        assert_eq!(tcp.logs(), ["upstream local", "downstream local", "log"]);
    }

    #[tokio::test]
    async fn failed_connect_closes_downstream_with_only_its_close_callback() {
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", logs_closes(Wat::default()))]);
        let options = ConnectOptions {
            connects: false,
            ..ConnectOptions::default()
        };
        let mut connection = tcp.connect(options);

        let at_client = receive_to_end(&mut connection.client).await;

        assert_eq!(at_client, "");
        connection.ended().await;
        assert_eq!(tcp.logs(), ["downstream local", "log"]);
    }

    #[tokio::test]
    async fn ending_runtime_closes_connection_after_drain() {
        let wat = logs_closes(Wat {
            done: "(call $log_text (i32.const 810) (i32.const 4)) (i32.const 1)",
            data_segments: r#"(data (i32.const 810) "done")"#,
            ..Wat::default()
        });
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", wat)]);
        tcp.runtime.inner.start_threads().unwrap();
        let mut options = ConnectOptions::default();
        options.limits.drain_timeout = Duration::from_millis(50);
        let mut connection = tcp.connect(options);
        send(&mut connection.client, "x").await;
        assert_eq!(receive(&mut connection.server, 1).await, "x");
        let inner = tcp.runtime.inner.clone();

        let ended = tokio::time::timeout(Duration::from_secs(5), inner.end()).await;

        assert!(ended.is_ok());
        assert_eq!(receive_to_end(&mut connection.client).await, "");
        let root_done_last = ["upstream local", "downstream local", "done", "log", "done"];
        assert_eq!(tcp.logs(), root_done_last);
    }

    #[tokio::test]
    async fn dropped_connection_task_ends_contexts() {
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", Wat::default())]);
        let mut connection = tcp.connect(ConnectOptions::default());
        send(&mut connection.client, "x").await;
        assert_eq!(receive(&mut connection.server, 1).await, "x");

        connection.task.abort();

        assert!(eventually(|| tcp.runtime.open_contexts() == 0).await);
    }

    #[tokio::test]
    async fn drain_does_not_wait_for_unread_bytes() {
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", Wat::default())]);
        let mut options = ConnectOptions {
            stream_buffer: 1024,
            ..ConnectOptions::default()
        };
        options.limits.drain_timeout = Duration::from_millis(50);
        let connection = tcp.connect(options);
        let mut client = connection.client;
        let _writes = tokio::spawn(async move { client.write_all(&[b'x'; 64 * 1024]).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let inner = tcp.runtime.inner.clone();

        let ended = tokio::time::timeout(Duration::from_secs(2), inner.end()).await;

        assert!(ended.is_ok(), "the runtime waited for the unread bytes");
    }

    #[tokio::test]
    async fn close_from_new_connection_does_not_connect_upstream() {
        let wat = logs_closes(Wat {
            new_connection: Some("(drop (call $close_stream (i32.const 2))) (i32.const 0)"),
            ..Wat::default()
        });
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", wat)]);
        let mut connection = tcp.connect(ConnectOptions::default());

        let at_client = receive_to_end(&mut connection.client).await;

        assert_eq!(at_client, "");
        connection.ended().await;
        assert_eq!(tcp.logs(), ["downstream local", "log"]);
    }

    #[tokio::test]
    async fn dropped_upstream_gives_remote_close() {
        let tcp = TcpRuntime::new(vec![tcp_plugin("a", logs_closes(Wat::default()))]);
        let mut connection = tcp.connect(ConnectOptions::default());
        send(&mut connection.client, "a").await;
        assert_eq!(receive(&mut connection.server, 1).await, "a");
        drop(connection.server);

        send(&mut connection.client, "b").await;

        assert_eq!(receive_to_end(&mut connection.client).await, "");
        assert!(eventually(|| tcp.logs().len() == 3).await);
        assert_eq!(tcp.logs(), ["upstream remote", "downstream local", "log"]);
    }
}
