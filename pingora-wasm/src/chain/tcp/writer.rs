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

//! Writer task for one side of a TCP connection

use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

enum WriterCommand {
    Data(Vec<u8>),
    Shutdown,
}

/// A task that writes the bytes of one side, so that a peer that does not read stops only the
/// direction it receives.
pub(super) struct SideWriter {
    commands: Option<mpsc::UnboundedSender<WriterCommand>>,
    queued: Arc<AtomicUsize>,
    wrote: Arc<Notify>,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl SideWriter {
    pub(super) fn spawn<W>(mut writer: W) -> Self
    where
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (commands, mut received) = mpsc::unbounded_channel();
        let queued = Arc::new(AtomicUsize::new(0));
        let written = queued.clone();
        let wrote = Arc::new(Notify::new());
        let notify = wrote.clone();
        let task = tokio::spawn(async move {
            while let Some(command) = received.recv().await {
                match command {
                    WriterCommand::Data(bytes) => {
                        writer.write_all(&bytes).await?;
                        writer.flush().await?;
                        written.fetch_sub(bytes.len(), Ordering::Relaxed);
                        notify.notify_one();
                    }
                    WriterCommand::Shutdown => return writer.shutdown().await,
                }
            }
            writer.flush().await
        });
        SideWriter {
            commands: Some(commands),
            queued,
            wrote,
            task: Some(task),
        }
    }

    pub(super) fn send(&mut self, bytes: Vec<u8>) {
        if bytes.is_empty() {
            return;
        }
        let Some(commands) = &self.commands else {
            return;
        };
        self.queued.fetch_add(bytes.len(), Ordering::Relaxed);
        let _ = commands.send(WriterCommand::Data(bytes));
    }

    /// Shut down the writing half after the bytes sent so far.
    pub(super) fn shutdown(&mut self) {
        if let Some(commands) = self.commands.take() {
            let _ = commands.send(WriterCommand::Shutdown);
        }
    }

    pub(super) fn queued(&self) -> usize {
        self.queued.load(Ordering::Relaxed)
    }

    /// Return the signal the task gives each time it has written bytes.
    pub(super) fn write_signal(&self) -> Arc<Notify> {
        self.wrote.clone()
    }

    pub(super) fn has_ended(&self) -> bool {
        self.task.is_none()
    }

    /// Wait for the task to end, and return whether it failed. Once it has ended, this never
    /// returns, so a `select!` branch on it does not fire twice.
    pub(super) async fn wait_for_end(&mut self) -> bool {
        let Some(task) = &mut self.task else {
            return std::future::pending().await;
        };
        let failed = !matches!(task.await, Ok(Ok(())));
        self.task = None;
        failed
    }

    /// Write the bytes sent so far and close, waiting at most `limit`.
    pub(super) async fn close(mut self, limit: std::time::Duration) {
        self.commands = None;
        if let Some(task) = self.task.take() {
            let abort = task.abort_handle();
            if tokio::time::timeout(limit, task).await.is_err() {
                abort.abort();
            }
        }
    }
}
