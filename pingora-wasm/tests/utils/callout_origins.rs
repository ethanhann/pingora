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

//! The origins that the callouts of the test plugins go to.

use async_trait::async_trait;
use once_cell::sync::Lazy;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_core::{Error, ErrorType, Result};
use pingora_wasm::{CalloutTarget, CalloutUpstreams};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Runtime;

static ORIGIN_RUNTIME: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

/// An origin that records each request, and responds with a fixed body or not at all.
pub struct CalloutOrigin {
    addr: SocketAddr,
    /// The path and the `host` of each request.
    requests: Mutex<Vec<(String, String)>>,
    body: Option<&'static str>,
}

impl CalloutOrigin {
    /// Start an origin that responds with `body`, or that never responds.
    fn start(body: Option<&'static str>) -> Arc<Self> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = Arc::new(CalloutOrigin {
            addr: listener.local_addr().unwrap(),
            requests: Mutex::new(Vec::new()),
            body,
        });
        let accepting = origin.clone();
        ORIGIN_RUNTIME.spawn(async move {
            let listener = TcpListener::from_std(listener).unwrap();
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(accepting.clone().respond(stream));
            }
        });
        origin
    }

    async fn respond(self: Arc<Self>, mut stream: TcpStream) {
        let mut request = Vec::new();
        let mut part = [0u8; 1024];
        while !request.ends_with(b"\r\n\r\n") {
            match stream.read(&mut part).await {
                Ok(n) if n > 0 => request.extend_from_slice(&part[..n]),
                _ => return,
            }
        }
        let head = String::from_utf8_lossy(&request).to_ascii_lowercase();
        let path = head.split(' ').nth(1).unwrap_or_default().to_string();
        let host = head
            .lines()
            .find_map(|line| line.strip_prefix("host: "))
            .unwrap_or_default()
            .to_string();
        self.requests.lock().unwrap().push((path, host));
        let Some(body) = self.body else {
            return std::future::pending().await;
        };
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
    }

    /// Return the path and the `host` of each request so far.
    pub fn requests(&self) -> Vec<(String, String)> {
        self.requests.lock().unwrap().clone()
    }

    /// Wait until the first request arrives.
    ///
    /// # Panics
    ///
    /// Panics when no request arrives in five seconds.
    pub async fn wait_for_a_request(&self) {
        let arrived = super::eventually(|| !self.requests().is_empty()).await;
        assert!(arrived, "no callout arrived at the origin");
    }
}

/// Callout upstreams that send the callouts of each plugin to its own origin, whatever the
/// upstream name.
pub struct CalloutOriginPerPlugin {
    origins: HashMap<&'static str, Arc<CalloutOrigin>>,
}

impl CalloutOriginPerPlugin {
    /// Start one origin for each plugin name. An origin with no body never responds.
    pub fn start(plugins: &[(&'static str, Option<&'static str>)]) -> Arc<Self> {
        let origins = plugins
            .iter()
            .map(|(plugin, body)| (*plugin, CalloutOrigin::start(*body)))
            .collect();
        Arc::new(CalloutOriginPerPlugin { origins })
    }

    pub fn origin(&self, plugin: &str) -> Arc<CalloutOrigin> {
        self.origins[plugin].clone()
    }
}

#[async_trait]
impl CalloutUpstreams for CalloutOriginPerPlugin {
    fn has_upstream(&self, plugin: &str, _upstream: &str) -> bool {
        self.origins.contains_key(plugin)
    }

    async fn callout_peer(&self, target: &CalloutTarget<'_>) -> Result<Box<HttpPeer>> {
        match self.origins.get(target.plugin) {
            Some(origin) => Ok(Box::new(HttpPeer::new(origin.addr, false, String::new()))),
            None => Error::e_explain(ErrorType::InternalError, "the plugin has no origin"),
        }
    }
}
