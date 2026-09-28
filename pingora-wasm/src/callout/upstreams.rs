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

//! The upstreams that plugins can send callouts to.

use async_trait::async_trait;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::{Error, ErrorType, Result};
use pingora_http::RequestHeader;
use std::collections::HashMap;

/// The interface that maps the upstream names of callouts to peers.
///
/// A plugin passes an upstream name, such as `authz`, with each callout. Envoy calls this name
/// a cluster. Implement this trait to resolve the names with your own service discovery and
/// load balancing, and set it as
/// [WasmServices::callout_upstreams](crate::WasmServices::callout_upstreams). If each upstream
/// has one fixed peer, use [StaticCalloutUpstreams].
///
/// For example, to select a backend of a load balancer by the `host` of the callout:
///
/// ```
/// use async_trait::async_trait;
/// use pingora_core::upstreams::peer::HttpPeer;
/// use pingora_core::{Error, ErrorType, Result};
/// use pingora_load_balancing::selection::Consistent;
/// use pingora_load_balancing::LoadBalancer;
/// use pingora_wasm::{CalloutTarget, CalloutUpstreams};
/// use std::collections::HashMap;
///
/// struct Balanced {
///     upstreams: HashMap<String, LoadBalancer<Consistent>>,
/// }
///
/// #[async_trait]
/// impl CalloutUpstreams for Balanced {
///     fn has_upstream(&self, _plugin: &str, upstream: &str) -> bool {
///         self.upstreams.contains_key(upstream)
///     }
///
///     async fn callout_peer(&self, target: &CalloutTarget<'_>) -> Result<Box<HttpPeer>> {
///         let host = &target.request.headers["host"];
///         let backend = self
///             .upstreams
///             .get(target.upstream)
///             .and_then(|balancer| balancer.select(host.as_bytes(), 256))
///             .ok_or_else(|| Error::explain(ErrorType::ConnectNoRoute, "no healthy backend"))?;
///         Ok(Box::new(HttpPeer::new(backend, false, String::new())))
///     }
/// }
/// ```
#[async_trait]
pub trait CalloutUpstreams: Send + Sync {
    /// Return whether `plugin` can send callouts to `upstream`.
    ///
    /// The runtime calls this method inside the plugin's call to `proxy_http_call`, so it must
    /// not block. When it returns `false`, the plugin receives `BAD_ARGUMENT` and no callout is
    /// sent.
    fn has_upstream(&self, plugin: &str, upstream: &str) -> bool;

    /// Select the peer for one callout.
    ///
    /// The runtime calls this method in the task that sends the callout, after
    /// [Self::has_upstream] returned `true`. The time it takes counts against the timeout of the
    /// callout, and the runtime drops the future when that timeout expires.
    ///
    /// `HttpPeer::new` resolves a host name with a blocking call, so build your peers before the
    /// server starts or pass an IP address. A peer with TLS needs a TLS feature of this crate,
    /// such as `openssl` or `rustls`. Without one, the callout fails at its timeout.
    ///
    /// # Errors
    ///
    /// Return an error when the upstream has no peer to offer, for example when every backend
    /// is unhealthy. The plugin then receives a 503 response with the body
    /// `no healthy upstream`.
    async fn callout_peer(&self, target: &CalloutTarget<'_>) -> Result<Box<HttpPeer>>;
}

/// A callout that needs a peer.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct CalloutTarget<'a> {
    /// The name of the plugin that sent the callout.
    pub plugin: &'a str,
    /// The upstream name that the plugin passed.
    pub upstream: &'a str,
    /// The request header of the callout. Its `host` header has the `:authority` that the
    /// plugin passed, which you can use as a load balancing key.
    pub request: &'a RequestHeader,
}

impl<'a> CalloutTarget<'a> {
    /// Create a target, for example to test your own [CalloutUpstreams].
    ///
    /// A field that a later version adds has a default value here.
    pub fn new(plugin: &'a str, upstream: &'a str, request: &'a RequestHeader) -> Self {
        CalloutTarget {
            plugin,
            upstream,
            request,
        }
    }
}

/// A fixed map from upstream names to peers.
///
/// Every plugin of the runtime can send callouts to every upstream in the map.
#[derive(Debug, Clone, Default)]
pub struct StaticCalloutUpstreams {
    peers: HashMap<String, HttpPeer>,
}

impl StaticCalloutUpstreams {
    /// Create an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an upstream, and return the peer that it replaced.
    pub fn insert(&mut self, upstream: impl Into<String>, peer: HttpPeer) -> Option<HttpPeer> {
        self.peers.insert(upstream.into(), peer)
    }

    /// Return the peer of an upstream.
    pub fn peer(&self, upstream: &str) -> Option<&HttpPeer> {
        self.peers.get(upstream)
    }
}

#[async_trait]
impl CalloutUpstreams for StaticCalloutUpstreams {
    fn has_upstream(&self, _plugin: &str, upstream: &str) -> bool {
        self.peers.contains_key(upstream)
    }

    async fn callout_peer(&self, target: &CalloutTarget<'_>) -> Result<Box<HttpPeer>> {
        match self.peer(target.upstream) {
            Some(peer) => Ok(Box::new(peer.clone())),
            None => Error::e_explain(
                ErrorType::ConnectNoRoute,
                format!("callout upstream {} has no peer", target.upstream),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(port: u16) -> HttpPeer {
        HttpPeer::new(("127.0.0.1", port), false, String::new())
    }

    #[test]
    fn insert_returns_the_peer_that_it_replaced() {
        let mut upstreams = StaticCalloutUpstreams::new();
        let first = upstreams.insert("authz", peer(8181));

        let replaced = upstreams.insert("authz", peer(8282));

        assert!(first.is_none());
        assert_eq!(replaced.unwrap().to_string(), peer(8181).to_string());
        assert_eq!(
            upstreams.peer("authz").unwrap().to_string(),
            peer(8282).to_string()
        );
        assert!(upstreams.peer("audit").is_none());
    }

    #[tokio::test]
    async fn static_upstreams_return_the_peer_of_an_upstream_in_the_map() {
        let mut upstreams = StaticCalloutUpstreams::new();
        upstreams.insert("authz", peer(8181));
        let request = RequestHeader::build("GET", b"/", None).unwrap();
        let cases = ["authz", "audit"];

        for (upstream, in_the_map) in cases.into_iter().zip([true, false]) {
            let target = CalloutTarget::new("a", upstream, &request);

            let selected = upstreams.callout_peer(&target).await;

            assert_eq!(upstreams.has_upstream("a", upstream), in_the_map);
            assert_eq!(selected.is_ok(), in_the_map);
        }
    }
}
