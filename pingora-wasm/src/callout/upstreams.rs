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

//! Callout upstreams

use async_trait::async_trait;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::{Error, ErrorType, Result};
use pingora_http::RequestHeader;
use std::collections::HashMap;

/// Resolver of callout upstream names to peers.
///
/// Every callout a plugin makes has an upstream name, such as `authz`. Implement this trait
/// to resolve those names with your own service discovery and load balancing, and set your
/// implementation as
/// [WasmServices::callout_upstreams](crate::WasmServices::callout_upstreams). If every upstream
/// has a single fixed peer, you can use [StaticCalloutUpstreams] instead.
///
/// For example, to pick a load balancer backend by the callout's `host` header:
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
///     fn has_upstream(&self, _plugin_name: &str, upstream_name: &str) -> bool {
///         self.upstreams.contains_key(upstream_name)
///     }
///
///     async fn callout_peer(&self, target: &CalloutTarget<'_>) -> Result<Box<HttpPeer>> {
///         let host = &target.request.headers["host"];
///         let backend = self
///             .upstreams
///             .get(target.upstream_name)
///             .and_then(|balancer| balancer.select(host.as_bytes(), 256))
///             .ok_or_else(|| Error::explain(ErrorType::ConnectNoRoute, "no healthy backend"))?;
///         Ok(Box::new(HttpPeer::new(backend, false, String::new())))
///     }
/// }
/// ```
#[async_trait]
pub trait CalloutUpstreams: Send + Sync {
    /// Return whether the plugin may send callouts to the upstream.
    ///
    /// This is called from inside the plugin's `proxy_http_call`, `proxy_grpc_call`, or
    /// `proxy_grpc_stream`, so it must not block. If it returns `false`, no callout is sent, and
    /// the plugin gets `BAD_ARGUMENT` from `proxy_http_call` or `PARSE_FAILURE` from
    /// `proxy_grpc_call` and `proxy_grpc_stream`.
    fn has_upstream(&self, plugin_name: &str, upstream_name: &str) -> bool;

    /// Select the peer for one callout.
    ///
    /// The time spent here counts against the timeout of an HTTP callout or a gRPC call, and the
    /// future is dropped if that timeout expires. Return an error if the upstream has no peer to
    /// offer, e.g. because every backend is unhealthy. The plugin then gets a 503 response with
    /// the body `no healthy upstream`, or the gRPC status `UNAVAILABLE` for a gRPC callout.
    ///
    /// `HttpPeer::new` resolves a hostname with a blocking call, so build your peers before the
    /// server starts or pass an IP address. A TLS peer needs one of this crate's TLS features,
    /// such as `openssl` or `rustls`, to be enabled. Without one, the callout fails.
    async fn callout_peer(&self, target: &CalloutTarget<'_>) -> Result<Box<HttpPeer>>;
}

/// The callout a peer is being selected for.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub struct CalloutTarget<'a> {
    /// The name of the plugin that made the callout.
    pub plugin_name: &'a str,
    /// The upstream name the plugin passed with the callout.
    ///
    /// A plugin can also pass a serialized `GrpcService` protobuf message to `proxy_grpc_call` or
    /// `proxy_grpc_stream`. The name is then the cluster name or the target URI in that message.
    pub upstream_name: &'a str,
    /// The request header of the callout. For an HTTP callout, its `host` header holds the
    /// `:authority` the plugin passed, which you can use as a load balancing key. For a gRPC
    /// callout, it holds the upstream name.
    pub request: &'a RequestHeader,
}

impl<'a> CalloutTarget<'a> {
    /// Create a target, e.g. to test your own [CalloutUpstreams].
    pub fn new(plugin_name: &'a str, upstream_name: &'a str, request: &'a RequestHeader) -> Self {
        CalloutTarget {
            plugin_name,
            upstream_name,
            request,
        }
    }
}

/// A fixed map of upstream names to peers.
///
/// Every plugin in the runtime may send callouts to every upstream in the map.
#[derive(Debug, Clone, Default)]
pub struct StaticCalloutUpstreams {
    peers: HashMap<String, HttpPeer>,
}

impl StaticCalloutUpstreams {
    /// Create an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an upstream, or replace the peer of one already in the map.
    pub fn insert(&mut self, upstream_name: impl Into<String>, peer: HttpPeer) -> Option<HttpPeer> {
        self.peers.insert(upstream_name.into(), peer)
    }

    /// Return the peer of an upstream.
    pub fn peer(&self, upstream_name: &str) -> Option<&HttpPeer> {
        self.peers.get(upstream_name)
    }
}

#[async_trait]
impl CalloutUpstreams for StaticCalloutUpstreams {
    fn has_upstream(&self, _plugin_name: &str, upstream_name: &str) -> bool {
        self.peers.contains_key(upstream_name)
    }

    async fn callout_peer(&self, target: &CalloutTarget<'_>) -> Result<Box<HttpPeer>> {
        match self.peer(target.upstream_name) {
            Some(peer) => Ok(Box::new(peer.clone())),
            None => Error::e_explain(
                ErrorType::ConnectNoRoute,
                format!(
                    "no peer configured for callout upstream {}",
                    target.upstream_name
                ),
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
    fn insert_returns_replaced_peer() {
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
    async fn static_upstreams_resolve_only_inserted_names() {
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
