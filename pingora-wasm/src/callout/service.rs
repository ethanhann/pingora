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

//! Per-guest callout service

use super::headers::callout_request_header;
use super::{AcceptedCallout, PluginCalloutConf};
use bytes::Bytes;
use log::{debug, warn};
use parking_lot::Mutex;
use proxy_wasm_host::abi::v0_2_1::{
    CalloutId, Callouts, ContextId, HttpCall, HttpCallRefusal, Invocation,
};
use std::mem;
use std::sync::Arc;

#[derive(Default)]
struct GuestCallState {
    /// The context the call was made for, or `None` outside of a guest call.
    calling_context: Option<ContextId>,
    accepted: Vec<AcceptedCallout>,
}

/// The callout service of one guest.
///
/// Callout ids are only unique within a guest, so each guest has its own service.
pub(crate) struct GuestCalloutService {
    conf: Arc<PluginCalloutConf>,
    call_in_progress: Mutex<GuestCallState>,
}

impl GuestCalloutService {
    pub(crate) fn new(conf: Arc<PluginCalloutConf>) -> Self {
        GuestCalloutService {
            conf,
            call_in_progress: Mutex::new(GuestCallState::default()),
        }
    }

    pub(crate) fn record_callouts<R>(
        &self,
        context: ContextId,
        guest_call: impl FnOnce() -> R,
    ) -> (R, Vec<AcceptedCallout>) {
        // Also discards the callouts left behind by an earlier call that unwound
        *self.call_in_progress.lock() = GuestCallState {
            calling_context: Some(context),
            accepted: Vec::new(),
        };
        let result = guest_call();
        let state_at_the_end = mem::take(&mut *self.call_in_progress.lock());
        (result, state_at_the_end.accepted)
    }
}

impl Callouts for GuestCalloutService {
    fn http_call(
        &self,
        call: Invocation,
        callout: CalloutId,
        request: HttpCall<'_>,
    ) -> Result<(), HttpCallRefusal> {
        let plugin = &self.conf.plugin_name;
        let mut call_in_progress = self.call_in_progress.lock();
        match call_in_progress.calling_context {
            Some(context) if context == call.context => {}
            // A stream callback that switches to its root context cannot make a callout. The
            // result would be delivered to the root context on the root callback thread while
            // the request continued without it.
            Some(_) => {
                warn!("wasm plugin {plugin}: callout rejected, not sent from the context of the current callback");
                return Err(HttpCallRefusal::Failed);
            }
            None => {
                warn!("wasm plugin {plugin}: callout rejected, sent outside of a plugin callback");
                return Err(HttpCallRefusal::Failed);
            }
        }
        let upstream = std::str::from_utf8(&request.upstream)
            .ok()
            .filter(|upstream| self.conf.upstreams.has_upstream(plugin, upstream));
        let Some(upstream) = upstream else {
            let upstream = String::from_utf8_lossy(&request.upstream);
            warn!("wasm plugin {plugin}: callout rejected, upstream {upstream} is not in callout_upstreams");
            return Err(HttpCallRefusal::UnknownUpstream);
        };
        let header = callout_request_header(plugin, &request.headers, request.body.len());
        let header = match header {
            Ok(header) => header,
            Err(rejected) => {
                self.conf.warn_of_rejected_header_once(&rejected);
                return Err(HttpCallRefusal::UnknownUpstream);
            }
        };
        if !request.trailers.is_empty() {
            debug!("wasm plugin {plugin}: callout trailers dropped, not supported");
        }
        call_in_progress.accepted.push(AcceptedCallout {
            id: callout,
            plugin_conf: self.conf.clone(),
            upstream: upstream.to_string(),
            request: Box::new(header),
            body: Bytes::copy_from_slice(&request.body),
            timeout: self.conf.effective_timeout(request.timeout),
            callback: call.callback,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callout::headers::tests::{pairs, post_to_authz};
    use crate::callout::StaticCalloutUpstreams;
    use crate::test_support::{crate_log_lines_with, record_crate_logs};
    use pingora_core::upstreams::peer::HttpPeer;
    use proxy_wasm_host::abi::v0_2_1::{Callback, GuestId};
    use std::time::Duration;

    fn context(id: u32) -> ContextId {
        ContextId::try_from(id).unwrap()
    }

    fn service_of_plugin(plugin_name: &str) -> GuestCalloutService {
        let mut upstreams = StaticCalloutUpstreams::new();
        upstreams.insert("authz", HttpPeer::new("127.0.0.1:1", false, String::new()));
        let limit = Duration::from_secs(10);
        let conf = PluginCalloutConf::new(plugin_name, Arc::new(upstreams), limit, limit, 1024);
        GuestCalloutService::new(Arc::new(conf))
    }

    fn authz_service() -> GuestCalloutService {
        service_of_plugin("a")
    }

    fn call_to(upstream: &'static [u8]) -> HttpCall<'static> {
        HttpCall::new(upstream)
            .with_headers(pairs(&post_to_authz()))
            .with_body(&b"body"[..])
    }

    fn send_from(
        service: &GuestCalloutService,
        from: u32,
        request: HttpCall<'_>,
    ) -> Result<(), HttpCallRefusal> {
        let call =
            Invocation::new(GuestId::next(), context(from)).with_callback(Callback::RequestHeaders);
        service.http_call(call, 1.try_into().unwrap(), request)
    }

    #[test]
    fn service_accepts_only_calling_context_and_known_upstream() {
        let mut bad_header = post_to_authz();
        bad_header.push(("bad name", "value"));
        let bad_header = HttpCall::new(&b"authz"[..]).with_headers(pairs(&bad_header));
        let cases = [
            (2, call_to(b"audit"), Err(HttpCallRefusal::UnknownUpstream)),
            (2, call_to(b"\xff"), Err(HttpCallRefusal::UnknownUpstream)),
            (2, bad_header, Err(HttpCallRefusal::UnknownUpstream)),
            (1, call_to(b"authz"), Err(HttpCallRefusal::Failed)),
            (2, call_to(b"authz"), Ok(())),
        ];

        for (from, request, want) in cases {
            let service = authz_service();

            let (got, accepted) =
                service.record_callouts(context(2), || send_from(&service, from, request));

            assert_eq!(got, want);
            assert_eq!(accepted.len(), usize::from(want.is_ok()));
        }
    }

    #[test]
    fn unknown_upstream_rejection_logs_plugin_and_upstream() {
        record_crate_logs();
        let conf = PluginCalloutConf::new(
            "refused-plugin",
            Arc::new(StaticCalloutUpstreams::new()),
            Duration::from_secs(10),
            Duration::from_secs(10),
            1024,
        );
        let service = GuestCalloutService::new(Arc::new(conf));

        let (refused, _) =
            service.record_callouts(context(2), || send_from(&service, 2, call_to(b"audit")));

        assert_eq!(refused, Err(HttpCallRefusal::UnknownUpstream));
        let lines = crate_log_lines_with("refused-plugin");
        let want = "wasm plugin refused-plugin: callout rejected, \
                    upstream audit is not in callout_upstreams";
        assert_eq!(lines, [want]);
    }

    #[test]
    fn accepted_callout_carries_plugin_request() {
        let service = authz_service();

        let (_, accepted) =
            service.record_callouts(context(2), || send_from(&service, 2, call_to(b"authz")));

        let callout = &accepted[0];
        assert_eq!(callout.upstream, "authz");
        assert_eq!(callout.request.method, "POST");
        assert_eq!(callout.request.raw_path(), b"/check?dry=1");
        assert_eq!(callout.request.headers["host"], "authz.test");
        assert_eq!(callout.request.headers["content-length"], "4");
        assert_eq!(&callout.body[..], b"body");
    }

    #[test]
    fn service_rejects_callout_outside_guest_call() {
        record_crate_logs();
        let service = service_of_plugin("outside-call");

        let before = send_from(&service, 2, call_to(b"authz"));
        service.record_callouts(context(2), || ());
        let after = send_from(&service, 2, call_to(b"authz"));

        assert_eq!(before, Err(HttpCallRefusal::Failed));
        assert_eq!(after, Err(HttpCallRefusal::Failed));
        let lines = crate_log_lines_with("wasm plugin outside-call:");
        let want = "wasm plugin outside-call: callout rejected, sent outside of a plugin callback";
        assert_eq!(lines, [want, want]);
    }

    #[test]
    fn rejected_callout_header_warns_once_per_plugin() {
        record_crate_logs();
        let service = service_of_plugin("bad-header");
        let mut headers = post_to_authz();
        headers.push(("bad name", "value"));

        let refusals = [(); 2].map(|()| {
            let request = HttpCall::new(&b"authz"[..]).with_headers(pairs(&headers));
            let send = || send_from(&service, 2, request);
            service.record_callouts(context(2), send).0
        });

        assert_eq!(refusals, [Err(HttpCallRefusal::UnknownUpstream); 2]);
        let lines = crate_log_lines_with("wasm plugin bad-header:");
        let want = "wasm plugin bad-header: callout rejected, header bad name is invalid, \
                    further occurrences are not logged";
        assert_eq!(lines, [want]);
    }

    #[test]
    fn callouts_of_unwound_guest_call_are_discarded() {
        let service = authz_service();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            service.record_callouts(context(2), || {
                send_from(&service, 2, call_to(b"authz")).unwrap();
                panic!("simulated unwind of a guest call");
            })
        }));

        let (_, accepted) = service.record_callouts(context(3), || ());

        assert!(unwound.is_err());
        assert!(accepted.is_empty());
    }
}
