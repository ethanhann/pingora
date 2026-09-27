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

//! What a plugin can read and write during a callback: the request and response headers, and
//! the response it can send in place of the upstream response.

mod names;
mod plugin_response;
mod request_headers;
mod response_headers;

pub use plugin_response::write_plugin_response;
pub(crate) use plugin_response::PluginResponse;
pub(crate) use request_headers::RequestHeaders;
pub(crate) use response_headers::ResponseHeaders;

use log::{debug, warn};
use proxy_wasm_host::abi::v0_2_1::types::{MapType, Status};
use proxy_wasm_host::abi::v0_2_1::{Access, Callback, Invocation, LocalResponse, StreamState};
use proxy_wasm_host::{HeaderMap, VecHeaderMap};

/// The state that a guest can read and write during one callback.
///
/// The phases move the Pingora headers into it before each callback, and back after it.
#[derive(Default)]
pub(crate) struct PingoraStream {
    pub(crate) request: Option<RequestHeaders>,
    pub(crate) response: Option<ResponseHeaders>,
    pub(crate) plugin_response: Option<PluginResponse>,
    empty: VecHeaderMap,
}

impl PingoraStream {
    fn request_map(&mut self) -> Result<&mut dyn HeaderMap, Status> {
        match self.request.as_mut() {
            Some(map) => Ok(map),
            None => Err(Status::BadArgument),
        }
    }
}

impl StreamState for PingoraStream {
    fn header_map(
        &mut self,
        call: Invocation,
        access: Access,
        map: MapType,
    ) -> Result<&mut dyn HeaderMap, Status> {
        let read = access == Access::Read;
        match (map, call.callback) {
            (MapType::HttpRequestHeaders, Some(Callback::RequestHeaders)) => self.request_map(),
            (
                MapType::HttpRequestHeaders,
                Some(Callback::ResponseHeaders | Callback::Done | Callback::Log),
            ) if read => self.request_map(),
            (MapType::HttpResponseHeaders, Some(Callback::ResponseHeaders)) => {
                match self.response.as_mut() {
                    Some(map) => Ok(map),
                    None => Err(Status::BadArgument),
                }
            }
            (MapType::HttpResponseHeaders, Some(Callback::Done | Callback::Log)) if read => {
                match self.response.as_mut() {
                    Some(map) => Ok(map),
                    None => Ok(&mut self.empty),
                }
            }
            _ => Err(Status::BadArgument),
        }
    }

    fn send_local_response(
        &mut self,
        call: Invocation,
        response: LocalResponse<'_>,
    ) -> Result<(), Status> {
        if call.callback != Some(Callback::RequestHeaders) {
            return Err(Status::Unimplemented);
        }
        let plugin_response = PluginResponse::build(&response).ok_or(Status::BadArgument)?;
        if !response.status_code_details.is_empty() {
            debug!(
                "plugin response {}: {}",
                response.status_code,
                String::from_utf8_lossy(&response.status_code_details)
            );
        }
        if let Some(grpc_status) = response.grpc_status {
            warn!("plugin response gRPC status {grpc_status} is not sent");
        }
        self.plugin_response = Some(plugin_response);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::uri::Scheme;
    use pingora_http::{RequestHeader, ResponseHeader};
    use proxy_wasm_host::abi::v0_2_1::{ContextId, GuestId};
    use std::borrow::Cow;

    fn call(callback: Callback) -> Invocation {
        Invocation::new(GuestId::next(), ContextId::try_from(1).unwrap()).with_callback(callback)
    }

    fn stream(with_response: bool) -> PingoraStream {
        PingoraStream {
            request: Some(RequestHeaders::new(
                RequestHeader::build("GET", b"/", None).unwrap(),
                Scheme::HTTP,
            )),
            response: with_response
                .then(|| ResponseHeaders::new(ResponseHeader::build(200, None).unwrap())),
            ..Default::default()
        }
    }

    fn served(
        stream: &mut PingoraStream,
        callback: Callback,
        access: Access,
        map: MapType,
    ) -> bool {
        stream.header_map(call(callback), access, map).is_ok()
    }

    #[test]
    fn header_map_follows_the_table() {
        use Access::{Read, Write};
        use Callback::*;
        use MapType::{HttpRequestHeaders as Req, HttpResponseHeaders as Resp};
        let mut s = stream(true);
        let cases = [
            (ContextCreate, Read, Req, false),
            (ContextCreate, Read, Resp, false),
            (RequestHeaders, Read, Req, true),
            (RequestHeaders, Write, Req, true),
            (RequestHeaders, Read, Resp, false),
            (ResponseHeaders, Read, Req, true),
            (ResponseHeaders, Write, Req, false),
            (ResponseHeaders, Read, Resp, true),
            (ResponseHeaders, Write, Resp, true),
            (Done, Read, Req, true),
            (Done, Write, Req, false),
            (Done, Read, Resp, true),
            (Done, Write, Resp, false),
            (Log, Read, Req, true),
            (Log, Write, Req, false),
            (Log, Read, Resp, true),
            (Log, Write, Resp, false),
            (Delete, Read, Req, false),
            (Delete, Read, Resp, false),
        ];

        let got: Vec<_> = cases
            .iter()
            .map(|(cb, access, map, _)| served(&mut s, *cb, *access, *map))
            .collect();

        let want: Vec<_> = cases.iter().map(|c| c.3).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn header_map_refuses_other_map_types() {
        let mut s = stream(true);

        let trailers = served(
            &mut s,
            Callback::RequestHeaders,
            Access::Read,
            MapType::HttpRequestTrailers,
        );

        assert!(!trailers);
    }

    #[test]
    fn log_without_a_response_reads_an_empty_map() {
        let mut s = stream(false);

        let map = s
            .header_map(
                call(Callback::Log),
                Access::Read,
                MapType::HttpResponseHeaders,
            )
            .unwrap();

        assert!(map.is_empty());
    }

    fn local(status: u32) -> LocalResponse<'static> {
        LocalResponse::new(status).with_body(Cow::Borrowed(&b"body"[..]))
    }

    #[test]
    fn send_local_response_records_the_response() {
        let mut s = stream(false);

        let result = s.send_local_response(call(Callback::RequestHeaders), local(403));

        assert_eq!(result, Ok(()));
        let recorded = s.plugin_response.unwrap();
        assert_eq!(recorded.header.status, 403);
        assert_eq!(&recorded.body[..], b"body");
    }

    #[test]
    fn send_local_response_replaces_a_first_call() {
        let mut s = stream(false);
        s.send_local_response(call(Callback::RequestHeaders), local(403))
            .unwrap();

        s.send_local_response(call(Callback::RequestHeaders), local(401))
            .unwrap();

        assert_eq!(s.plugin_response.unwrap().header.status, 401);
    }

    #[test]
    fn send_local_response_refuses_a_bad_status_and_other_callbacks() {
        let cases = [
            (Callback::RequestHeaders, 99, Status::BadArgument),
            (Callback::ResponseHeaders, 403, Status::Unimplemented),
            (Callback::Log, 403, Status::Unimplemented),
        ];

        for (callback, status, refusal) in cases {
            let mut s = stream(true);

            let result = s.send_local_response(call(callback), local(status));

            assert_eq!(result, Err(refusal));
            assert!(s.plugin_response.is_none());
        }
    }
}
