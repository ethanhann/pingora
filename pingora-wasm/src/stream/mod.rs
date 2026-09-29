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

//! What a plugin can read and write during a callback: the headers, the body, the response
//! trailers, and the response it can send in place of the upstream response.

mod body;
mod names;
mod plugin_response;
mod request_headers;
mod response_headers;
mod response_trailers;

pub(crate) use body::BodyBuffer;
pub use plugin_response::write_plugin_response;
pub(crate) use plugin_response::PluginResponse;
pub(crate) use request_headers::RequestHeaders;
pub(crate) use response_headers::ResponseHeaders;
pub(crate) use response_trailers::ResponseTrailers;

use log::{debug, warn};
use proxy_wasm_host::abi::v0_2_1::types::{BufferType, MapType, Status, StreamType};
use proxy_wasm_host::abi::v0_2_1::{Access, Callback, Invocation, LocalResponse, StreamState};
use proxy_wasm_host::{Buffer, HeaderMap, VecHeaderMap};

/// The state that a guest can read and write during one callback.
///
/// The phases move the Pingora headers, the body bytes, and the trailers into it before each
/// callback, and back after it.
#[derive(Default)]
pub(crate) struct PingoraStream {
    pub(crate) request: Option<RequestHeaders>,
    pub(crate) response: Option<ResponseHeaders>,
    pub(crate) trailers: Option<ResponseTrailers>,
    pub(crate) body_buffer: BodyBuffer,
    pub(crate) plugin_response: Option<PluginResponse>,
    asked_to_continue_request: bool,
    asked_to_continue_response: bool,
    /// The callback whose access applies while the plugin receives the result of a callout.
    pub(crate) delivery_callback: Option<Callback>,
    empty: VecHeaderMap,
}

impl PingoraStream {
    fn request_map(&mut self) -> Result<&mut dyn HeaderMap, Status> {
        match self.request.as_mut() {
            Some(map) => Ok(map),
            None => Err(Status::BadArgument),
        }
    }

    /// Forget the directions that the plugin asked to continue in the last guest call.
    pub(crate) fn clear_continue_requests(&mut self) {
        self.asked_to_continue_request = false;
        self.asked_to_continue_response = false;
    }

    /// Return whether the plugin asked to continue `direction` in the last guest call.
    pub(crate) fn continue_requested(&self, direction: StreamType) -> bool {
        match direction {
            StreamType::HttpRequest => self.asked_to_continue_request,
            StreamType::HttpResponse => self.asked_to_continue_response,
            _ => false,
        }
    }

    /// Return the callback whose access applies to a host call of the plugin.
    ///
    /// While a plugin receives the result of a callout, it has the access of the callback that
    /// it waits in.
    fn access_callback(&self, call: Invocation) -> Option<Callback> {
        match call.callback {
            Some(Callback::HttpCallResponse) => self.delivery_callback,
            callback => callback,
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
        match (map, self.access_callback(call)) {
            (MapType::HttpRequestHeaders, Some(Callback::RequestHeaders)) => self.request_map(),
            (
                MapType::HttpRequestHeaders,
                Some(
                    Callback::ResponseHeaders
                    | Callback::Done
                    | Callback::Log
                    | Callback::RequestBody
                    | Callback::ResponseBody
                    | Callback::ResponseTrailers,
                ),
            ) if read => self.request_map(),
            (MapType::HttpResponseTrailers, Some(Callback::ResponseTrailers)) => {
                match self.trailers.as_mut() {
                    Some(map) => Ok(map),
                    None => Err(Status::BadArgument),
                }
            }
            // A guest built with the Rust SDK panics on an error status, and reads an empty map
            // as a missing value
            (
                MapType::HttpResponseHeaders
                | MapType::HttpRequestTrailers
                | MapType::HttpResponseTrailers,
                Some(Callback::RequestBody | Callback::ResponseBody | Callback::ResponseTrailers),
            ) if read => Ok(&mut self.empty),
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

    fn buffer(
        &mut self,
        call: Invocation,
        _access: Access,
        buffer: BufferType,
    ) -> Result<&mut dyn Buffer, Status> {
        match (buffer, self.access_callback(call)) {
            (BufferType::HttpRequestBody, Some(Callback::RequestBody))
            | (BufferType::HttpResponseBody, Some(Callback::ResponseBody)) => {
                Ok(&mut self.body_buffer)
            }
            _ => Err(Status::NotFound),
        }
    }

    fn send_local_response(
        &mut self,
        call: Invocation,
        response: LocalResponse<'_>,
    ) -> Result<(), Status> {
        if !matches!(
            self.access_callback(call),
            Some(
                Callback::RequestHeaders
                    | Callback::RequestBody
                    | Callback::ResponseHeaders
                    | Callback::ResponseBody
                    | Callback::ResponseTrailers
            )
        ) {
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

    fn continue_stream(&mut self, _call: Invocation, stream: StreamType) -> Result<(), Status> {
        match stream {
            StreamType::HttpRequest => self.asked_to_continue_request = true,
            StreamType::HttpResponse => self.asked_to_continue_response = true,
            StreamType::Downstream => {}
            StreamType::Upstream => return Err(Status::Unimplemented),
        }
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
            trailers: Some(ResponseTrailers::default()),
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
        use MapType::{
            HttpRequestHeaders as Req, HttpRequestTrailers as ReqTrailers,
            HttpResponseHeaders as Resp, HttpResponseTrailers as RespTrailers,
        };
        let mut s = stream(true);
        let cases = [
            (RequestBody, Read, Req, true),
            (RequestBody, Write, Req, false),
            (RequestBody, Read, Resp, true),
            (RequestBody, Write, Resp, false),
            (RequestBody, Read, ReqTrailers, true),
            (RequestBody, Write, RespTrailers, false),
            (ResponseBody, Read, Req, true),
            (ResponseBody, Read, Resp, true),
            (ResponseBody, Write, Resp, false),
            (ResponseBody, Read, RespTrailers, true),
            (ResponseBody, Write, RespTrailers, false),
            (ResponseTrailers, Read, Req, true),
            (ResponseTrailers, Read, Resp, true),
            (ResponseTrailers, Write, Resp, false),
            (ResponseTrailers, Read, ReqTrailers, true),
            (ResponseTrailers, Write, ReqTrailers, false),
            (ResponseTrailers, Read, RespTrailers, true),
            (ResponseTrailers, Write, RespTrailers, true),
            (ResponseHeaders, Read, RespTrailers, false),
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
    fn a_delivery_has_the_access_of_the_callback_that_waits() {
        use Access::{Read, Write};
        use BufferType::{HttpRequestBody, HttpResponseBody};
        use MapType::{
            HttpRequestHeaders as Req, HttpResponseHeaders as Resp,
            HttpResponseTrailers as RespTrailers,
        };
        let cases = [
            (Some(Callback::RequestHeaders), (Req, Write), None, true),
            (
                Some(Callback::RequestBody),
                (Req, Read),
                Some(HttpRequestBody),
                true,
            ),
            (Some(Callback::ResponseHeaders), (Resp, Write), None, true),
            (
                Some(Callback::ResponseBody),
                (Req, Read),
                Some(HttpResponseBody),
                true,
            ),
            (
                Some(Callback::ResponseTrailers),
                (RespTrailers, Write),
                None,
                true,
            ),
            (None, (Req, Read), None, false),
        ];

        for (waits_in, (map, access), body, has_access) in cases {
            let mut s = stream(true);
            s.delivery_callback = waits_in;
            let call = call(Callback::HttpCallResponse);
            let buffers = [HttpRequestBody, HttpResponseBody];

            let got_map = s.header_map(call, access, map).is_ok();
            let got_body = buffers.map(|buffer| s.buffer(call, Read, buffer).is_ok());
            let can_respond = s.send_local_response(call, LocalResponse::new(403)).is_ok();

            let want_body = buffers.map(|buffer| Some(buffer) == body);
            assert_eq!(got_map, has_access, "{waits_in:?}");
            assert_eq!(got_body, want_body, "{waits_in:?}");
            assert_eq!(can_respond, has_access, "{waits_in:?}");
        }
    }

    #[test]
    fn a_delivery_cannot_write_what_its_callback_cannot_write() {
        let mut s = stream(true);
        s.delivery_callback = Some(Callback::RequestBody);
        let call = call(Callback::HttpCallResponse);

        let written = s.header_map(call, Access::Write, MapType::HttpRequestHeaders);

        assert!(written.is_err());
    }

    #[test]
    fn continue_stream_records_a_continue_for_each_http_direction() {
        use StreamType::{Downstream, HttpRequest, HttpResponse, Upstream};
        let cases = [
            (vec![HttpRequest], Ok(()), (true, false)),
            (vec![HttpResponse], Ok(()), (false, true)),
            (vec![HttpRequest, HttpResponse], Ok(()), (true, true)),
            (vec![Downstream], Ok(()), (false, false)),
            (vec![Upstream], Err(Status::Unimplemented), (false, false)),
        ];

        for (continues, last_status, recorded) in cases {
            let mut s = stream(false);
            let call = call(Callback::HttpCallResponse);

            let statuses: Vec<_> = continues
                .iter()
                .map(|direction| s.continue_stream(call, *direction))
                .collect();

            assert_eq!(statuses.last(), Some(&last_status));
            let got = (
                s.continue_requested(HttpRequest),
                s.continue_requested(HttpResponse),
            );
            assert_eq!(got, recorded);
        }
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
    fn a_map_that_is_not_available_reads_as_an_empty_map() {
        let cases = [
            (Callback::RequestBody, MapType::HttpResponseHeaders),
            (Callback::ResponseBody, MapType::HttpResponseHeaders),
            (Callback::ResponseBody, MapType::HttpResponseTrailers),
            (Callback::ResponseTrailers, MapType::HttpRequestTrailers),
        ];

        for (callback, map) in cases {
            let mut s = stream(true);

            let map = s.header_map(call(callback), Access::Read, map).unwrap();

            assert!(map.is_empty(), "{callback:?}");
        }
    }

    #[test]
    fn buffer_follows_the_table() {
        use BufferType::{HttpRequestBody as Req, HttpResponseBody as Resp};
        let cases = [
            (Callback::RequestBody, Req, true),
            (Callback::RequestBody, Resp, false),
            (Callback::ResponseBody, Resp, true),
            (Callback::ResponseBody, Req, false),
            (Callback::RequestHeaders, Req, false),
            (Callback::ResponseTrailers, Resp, false),
            (Callback::Log, Resp, false),
            (Callback::RequestBody, BufferType::DownstreamData, false),
        ];
        for (callback, buffer, served) in cases {
            let mut s = stream(true);

            let status = s
                .buffer(call(callback), Access::Write, buffer)
                .map(|_| ())
                .err();

            let expected = (!served).then_some(Status::NotFound);
            assert_eq!(status, expected, "{callback:?}");
        }
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
    fn send_local_response_follows_the_table() {
        let cases = [
            (Callback::RequestHeaders, 403, Ok(())),
            (Callback::RequestBody, 403, Ok(())),
            (Callback::ResponseHeaders, 403, Ok(())),
            (Callback::ResponseBody, 403, Ok(())),
            (Callback::ResponseTrailers, 403, Ok(())),
            (Callback::RequestHeaders, 99, Err(Status::BadArgument)),
            (Callback::ResponseBody, 99, Err(Status::BadArgument)),
            (Callback::Done, 403, Err(Status::Unimplemented)),
            (Callback::Log, 403, Err(Status::Unimplemented)),
            (Callback::ContextCreate, 403, Err(Status::Unimplemented)),
        ];

        for (callback, status, expected) in cases {
            let mut s = stream(true);

            let result = s.send_local_response(call(callback), local(status));

            assert_eq!(result, expected, "{callback:?}");
            assert_eq!(s.plugin_response.is_some(), expected.is_ok());
        }
    }
}
