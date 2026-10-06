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

//! Per-request stream state

mod body;
mod headers;
mod plugin_response;
mod tcp;

pub(crate) use body::BodyBuffer;
pub(crate) use headers::{RequestHeaders, ResponseHeaders, ResponseTrailers};
pub(crate) use plugin_response::PluginResponse;
pub(crate) use tcp::TcpCallbackState;

use crate::callout::grpc::is_grpc_content_type;
use crate::properties::built_in::{write_built_in_property, ReadableHeaders, RequestFacts};
use crate::properties::{join_path, WasmProperties};
use crate::WasmForeignFunctions;
use http::header::CONTENT_TYPE;
use log::{debug, warn};
use proxy_wasm_host::abi::v0_2_1::types::{BufferType, MapType, Status, StreamType};
use proxy_wasm_host::abi::v0_2_1::{
    Access, Callback, ForeignCall, Invocation, LocalResponse, StreamState,
};
use proxy_wasm_host::{Buffer, HeaderMap, VecHeaderMap};
use std::sync::Arc;

/// The state a guest can read and write during a single callback.
///
/// A filter moves Pingora's headers, body bytes, and trailers in before each callback and takes
/// them back out afterwards.
#[derive(Default)]
pub(crate) struct PingoraStream {
    pub(crate) plugin_name: Arc<str>,
    pub(crate) request: Option<RequestHeaders>,
    pub(crate) response: Option<ResponseHeaders>,
    pub(crate) trailers: Option<ResponseTrailers>,
    pub(crate) body_buffer: BodyBuffer,
    pub(crate) plugin_response: Option<PluginResponse>,
    asked_to_continue_request: bool,
    asked_to_continue_response: bool,
    /// The callback the plugin is paused in while a callout response is being delivered to it.
    pub(crate) delivery_callback: Option<Callback>,
    empty: VecHeaderMap,
    pub(crate) request_facts: RequestFacts,
    /// Per-request properties set by the proxy. Plugins cannot change them.
    pub(crate) proxy_properties: WasmProperties,
    pub(crate) guest_properties: WasmProperties,
    fixed_properties: Arc<WasmProperties>,
    foreign_functions: Arc<WasmForeignFunctions>,
    joined_path: Vec<u8>,
    /// Set for the contexts of a TCP connection.
    pub(crate) tcp: Option<TcpCallbackState>,
}

impl PingoraStream {
    pub(crate) fn new(
        fixed_properties: Arc<WasmProperties>,
        foreign_functions: Arc<WasmForeignFunctions>,
    ) -> Self {
        PingoraStream {
            fixed_properties,
            foreign_functions,
            ..PingoraStream::default()
        }
    }

    fn request_map(&mut self) -> Result<&mut dyn HeaderMap, Status> {
        match self.request.as_mut() {
            Some(map) => Ok(map),
            None => Err(Status::BadArgument),
        }
    }

    pub(crate) fn clear_continue_requests(&mut self) {
        self.asked_to_continue_request = false;
        self.asked_to_continue_response = false;
        if let Some(tcp) = &mut self.tcp {
            tcp.clear_requests();
        }
    }

    pub(crate) fn continue_requested(&self, direction: StreamType) -> bool {
        match (direction, &self.tcp) {
            (StreamType::HttpRequest, _) => self.asked_to_continue_request,
            (StreamType::HttpResponse, _) => self.asked_to_continue_response,
            (_, Some(tcp)) => tcp.continue_requested(direction),
            (_, None) => false,
        }
    }

    fn tcp_data(&mut self, buffer: BufferType) -> Result<&mut dyn Buffer, Status> {
        let tcp = self.tcp.as_mut().ok_or(Status::NotFound)?;
        let data = match buffer {
            BufferType::DownstreamData => tcp.downstream_data.as_mut(),
            BufferType::UpstreamData => tcp.upstream_data.as_mut(),
            _ => None,
        };
        data.map(|data| data as &mut dyn Buffer)
            .ok_or(Status::NotFound)
    }

    fn is_grpc_request(&self) -> bool {
        let content_type = self
            .request
            .as_ref()
            .and_then(|request| request.header.headers.get(CONTENT_TYPE));
        content_type.is_some_and(|value| is_grpc_content_type(value.as_bytes()))
    }

    /// Return the callback whose access rules apply to a host call.
    fn access_callback(&self, call: Invocation) -> Option<Callback> {
        match call.callback {
            // A plugin that receives a callout result gets the access of the callback it is
            // paused in
            Some(
                Callback::HttpCallResponse
                | Callback::GrpcReceiveInitialMetadata
                | Callback::GrpcReceive
                | Callback::GrpcReceiveTrailingMetadata
                | Callback::GrpcClose,
            ) => self.delivery_callback,
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
            // An empty map instead of an error, since a guest built with the Rust SDK panics on
            // an error status and treats an empty map as a missing value
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
        if self.tcp.is_some() {
            return self.tcp_data(buffer);
        }
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
        let mut plugin_response = PluginResponse::build(&response).ok_or(Status::BadArgument)?;
        let plugin = &self.plugin_name;
        if !response.status_code_details.is_empty() {
            debug!(
                "wasm plugin {plugin}: sent a {} response with details: {}",
                response.status_code,
                String::from_utf8_lossy(&response.status_code_details)
            );
        }
        if self.is_grpc_request() {
            plugin_response = plugin_response
                .into_grpc(response.grpc_status)
                .ok_or(Status::BadArgument)?;
        } else if let Some(grpc_status) = response.grpc_status {
            warn!("wasm plugin {plugin}: response gRPC status {grpc_status} dropped, request is not gRPC");
        }
        self.plugin_response = Some(plugin_response);
        Ok(())
    }

    fn property(
        &mut self,
        _call: Invocation,
        path: &[&[u8]],
        out: &mut Vec<u8>,
    ) -> Result<(), Status> {
        join_path(path.iter().copied(), &mut self.joined_path);
        let key = &self.joined_path;
        let headers = ReadableHeaders {
            request: self.request.as_ref(),
            response: self.response.as_ref().map(|r| &r.header),
        };
        if let Some(value) = self.proxy_properties.get_joined(key) {
            out.extend_from_slice(value);
            return Ok(());
        }
        if write_built_in_property(key, &self.request_facts, &headers, out) {
            return Ok(());
        }
        let fixed = self.fixed_properties.get_joined(key);
        match fixed.or_else(|| self.guest_properties.get_joined(key)) {
            Some(value) => {
                out.extend_from_slice(value);
                Ok(())
            }
            None => Err(Status::NotFound),
        }
    }

    // Always returns `Ok`, even for a path a proxy, built-in, or fixed property already provides,
    // because a guest built with the Rust SDK panics on any other status. Reads of such a path
    // keep returning the value the proxy or the runtime provides.
    fn set_property(
        &mut self,
        _call: Invocation,
        path: &[&[u8]],
        value: &[u8],
    ) -> Result<(), Status> {
        join_path(path.iter().copied(), &mut self.joined_path);
        self.guest_properties
            .insert_joined(&self.joined_path, value);
        Ok(())
    }

    fn call_foreign_function(
        &mut self,
        _call: Invocation,
        request: ForeignCall<'_>,
        out: &mut Vec<u8>,
    ) -> Result<(), Status> {
        let functions = &self.foreign_functions;
        functions.call(&self.plugin_name, &request.name, &request.arguments, out)
    }

    fn continue_stream(&mut self, _call: Invocation, stream: StreamType) -> Result<(), Status> {
        if let Some(tcp) = &mut self.tcp {
            tcp.request_continue(stream);
            return Ok(());
        }
        match stream {
            StreamType::HttpRequest => self.asked_to_continue_request = true,
            StreamType::HttpResponse => self.asked_to_continue_response = true,
            StreamType::Downstream => {}
            StreamType::Upstream => return Err(Status::Unimplemented),
        }
        Ok(())
    }

    fn close_stream(&mut self, _call: Invocation, stream: StreamType) -> Result<(), Status> {
        match &mut self.tcp {
            Some(tcp) => {
                tcp.request_close(stream);
                Ok(())
            }
            None => Err(Status::Unimplemented),
        }
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

    fn is_available(
        stream: &mut PingoraStream,
        callback: Callback,
        access: Access,
        map: MapType,
    ) -> bool {
        stream.header_map(call(callback), access, map).is_ok()
    }

    #[test]
    fn header_map_access_depends_on_callback() {
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
            .map(|(cb, access, map, _)| is_available(&mut s, *cb, *access, *map))
            .collect();

        let want: Vec<_> = cases.iter().map(|c| c.3).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn callout_delivery_has_access_of_paused_callback() {
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
    fn callout_delivery_cannot_write_beyond_paused_callback() {
        let mut s = stream(true);
        s.delivery_callback = Some(Callback::RequestBody);
        let call = call(Callback::HttpCallResponse);

        let written = s.header_map(call, Access::Write, MapType::HttpRequestHeaders);

        assert!(written.is_err());
    }

    #[test]
    fn continue_stream_records_each_http_direction() {
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
    fn header_map_rejects_request_trailers_in_request_headers() {
        let mut s = stream(true);

        let trailers = is_available(
            &mut s,
            Callback::RequestHeaders,
            Access::Read,
            MapType::HttpRequestTrailers,
        );

        assert!(!trailers);
    }

    #[test]
    fn unavailable_map_reads_as_empty() {
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
    fn buffer_is_available_only_in_matching_body_callback() {
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
        for (callback, buffer, available) in cases {
            let mut s = stream(true);

            let status = s
                .buffer(call(callback), Access::Write, buffer)
                .map(|_| ())
                .err();

            let expected = (!available).then_some(Status::NotFound);
            assert_eq!(status, expected, "{callback:?}");
        }
    }

    fn local(status: u32) -> LocalResponse<'static> {
        LocalResponse::new(status).with_body(Cow::Borrowed(&b"body"[..]))
    }

    #[test]
    fn send_local_response_records_response() {
        let mut s = stream(false);

        let result = s.send_local_response(call(Callback::RequestHeaders), local(403));

        assert_eq!(result, Ok(()));
        let recorded = s.plugin_response.unwrap();
        assert_eq!(recorded.header.status, 403);
        assert_eq!(&recorded.body[..], b"body");
    }

    #[test]
    fn send_local_response_takes_grpc_form_for_grpc_request() {
        let cases = [
            (
                "application/grpc",
                Some(10),
                "200",
                Some("10"),
                Some("body"),
            ),
            (
                "application/grpc+proto",
                None,
                "200",
                Some("7"),
                Some("body"),
            ),
            ("application/json", Some(10), "403", None, None),
        ];

        for (content_type, grpc_status, want_status, want_grpc_status, want_message) in cases {
            let mut s = stream(false);
            let request = &mut s.request.as_mut().unwrap().header;
            request.insert_header(CONTENT_TYPE, content_type).unwrap();
            let mut response = local(403).with_headers(vec![(
                Cow::Borrowed(&b"x-denied"[..]),
                Cow::Borrowed(&b"yes"[..]),
            )]);
            response.grpc_status = grpc_status;

            s.send_local_response(call(Callback::RequestHeaders), response)
                .unwrap();

            let recorded = s.plugin_response.unwrap();
            let get = |name| {
                recorded
                    .header
                    .headers
                    .get(name)
                    .map(|v| v.to_str().unwrap())
            };
            assert_eq!(
                recorded.header.status.as_str(),
                want_status,
                "{content_type}"
            );
            assert_eq!(get("grpc-status"), want_grpc_status, "{content_type}");
            assert_eq!(get("grpc-message"), want_message, "{content_type}");
            assert_eq!(get("x-denied"), Some("yes"), "{content_type}");
            let grpc = want_grpc_status.is_some();
            let want_content_type = grpc.then_some("application/grpc");
            assert_eq!(get("content-type"), want_content_type, "{content_type}");
            assert_eq!(recorded.body.is_empty(), grpc, "{content_type}");
        }
    }

    #[test]
    fn second_send_local_response_replaces_first() {
        let mut s = stream(false);
        s.send_local_response(call(Callback::RequestHeaders), local(403))
            .unwrap();

        s.send_local_response(call(Callback::RequestHeaders), local(401))
            .unwrap();

        assert_eq!(s.plugin_response.unwrap().header.status, 401);
    }

    #[test]
    fn send_local_response_needs_request_or_response_callback_and_final_status() {
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

    #[test]
    fn guest_property_write_cannot_shadow_proxy_built_in_or_fixed_value() {
        let mut stream = stream(false);
        stream
            .proxy_properties
            .insert(&["xds", "route_name"], "checkout");
        let mut fixed = WasmProperties::new();
        fixed.insert(&["node", "name"], "edge-1");
        stream.fixed_properties = Arc::new(fixed);
        let call = call(Callback::RequestHeaders);
        let paths: [[&[u8]; 2]; 4] = [
            [b"xds", b"route_name"],
            [b"request", b"method"],
            [b"node", b"name"],
            [b"plugin", b"note"],
        ];

        let writes = paths.map(|path| stream.set_property(call, &path, b"guest"));

        let mut read = |path: [&[u8]; 2]| {
            let mut value = Vec::new();
            stream.property(call, &path, &mut value).map(|()| value)
        };
        assert_eq!(writes, [Ok(()); 4]);
        assert_eq!(read(paths[0]), Ok(b"checkout".to_vec()));
        assert_eq!(read(paths[1]), Ok(b"GET".to_vec()));
        assert_eq!(read(paths[2]), Ok(b"edge-1".to_vec()));
        assert_eq!(read(paths[3]), Ok(b"guest".to_vec()));
        let written_over_fixed = stream.guest_properties.get(&["node", "name"]);
        assert_eq!(written_over_fixed, Some(&b"guest"[..]));
    }
}
