;; Template for the small Proxy-Wasm guests that tests assemble.
;; Tests replace the line holding only the word CALLBACKS with the callbacks they export.
(module
  (import "env" "proxy_set_buffer_bytes"
    (func $set (param i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_send_local_response"
    (func $send (param i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_replace_header_map_value"
    (func $replace (param i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_remove_header_map_value"
    (func $remove (param i32 i32 i32) (result i32)))
  (import "env" "proxy_http_call"
    (func $http_call (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_continue_stream" (func $continue_stream (param i32) (result i32)))
  (import "env" "proxy_close_stream" (func $close_stream (param i32) (result i32)))
  (import "env" "proxy_get_buffer_bytes"
    (func $get_buffer (param i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_add_header_map_value"
    (func $add_header (param i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_get_header_map_value"
    (func $get_header (param i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_log" (func $log (param i32 i32 i32) (result i32)))
  (import "env" "proxy_set_tick_period_milliseconds"
    (func $set_tick_period (param i32) (result i32)))
  (import "env" "proxy_define_metric"
    (func $define_metric (param i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_increment_metric"
    (func $increment_metric (param i32 i64) (result i32)))
  (import "env" "proxy_get_property"
    (func $get_property (param i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_set_property"
    (func $set_property (param i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_set_effective_context"
    (func $set_effective_context (param i32) (result i32)))
  (import "env" "proxy_done" (func $proxy_done (result i32)))
  (import "env" "proxy_get_header_map_pairs"
    (func $get_header_pairs (param i32 i32 i32) (result i32)))
  (import "env" "proxy_register_shared_queue"
    (func $register_queue (param i32 i32 i32) (result i32)))
  (import "env" "proxy_enqueue_shared_queue"
    (func $enqueue (param i32 i32 i32) (result i32)))
  (import "env" "proxy_grpc_call"
    (func $grpc_call (param i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_grpc_stream"
    (func $grpc_stream (param i32 i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_grpc_send" (func $grpc_send (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 16) "replaced")
  (data (i32.const 32) "teapot")
  (data (i32.const 48) "x-trailer")
  (data (i32.const 64) "set")
  (data (i32.const 80) "a")
  (data (i32.const 81) "b")
  (data (i32.const 96) "content-length")
  (data (i32.const 112) "authz")
  ;; Serialized callout headers for GET /check with the authority authz.test
  (data (i32.const 128) "\03\00\00\00\07\00\00\00\03\00\00\00\05\00\00\00\06\00\00\00\0a\00\00\00\0a\00\00\00\3a\6d\65\74\68\6f\64\00\47\45\54\00\3a\70\61\74\68\00\2f\63\68\65\63\6b\00\3a\61\75\74\68\6f\72\69\74\79\00\61\75\74\68\7a\2e\74\65\73\74\00")
  (data (i32.const 256) "x-asked")
  (data (i32.const 264) "yes")
  (data (i32.const 272) "failed")
  (data (i32.const 280) "response")
  (data (i32.const 288) "accepted")
  (data (i32.const 296) "refused")
  (data (i32.const 304) "missing")
  (data (i32.const 312) "tick")
  (data (i32.const 320) "svc")
  (data (i32.const 324) "Check")
  (data (i32.const 330) "ping")
  ;; An empty serialized map, for gRPC metadata
  (data (i32.const 336) "\00\00\00\00")
  (data (i32.const 340) "closed")
  ;; Addresses from 700 up are for strings that tests add for their own callbacks.

  ;; Prepend one letter to the body. The buffer is 0 for the request and 1 for the response.
  (func $mark (param $buffer i32) (param $letter i32) (result i32)
    (drop (call $set
      (local.get $buffer) (i32.const 0) (i32.const 0) (local.get $letter) (i32.const 1)))
    i32.const 0)
  (func $mark_a (param $buffer i32) (result i32)
    (call $mark (local.get $buffer) (i32.const 80)))
  (func $mark_b (param $buffer i32) (result i32)
    (call $mark (local.get $buffer) (i32.const 81)))

  ;; Replace the first $size bytes of the body with "replaced".
  (func $replace_body (param $buffer i32) (param $size i32) (result i32)
    (drop (call $set
      (local.get $buffer) (i32.const 0) (local.get $size) (i32.const 16) (i32.const 8)))
    i32.const 0)

  ;; Send a response with the body "teapot" and pause.
  (func $respond (param $status i32) (result i32)
    (drop (call $send
      (local.get $status) (i32.const 0) (i32.const 0) (i32.const 32) (i32.const 6)
      (i32.const 0) (i32.const 0) (i32.const -1)))
    i32.const 1)

  ;; Set the response trailer "x-trailer" to "set".
  (func $set_trailer (result i32)
    (drop (call $replace
      (i32.const 3) (i32.const 48) (i32.const 9) (i32.const 64) (i32.const 3)))
    i32.const 0)

  ;; Remove "content-length" from the response headers.
  (func $remove_length (result i32)
    (drop (call $remove (i32.const 2) (i32.const 96) (i32.const 14)))
    i32.const 0)

  ;; Send a callout to the upstream "authz" without setting a timeout, and pause.
  (func $call_authz_and_pause (result i32)
    (drop (call $http_call
      (i32.const 112) (i32.const 5) (i32.const 128) (i32.const 75)
      (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 520)))
    i32.const 1)

  ;; Send a callout and log "accepted" if the host took it, or "refused" if not.
  (func $call_and_log_status
    (if (call $http_call
      (i32.const 112) (i32.const 5) (i32.const 128) (i32.const 75)
      (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 0) (i32.const 520))
      (then (drop (call $log (i32.const 2) (i32.const 296) (i32.const 7))))
      (else (drop (call $log (i32.const 2) (i32.const 288) (i32.const 8))))))

  ;; Respond with 418 if the request has an "x-asked" header, and continue otherwise.
  (func $teapot_if_asked (result i32)
    (if (result i32) (call $get_header
      (i32.const 0) (i32.const 256) (i32.const 7) (i32.const 512) (i32.const 516))
      (then (i32.const 0))
      (else (call $respond (i32.const 418)))))

  ;; Send a callout and continue without waiting for it.
  (func $call_without_pause (result i32)
    (drop (call $call_authz_and_pause))
    i32.const 0)

  ;; Continue the stream, which is 0 for the request and 1 for the response.
  (func $continue (param $stream i32)
    (drop (call $continue_stream (local.get $stream))))

  ;; Continue the stream and still return pause.
  (func $continue_and_pause (param $stream i32) (result i32)
    (call $continue (local.get $stream))
    i32.const 1)

  ;; Continue the stream on the second call only.
  (func $continue_on_second (param $stream i32)
    (i32.store (i32.const 600) (i32.add (i32.load (i32.const 600)) (i32.const 1)))
    (if (i32.eq (i32.load (i32.const 600)) (i32.const 2))
      (then (call $continue (local.get $stream)))))

  ;; Add the request header "x-asked" with the value "yes".
  (func $mark_asked
    (drop (call $add_header
      (i32.const 0) (i32.const 256) (i32.const 7) (i32.const 264) (i32.const 3))))

  ;; Respond with 418 and the callout response body, or with 500 if the callout has no headers.
  (func $relay_callout_body (param $headers i32) (param $size i32)
    (if (i32.eqz (local.get $headers))
      (then (drop (call $respond (i32.const 500))))
      (else
        (drop (call $get_buffer
          (i32.const 4) (i32.const 0) (local.get $size) (i32.const 512) (i32.const 516)))
        (drop (call $send
          (i32.const 418) (i32.const 0) (i32.const 0)
          (i32.load (i32.const 512)) (i32.load (i32.const 516))
          (i32.const 0) (i32.const 0) (i32.const -1))))))

  ;; Log "failed" for a callout result without headers, and "response" otherwise.
  (func $log_result (param $headers i32)
    (if (i32.eqz (local.get $headers))
      (then (drop (call $log (i32.const 2) (i32.const 272) (i32.const 6))))
      (else (drop (call $log (i32.const 2) (i32.const 280) (i32.const 8))))))

  ;; Log the value of the property at the given path, or "missing" if it cannot be read.
  (func $log_property (param $path i32) (param $size i32)
    (if (call $get_property (local.get $path) (local.get $size) (i32.const 512) (i32.const 516))
      (then (drop (call $log (i32.const 2) (i32.const 304) (i32.const 7))))
      (else (drop (call $log
        (i32.const 2) (i32.load (i32.const 512)) (i32.load (i32.const 516)))))))

  ;; Copy the property at the given path into a header of the given map, which is 0 for the
  ;; request headers and 2 for the response headers. Nothing is added if the read fails.
  (func $property_to_header
    (param $map i32) (param $path i32) (param $size i32) (param $name i32) (param $name_size i32)
    (if (i32.eqz (call $get_property
      (local.get $path) (local.get $size) (i32.const 512) (i32.const 516)))
      (then (drop (call $add_header
        (local.get $map) (local.get $name) (local.get $name_size)
        (i32.load (i32.const 512)) (i32.load (i32.const 516)))))))

  ;; Send a gRPC call with the message "ping" to the upstream "authz" and pause.
  (func $grpc_call_and_pause (result i32)
    (drop (call $grpc_call
      (i32.const 112) (i32.const 5) (i32.const 320) (i32.const 3) (i32.const 324) (i32.const 5)
      (i32.const 336) (i32.const 4) (i32.const 330) (i32.const 4) (i32.const 1000) (i32.const 520)))
    i32.const 1)

  ;; Open a gRPC stream to the upstream "authz" and keep its id at 524.
  (func $open_grpc_stream
    (drop (call $grpc_stream
      (i32.const 112) (i32.const 5) (i32.const 320) (i32.const 3) (i32.const 324) (i32.const 5)
      (i32.const 336) (i32.const 4) (i32.const 524))))

  ;; Send "ping" on the stream whose id is at 524, and pause.
  (func $grpc_send_and_pause (result i32)
    (drop (call $grpc_send (i32.load (i32.const 524)) (i32.const 330) (i32.const 4) (i32.const 0)))
    i32.const 1)

  ;; Log the gRPC message of the given size.
  (func $log_grpc_message (param $size i32)
    (drop (call $get_buffer
      (i32.const 5) (i32.const 0) (local.get $size) (i32.const 512) (i32.const 516)))
    (drop (call $log (i32.const 2) (i32.load (i32.const 512)) (i32.load (i32.const 516)))))

  ;; Log "tick".
  (func $log_tick
    (drop (call $log (i32.const 2) (i32.const 312) (i32.const 4))))

  (func (export "proxy_on_memory_allocate") (param i32) (result i32) i32.const 1024)
  (func (export "proxy_on_context_create") (param i32 i32))
CALLBACKS
)
