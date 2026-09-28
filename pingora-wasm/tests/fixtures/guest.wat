;; A small Proxy-Wasm guest for tests.
;; A test replaces the line that holds only the word CALLBACKS with the callbacks it exports.
(module
  (import "env" "proxy_set_buffer_bytes"
    (func $set (param i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_send_local_response"
    (func $send (param i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_replace_header_map_value"
    (func $replace (param i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_remove_header_map_value"
    (func $remove (param i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 16) "replaced")
  (data (i32.const 32) "teapot")
  (data (i32.const 48) "x-trailer")
  (data (i32.const 64) "set")
  (data (i32.const 80) "a")
  (data (i32.const 81) "b")
  (data (i32.const 96) "content-length")

  ;; Write one letter in front of the body. The buffer is 0 for a request and 1 for a response.
  (func $mark (param $buffer i32) (param $letter i32) (result i32)
    (drop (call $set
      (local.get $buffer) (i32.const 0) (i32.const 0) (local.get $letter) (i32.const 1)))
    i32.const 0)
  (func $mark_a (param $buffer i32) (result i32)
    (call $mark (local.get $buffer) (i32.const 80)))
  (func $mark_b (param $buffer i32) (result i32)
    (call $mark (local.get $buffer) (i32.const 81)))

  ;; Replace the first bytes of the body with "replaced".
  (func $replace_body (param $buffer i32) (param $size i32) (result i32)
    (drop (call $set
      (local.get $buffer) (i32.const 0) (local.get $size) (i32.const 16) (i32.const 8)))
    i32.const 0)

  ;; Send a response with the body "teapot", and pause.
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

  (func (export "proxy_on_memory_allocate") (param i32) (result i32) i32.const 1024)
  (func (export "proxy_on_context_create") (param i32 i32))
  (func (export "proxy_on_configure") (param i32 i32) (result i32) i32.const 1)
  (func (export "proxy_on_log") (param i32))
  (func (export "proxy_on_delete") (param i32))
CALLBACKS
)
