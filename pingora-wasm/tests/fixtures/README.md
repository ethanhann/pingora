# Test fixtures

The compiled Proxy-Wasm plugins in this directory are built for the `wasm32-wasip1` target from the `crates/test-guests` workspace of [proxy-wasm-host](https://github.com/ethanhann/proxy-wasm-host) at commit `0252ba5`.
The exceptions are `plugin-services.wasm`, `sdk-grpc-auth-random.wasm`, and `exercise-all.wasm`, which are built from commit `be279de`, the commit that added the source of `plugin-services`.
The sources of `sdk-grpc-auth-random` and `exercise-all` are the same at both commits.

| File | Source | License |
|---|---|---|
| `add-request-header.wasm` | `crates/test-guests/add-request-header` | Apache-2.0, Copyright (c) 2026 Ethan Hann |
| `http-example.wasm` | `crates/test-guests/http-example` | Apache-2.0, Copyright (c) 2026 Ethan Hann |
| `sdk-http-headers.wasm` | `crates/test-guests/sdk-http-headers`, the `http_headers` example of the Rust Proxy-Wasm SDK v0.2.5 | Apache-2.0, Copyright 2020 Google LLC |
| `sdk-http-body.wasm` | `crates/test-guests/sdk-http-body`, the `http_body` example of the Rust Proxy-Wasm SDK v0.2.5 | Apache-2.0, Copyright 2020 Google LLC |
| `sdk-http-auth-random.wasm` | `crates/test-guests/sdk-http-auth-random`, the `http_auth_random` example of the Rust Proxy-Wasm SDK v0.2.5 | Apache-2.0, Copyright 2020 Google LLC |
| `plugin-services.wasm` | `crates/test-guests/plugin-services` | Apache-2.0, Copyright (c) 2026 Ethan Hann |
| `sdk-http-config.wasm` | `crates/test-guests/sdk-http-config`, the `http_config` example of the Rust Proxy-Wasm SDK v0.2.5 | Apache-2.0, Copyright 2020 Google LLC |
| `sdk-grpc-auth-random.wasm` | `crates/test-guests/sdk-grpc-auth-random`, the `grpc_auth_random` example of the Rust Proxy-Wasm SDK v0.2.5 | Apache-2.0, Copyright 2020 Google LLC |
| `exercise-all.wasm` | `crates/test-guests/exercise-all` | Apache-2.0, Copyright (c) 2026 Ethan Hann |

To update a fixture, rebuild it in that workspace and copy the resulting `.wasm` file into this directory.

`guest.wat` is the template for the small guests that the tests assemble at run time.
