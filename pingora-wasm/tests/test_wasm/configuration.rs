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

//! Configuration tests
//!
//! Covers a proxy whose plugins and chain are read from a YAML file.

use super::{all, get};
use crate::utils::{echo_origin, init};

#[tokio::test]
async fn proxy_built_from_yaml_runs_plugins_in_chain_order() {
    init().await;
    let (origin, _) = echo_origin().await;

    let res = get(6413, "/", origin.addr().port(), &[]).await;

    assert_eq!(res.status(), 200);
    assert_eq!(all(&res, "x-echo-x-order"), ["first", "second"]);
}
