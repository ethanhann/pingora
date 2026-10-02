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

//! Properties from a file

use crate::properties::WasmProperties;
use serde::de::{Error, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::fmt;

enum PropertyNode {
    Value(String),
    Segments(Vec<(String, PropertyNode)>),
}

struct PropertyNodeVisitor;

impl<'de> Visitor<'de> for PropertyNodeVisitor {
    type Value = PropertyNode;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a string or a mapping of path segments")
    }

    fn visit_str<E: Error>(self, value: &str) -> Result<PropertyNode, E> {
        Ok(PropertyNode::Value(value.to_string()))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<PropertyNode, A::Error> {
        let mut segments = Vec::new();
        while let Some(segment) = map.next_entry::<String, PropertyNode>()? {
            segments.push(segment);
        }
        Ok(PropertyNode::Segments(segments))
    }
}

impl<'de> Deserialize<'de> for PropertyNode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(PropertyNodeVisitor)
    }
}

fn insert_values<'a>(
    properties: &mut WasmProperties,
    path: &mut Vec<&'a str>,
    node: &'a PropertyNode,
) {
    match node {
        PropertyNode::Value(value) => properties.insert(path, value.as_str()),
        PropertyNode::Segments(segments) => {
            for (segment, child) in segments {
                path.push(segment);
                insert_values(properties, path, child);
                path.pop();
            }
        }
    }
}

impl<'de> Deserialize<'de> for WasmProperties {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let root = PropertyNode::deserialize(deserializer)?;
        if let PropertyNode::Value(_) = root {
            return Err(D::Error::custom(
                "properties must be a mapping of path segments",
            ));
        }
        let mut properties = WasmProperties::new();
        insert_values(&mut properties, &mut Vec::new(), &root);
        Ok(properties)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_mapping_becomes_paths() {
        let yaml =
            "node:\n  name: edge-1\n  metadata:\n    NAME: a\n    istio.io/rev: b\nzone: c\n";

        let properties: WasmProperties = serde_yaml::from_str(yaml).unwrap();

        let mut want = WasmProperties::new();
        want.insert(&["node", "name"], "edge-1");
        want.insert(&["node", "metadata", "NAME"], "a");
        want.insert(&["node", "metadata", "istio.io/rev"], "b");
        want.insert(&["zone"], "c");
        assert_eq!(properties, want);
    }

    #[test]
    fn non_string_value_is_rejected() {
        let cases = ["node:\n  port: 8080\n", "node:\n  ready: true\n", "edge-1"];

        for yaml in cases {
            let read = serde_yaml::from_str::<WasmProperties>(yaml);

            let err = read.unwrap_err().to_string();
            assert!(err.contains("a mapping of path segments"), "{yaml}: {err}");
        }
    }
}
