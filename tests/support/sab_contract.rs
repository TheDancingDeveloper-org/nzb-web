use serde_json::Value;

/// Load a checked-in SABnzbd golden response.
pub fn golden(source: &str) -> Value {
    serde_json::from_str(source).expect("golden SABnzbd fixture must be valid JSON")
}

/// Normalize dynamic response fields and assert their original JSON types.
///
/// Golden fixtures use `$type:*` markers only for values that cannot be made
/// deterministic (timestamps, rates, paths, host measurements, and generated
/// IDs). The marker is copied into the normalized response after its type has
/// been checked, leaving an ordinary equality assertion to report missing,
/// extra, or semantically different fields with a complete JSON diff.
pub fn normalize_dynamic_fields(actual: &mut Value, expected: &Value) {
    normalize_at(actual, expected, "$");
}

pub fn assert_matches_golden(mut actual: Value, expected: &Value) {
    normalize_dynamic_fields(&mut actual, expected);
    assert_eq!(actual, *expected, "normalized SABnzbd response mismatch");
}

fn normalize_at(actual: &mut Value, expected: &Value, path: &str) {
    if let Some(marker) = expected.as_str().filter(|value| value.starts_with("$type:")) {
        assert_marker(actual, marker, path);
        *actual = expected.clone();
        return;
    }

    match (actual, expected) {
        (Value::Object(actual), Value::Object(expected)) => {
            for (key, expected_value) in expected {
                let field_path = format!("{path}.{key}");
                let actual_value = actual
                    .get_mut(key)
                    .unwrap_or_else(|| panic!("missing SABnzbd response field `{field_path}`"));
                normalize_at(actual_value, expected_value, &field_path);
            }
        }
        (Value::Array(actual), Value::Array(expected)) => {
            assert_eq!(
                actual.len(),
                expected.len(),
                "array length differs at `{path}`"
            );
            for (index, (actual_value, expected_value)) in
                actual.iter_mut().zip(expected).enumerate()
            {
                normalize_at(actual_value, expected_value, &format!("{path}[{index}]"));
            }
        }
        _ => {}
    }
}

fn assert_marker(actual: &Value, marker: &str, path: &str) {
    let valid = match marker {
        "$type:any" => true,
        "$type:array" => actual.is_array(),
        "$type:boolean" => actual.is_boolean(),
        "$type:integer" => actual.as_i64().is_some() || actual.as_u64().is_some(),
        "$type:null" => actual.is_null(),
        "$type:null-or-string" => actual.is_null() || actual.is_string(),
        "$type:number" => actual.is_number(),
        "$type:object" => actual.is_object(),
        "$type:string" => actual.is_string(),
        unsupported => panic!("unsupported golden marker `{unsupported}` at `{path}`"),
    };

    assert!(
        valid,
        "SABnzbd response type mismatch at `{path}`: expected `{marker}`, got {actual}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn normalizes_only_marked_dynamic_fields() {
        let expected = json!({"fixed": "Idle", "rate": "$type:string", "count": "$type:integer"});
        let mut actual = json!({"fixed": "Idle", "rate": "12.3 MB/s", "count": 4});

        normalize_dynamic_fields(&mut actual, &expected);

        assert_eq!(actual, expected);
    }

    #[test]
    #[should_panic(expected = "$.rate")]
    fn reports_the_path_of_a_dynamic_type_mismatch() {
        let expected = json!({"rate": "$type:string"});
        let mut actual = json!({"rate": 12});
        normalize_dynamic_fields(&mut actual, &expected);
    }
}
