use chrono::{DateTime, Local};
use serde_json::Value;

pub fn text(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("—")
        .to_owned()
}

pub fn official_security_name(value: &Value) -> String {
    ["description", "symbol"]
        .into_iter()
        .filter_map(|key| value.get(key).and_then(Value::as_str))
        .map(str::trim)
        .find(|value| !value.is_empty())
        .unwrap_or("—")
        .to_owned()
}

pub fn security_exchange(value: &Value) -> String {
    ["primary_exchange", "exchange"]
        .into_iter()
        .filter_map(|key| value.get(key).and_then(Value::as_str))
        .map(str::trim)
        .find(|value| !value.is_empty())
        .unwrap_or("—")
        .to_owned()
}

pub fn nested_text(value: &Value, pointer: &str) -> String {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or("—")
        .to_owned()
}

pub fn number(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_f64)
        .map(format_number)
        .unwrap_or_else(|| "—".into())
}

pub fn integer(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_i64)
        .map(|item| item.to_string())
        .unwrap_or_else(|| "—".into())
}

pub fn boolean(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

pub fn array(value: &Value, key: &str) -> Vec<Value> {
    value
        .get(key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

pub fn short_id(value: &Value, key: &str) -> String {
    let value = text(value, key);
    if value.len() > 12 {
        format!("{}…", &value[..12])
    } else {
        value
    }
}

pub fn format_number(value: f64) -> String {
    if value.fract().abs() < f64::EPSILON {
        format!("{value:.0}")
    } else {
        format!("{value:.4}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_owned()
    }
}

pub fn bytes(value: &Value, key: &str) -> String {
    let Some(value) = value.get(key).and_then(Value::as_u64) else {
        return "—".into();
    };
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut amount = value as f64;
    let mut unit = 0;
    while amount >= 1024.0 && unit < UNITS.len() - 1 {
        amount /= 1024.0;
        unit += 1;
    }
    format!("{amount:.1} {}", UNITS[unit])
}

pub fn local_time(value: &Value, key: &str) -> String {
    let Some(value) = value.get(key).and_then(Value::as_str) else {
        return "—".into();
    };
    format_local_time(value)
}

pub fn localize_json_times(value: &Value) -> Value {
    fn visit(key: Option<&str>, value: &Value) -> Value {
        match value {
            Value::Object(object) => Value::Object(
                object
                    .iter()
                    .map(|(key, value)| (key.clone(), visit(Some(key), value)))
                    .collect(),
            ),
            Value::Array(values) => {
                Value::Array(values.iter().map(|value| visit(key, value)).collect())
            }
            Value::String(value)
                if key.is_some_and(|key| {
                    key.ends_with("_at")
                        || key.ends_with("_time")
                        || matches!(key, "time" | "observed_at")
                }) =>
            {
                Value::String(format_local_time(value))
            }
            _ => value.clone(),
        }
    }
    visit(None, value)
}

fn format_local_time(value: &str) -> String {
    DateTime::parse_from_rfc3339(value)
        .map(|date| {
            date.with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        })
        .unwrap_or_else(|_| value.to_owned())
}
