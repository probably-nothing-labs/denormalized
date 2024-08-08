use std::sync::Arc;

use arrow_array::GenericListArray;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use datafusion::common::ScalarValue;

use arrow::{
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::*,
};
use half::f16;
use serde_json::{json, Value};

use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SerializableScalarValue(#[serde(with = "scalar_value_serde")] ScalarValue);

impl From<ScalarValue> for SerializableScalarValue {
    fn from(value: ScalarValue) -> Self {
        SerializableScalarValue(value)
    }
}

impl From<SerializableScalarValue> for ScalarValue {
    fn from(value: SerializableScalarValue) -> Self {
        value.0
    }
}

mod scalar_value_serde {
    use super::*;
    use serde::{de::Error, Deserializer, Serializer};

    pub fn serialize<S>(value: &ScalarValue, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let json = scalar_to_json(value);
        json.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ScalarValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        let json = serde_json::Value::deserialize(deserializer)?;
        json_to_scalar(&json).map_err(D::Error::custom)
    }
}

pub fn string_to_data_type(s: &str) -> Result<DataType, Box<dyn std::error::Error>> {
    match s {
        "Null" => Ok(DataType::Null),
        "Boolean" => Ok(DataType::Boolean),
        "Int8" => Ok(DataType::Int8),
        "Int16" => Ok(DataType::Int16),
        "Int32" => Ok(DataType::Int32),
        "Int64" => Ok(DataType::Int64),
        "UInt8" => Ok(DataType::UInt8),
        "UInt16" => Ok(DataType::UInt16),
        "UInt32" => Ok(DataType::UInt32),
        "UInt64" => Ok(DataType::UInt64),
        "Float16" => Ok(DataType::Float16),
        "Float32" => Ok(DataType::Float32),
        "Float64" => Ok(DataType::Float64),
        "Binary" => Ok(DataType::Binary),
        "LargeBinary" => Ok(DataType::LargeBinary),
        "Utf8" => Ok(DataType::Utf8),
        "LargeUtf8" => Ok(DataType::LargeUtf8),
        "Date32" => Ok(DataType::Date32),
        "Date64" => Ok(DataType::Date64),
        s if s.starts_with("Timestamp(") => {
            let parts: Vec<&str> = s[10..s.len() - 1].split(',').collect();
            if parts.len() != 2 {
                return Err("Invalid Timestamp format".into());
            }
            let time_unit = match parts[0].trim() {
                "Second" => TimeUnit::Second,
                "Millisecond" => TimeUnit::Millisecond,
                "Microsecond" => TimeUnit::Microsecond,
                "Nanosecond" => TimeUnit::Nanosecond,
                _ => return Err("Invalid TimeUnit".into()),
            };
            let timezone = parts[1].trim().trim_matches('"');
            let timezone = if timezone == "None" {
                None
            } else {
                Some(timezone.into())
            };
            Ok(DataType::Timestamp(time_unit, timezone))
        }
        s if s.starts_with("Time32(") => {
            let time_unit = match &s[7..s.len() - 1] {
                "Second" => TimeUnit::Second,
                "Millisecond" => TimeUnit::Millisecond,
                _ => return Err("Invalid TimeUnit for Time32".into()),
            };
            Ok(DataType::Time32(time_unit))
        }
        s if s.starts_with("Time64(") => {
            let time_unit = match &s[7..s.len() - 1] {
                "Microsecond" => TimeUnit::Microsecond,
                "Nanosecond" => TimeUnit::Nanosecond,
                _ => return Err("Invalid TimeUnit for Time64".into()),
            };
            Ok(DataType::Time64(time_unit))
        }
        s if s.starts_with("Duration(") => {
            let time_unit = match &s[9..s.len() - 1] {
                "Second" => TimeUnit::Second,
                "Millisecond" => TimeUnit::Millisecond,
                "Microsecond" => TimeUnit::Microsecond,
                "Nanosecond" => TimeUnit::Nanosecond,
                _ => return Err("Invalid TimeUnit for Duration".into()),
            };
            Ok(DataType::Duration(time_unit))
        }
        s if s.starts_with("Interval(") => {
            let interval_unit = match &s[9..s.len() - 1] {
                "YearMonth" => IntervalUnit::YearMonth,
                "DayTime" => IntervalUnit::DayTime,
                "MonthDayNano" => IntervalUnit::MonthDayNano,
                _ => return Err("Invalid IntervalUnit".into()),
            };
            Ok(DataType::Interval(interval_unit))
        }
        s if s.starts_with("FixedSizeBinary(") => {
            let size: i32 = s[16..s.len() - 1].parse()?;
            Ok(DataType::FixedSizeBinary(size))
        }
        s if s.starts_with("List(") => {
            let inner_type = string_to_data_type(&s[5..s.len() - 1])?;
            Ok(DataType::List(Arc::new(Field::new(
                "item", inner_type, true,
            ))))
        }
        s if s.starts_with("LargeList(") => {
            let inner_type = string_to_data_type(&s[10..s.len() - 1])?;
            Ok(DataType::LargeList(Arc::new(Field::new(
                "item", inner_type, true,
            ))))
        }
        s if s.starts_with("FixedSizeList(") => {
            let parts: Vec<&str> = s[14..s.len() - 1].split(',').collect();
            if parts.len() != 2 {
                return Err("Invalid FixedSizeList format".into());
            }
            let inner_type = string_to_data_type(parts[0].trim())?;
            let size: i32 = parts[1].trim().parse()?;
            Ok(DataType::FixedSizeList(
                Arc::new(Field::new("item", inner_type, true)),
                size,
            ))
        }
        s if s.starts_with("Decimal128(") => {
            let parts: Vec<&str> = s[10..s.len() - 1].split(',').collect();
            if parts.len() != 2 {
                return Err("Invalid Decimal128 format".into());
            }
            let precision: u8 = parts[0].trim().parse()?;
            let scale: i8 = parts[1].trim().parse()?;
            Ok(DataType::Decimal128(precision, scale))
        }
        s if s.starts_with("Decimal256(") => {
            let parts: Vec<&str> = s[10..s.len() - 1].split(',').collect();
            if parts.len() != 2 {
                return Err("Invalid Decimal256 format".into());
            }
            let precision: u8 = parts[0].trim().parse()?;
            let scale: i8 = parts[1].trim().parse()?;
            Ok(DataType::Decimal256(precision, scale))
        }
        _ => Err(format!("Unsupported DataType string: {}", s).into()),
    }
}

pub fn scalar_to_json(value: &ScalarValue) -> serde_json::Value {
    match value {
        ScalarValue::Null => json!({"type": "Null"}),
        ScalarValue::Boolean(v) => json!({"type": "Boolean", "value": v}),
        ScalarValue::Float16(v) => json!({"type": "Float16", "value": v.map(|f| f.to_f32())}),
        ScalarValue::Float32(v) => json!({"type": "Float32", "value": v}),
        ScalarValue::Float64(v) => json!({"type": "Float64", "value": v}),
        ScalarValue::Decimal128(v, p, s) => json!({
            "type": "Decimal128",
            "value": v.map(|x| x.to_string()),
            "precision": p,
            "scale": s
        }),
        ScalarValue::Decimal256(v, p, s) => json!({
            "type": "Decimal256",
            "value": v.map(|x| x.to_string()),
            "precision": p,
            "scale": s
        }),
        ScalarValue::Int8(v) => json!({"type": "Int8", "value": v}),
        ScalarValue::Int16(v) => json!({"type": "Int16", "value": v}),
        ScalarValue::Int32(v) => json!({"type": "Int32", "value": v}),
        ScalarValue::Int64(v) => json!({"type": "Int64", "value": v}),
        ScalarValue::UInt8(v) => json!({"type": "UInt8", "value": v}),
        ScalarValue::UInt16(v) => json!({"type": "UInt16", "value": v}),
        ScalarValue::UInt32(v) => json!({"type": "UInt32", "value": v}),
        ScalarValue::UInt64(v) => json!({"type": "UInt64", "value": v}),
        ScalarValue::Utf8(v) => json!({"type": "Utf8", "value": v}),
        ScalarValue::LargeUtf8(v) => json!({"type": "LargeUtf8", "value": v}),
        ScalarValue::Binary(v) => json!({
            "type": "Binary",
            "value": v.as_ref().map(|b| STANDARD.encode(b))
        }),
        ScalarValue::LargeBinary(v) => json!({
            "type": "LargeBinary",
            "value": v.as_ref().map(|b| STANDARD.encode(b))
        }),
        ScalarValue::FixedSizeBinary(size, v) => json!({
            "type": "FixedSizeBinary",
            "size": size,
            "value": v.as_ref().map(|b| STANDARD.encode(b))
        }),
        ScalarValue::List(v) => {
            let sv = ScalarValue::try_from_array(&v.value(0), 0).unwrap();
            let dt = sv.data_type().to_string();
            json!({
                "type": "List",
                "field_type": dt,
                "value": scalar_to_json(&sv)
            })
        }
        ScalarValue::Date32(v) => json!({"type": "Date32", "value": v}),
        ScalarValue::Date64(v) => json!({"type": "Date64", "value": v}),
        ScalarValue::Time32Second(v) => json!({"type": "Time32Second", "value": v}),
        ScalarValue::Time32Millisecond(v) => json!({"type": "Time32Millisecond", "value": v}),
        ScalarValue::Time64Microsecond(v) => json!({"type": "Time64Microsecond", "value": v}),
        ScalarValue::Time64Nanosecond(v) => json!({"type": "Time64Nanosecond", "value": v}),
        ScalarValue::TimestampSecond(v, tz)
        | ScalarValue::TimestampMillisecond(v, tz)
        | ScalarValue::TimestampMicrosecond(v, tz)
        | ScalarValue::TimestampNanosecond(v, tz) => json!({
            "type": "Timestamp",
            "value": v,
            "timezone": tz.as_ref().map(|s| s.to_string()),
            "unit": match value {
                ScalarValue::TimestampSecond(_, _) => "Second",
                ScalarValue::TimestampMillisecond(_, _) => "Millisecond",
                ScalarValue::TimestampMicrosecond(_, _) => "Microsecond",
                ScalarValue::TimestampNanosecond(_, _) => "Nanosecond",
                _ => unreachable!(),
            }
        }),
        ScalarValue::IntervalYearMonth(v) => json!({"type": "IntervalYearMonth", "value": v}),
        ScalarValue::IntervalDayTime(v) => json!({
            "type": "IntervalDayTime",
            "value": v.map(|x| (x.days, x.milliseconds))
        }),
        ScalarValue::IntervalMonthDayNano(v) => json!({
            "type": "IntervalMonthDayNano",
            "value": v.map(|x| (x.months, x.days, x.nanoseconds))
        }),
        ScalarValue::DurationSecond(v) => json!({"type": "DurationSecond", "value": v}),
        ScalarValue::DurationMillisecond(v) => json!({"type": "DurationMillisecond", "value": v}),
        ScalarValue::DurationMicrosecond(v) => json!({"type": "DurationMicrosecond", "value": v}),
        ScalarValue::DurationNanosecond(v) => json!({"type": "DurationNanosecond", "value": v}),
        ScalarValue::Struct(v) => {
            let fields = v
                .as_ref()
                .fields()
                .iter()
                .map(|f| (f.name().to_string(), f.data_type().to_string()))
                .collect::<Vec<_>>();

            let values = v
                .columns()
                .as_ref()
                .iter()
                .map(|c| {
                    let sv = ScalarValue::try_from_array(c, 0).unwrap();
                    scalar_to_json(&sv)
                })
                .collect::<Vec<_>>();
            json!({"type" : "Struct", "fields": fields, "values": values})
        }
        ScalarValue::Utf8View(_) => todo!(),
        ScalarValue::BinaryView(_) => todo!(),
        ScalarValue::FixedSizeList(_) => todo!(),
        ScalarValue::LargeList(_) => todo!(),
        ScalarValue::Map(_) => todo!(),
        ScalarValue::Union(_, _, _) => todo!(),
        ScalarValue::Dictionary(_, _) => todo!(),
    }
}

pub fn json_to_scalar(json: &Value) -> Result<ScalarValue, Box<dyn std::error::Error>> {
    let obj = json.as_object().ok_or("Expected JSON object")?;
    let typ = obj
        .get("type")
        .and_then(Value::as_str)
        .ok_or("Missing or invalid 'type'")?;

    match typ {
        "Null" => Ok(ScalarValue::Null),
        "Boolean" => Ok(ScalarValue::Boolean(
            obj.get("value").and_then(Value::as_bool),
        )),
        "Float16" => Ok(ScalarValue::Float16(
            obj.get("value")
                .and_then(Value::as_f64)
                .map(|f| f16::from_f32(f as f32)),
        )),
        "Float32" => Ok(ScalarValue::Float32(
            obj.get("value").and_then(Value::as_f64).map(|f| f as f32),
        )),
        "Float64" => Ok(ScalarValue::Float64(
            obj.get("value").and_then(Value::as_f64),
        )),
        "Decimal128" => {
            let value = obj
                .get("value")
                .and_then(Value::as_str)
                .map(|s| s.parse::<i128>().unwrap());
            let precision = obj.get("precision").and_then(Value::as_u64).unwrap() as u8;
            let scale = obj.get("scale").and_then(Value::as_i64).unwrap() as i8;
            Ok(ScalarValue::Decimal128(value, precision, scale))
        }
        "Decimal256" => {
            let value = obj
                .get("value")
                .and_then(Value::as_str)
                .map(|s| s.parse::<i256>().unwrap());
            let precision = obj.get("precision").and_then(Value::as_u64).unwrap() as u8;
            let scale = obj.get("scale").and_then(Value::as_i64).unwrap() as i8;
            Ok(ScalarValue::Decimal256(value, precision, scale))
        }
        "Int8" => Ok(ScalarValue::Int8(
            obj.get("value").and_then(Value::as_i64).map(|i| i as i8),
        )),
        "Int16" => Ok(ScalarValue::Int16(
            obj.get("value").and_then(Value::as_i64).map(|i| i as i16),
        )),
        "Int32" => Ok(ScalarValue::Int32(
            obj.get("value").and_then(Value::as_i64).map(|i| i as i32),
        )),
        "Int64" => Ok(ScalarValue::Int64(obj.get("value").and_then(Value::as_i64))),
        "UInt8" => Ok(ScalarValue::UInt8(
            obj.get("value").and_then(Value::as_u64).map(|i| i as u8),
        )),
        "UInt16" => Ok(ScalarValue::UInt16(
            obj.get("value").and_then(Value::as_u64).map(|i| i as u16),
        )),
        "UInt32" => Ok(ScalarValue::UInt32(
            obj.get("value").and_then(Value::as_u64).map(|i| i as u32),
        )),
        "UInt64" => Ok(ScalarValue::UInt64(
            obj.get("value").and_then(Value::as_u64),
        )),
        "Utf8" => Ok(ScalarValue::Utf8(
            obj.get("value").and_then(Value::as_str).map(String::from),
        )),
        "LargeUtf8" => Ok(ScalarValue::LargeUtf8(
            obj.get("value").and_then(Value::as_str).map(String::from),
        )),
        "Binary" => Ok(ScalarValue::Binary(
            obj.get("value")
                .and_then(Value::as_str)
                .map(|s| STANDARD.decode(s).unwrap()),
        )),
        "LargeBinary" => Ok(ScalarValue::LargeBinary(
            obj.get("value")
                .and_then(Value::as_str)
                .map(|s| STANDARD.decode(s).unwrap()),
        )),
        "FixedSizeBinary" => {
            let size = obj.get("size").and_then(Value::as_u64).unwrap() as i32;
            let value = obj
                .get("value")
                .and_then(Value::as_str)
                .map(|s| STANDARD.decode(s).unwrap());
            Ok(ScalarValue::FixedSizeBinary(size, value))
        }
        "List" => {
            let value = obj.get("value").ok_or("Missing 'value' for List")?;
            let field_type = obj
                .get("field_type")
                .map(|ft| ft.as_str())
                .ok_or("Missing 'field_type' for List")?;
            let dt: DataType = string_to_data_type(field_type.unwrap())?;
            let element: ScalarValue = json_to_scalar(value)?;
            let array = element.to_array_of_size(1).unwrap();
            let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0]));
            let field = Field::new("item", dt, true);
            let list = GenericListArray::try_new(Arc::new(field), offsets, array, None)?;
            Ok(ScalarValue::List(Arc::new(list)))
        }
        "Date32" => Ok(ScalarValue::Date32(
            obj.get("value").and_then(Value::as_i64).map(|i| i as i32),
        )),
        "Date64" => Ok(ScalarValue::Date64(
            obj.get("value").and_then(Value::as_i64),
        )),
        "Time32Second" => Ok(ScalarValue::Time32Second(
            obj.get("value").and_then(Value::as_i64).map(|i| i as i32),
        )),
        "Time32Millisecond" => Ok(ScalarValue::Time32Millisecond(
            obj.get("value").and_then(Value::as_i64).map(|i| i as i32),
        )),
        "Time64Microsecond" => Ok(ScalarValue::Time64Microsecond(
            obj.get("value").and_then(Value::as_i64),
        )),
        "Time64Nanosecond" => Ok(ScalarValue::Time64Nanosecond(
            obj.get("value").and_then(Value::as_i64),
        )),
        "Timestamp" => {
            let value = obj.get("value").and_then(Value::as_i64);
            let timezone = obj
                .get("timezone")
                .and_then(Value::as_str)
                .map(|s| s.to_string().into());
            let unit = obj
                .get("unit")
                .and_then(Value::as_str)
                .ok_or("Missing or invalid 'unit'")?;
            match unit {
                "Second" => Ok(ScalarValue::TimestampSecond(value, timezone)),
                "Millisecond" => Ok(ScalarValue::TimestampMillisecond(value, timezone)),
                "Microsecond" => Ok(ScalarValue::TimestampMicrosecond(value, timezone)),
                "Nanosecond" => Ok(ScalarValue::TimestampNanosecond(value, timezone)),
                _ => Err("Invalid timestamp unit".into()),
            }
        }
        "IntervalYearMonth" => Ok(ScalarValue::IntervalYearMonth(
            obj.get("value").and_then(Value::as_i64).map(|i| i as i32),
        )),
        // "IntervalDayTime" => {
        //     let value = obj
        //         .get("value")
        //         .and_then(Value::as_array)
        //         .map(|arr| {
        //             if arr.len() == 2 {
        //                 Some(arrow_buffer::IntervalDayTime::make_value(
        //                     arr[0].as_i64().unwrap() as i32,
        //                     arr[1].as_i64().unwrap() as i32,
        //                 ))
        //             } else {
        //                 None
        //             }
        //         })
        //         .flatten();
        //     Ok(ScalarValue::IntervalDayTime(value))
        // }
        // "IntervalMonthDayNano" => {
        //     let value = obj
        //         .get("value")
        //         .and_then(Value::as_array)
        //         .map(|arr| {
        //             if arr.len() == 3 {
        //                 Some(arrow_buffer::IntervalMonthDayNano::make_value(
        //                     arr[0].as_i64().unwrap() as i32,
        //                     arr[1].as_i64().unwrap() as i32,
        //                     arr[2].as_i64().unwrap(),
        //                 ))
        //             } else {
        //                 None
        //             }
        //         })
        //         .flatten();
        //     Ok(ScalarValue::IntervalMonthDayNano(value))
        // }
        "DurationSecond" => Ok(ScalarValue::DurationSecond(
            obj.get("value").and_then(Value::as_i64),
        )),
        "DurationMillisecond" => Ok(ScalarValue::DurationMillisecond(
            obj.get("value").and_then(Value::as_i64),
        )),
        "DurationMicrosecond" => Ok(ScalarValue::DurationMicrosecond(
            obj.get("value").and_then(Value::as_i64),
        )),
        "DurationNanosecond" => Ok(ScalarValue::DurationNanosecond(
            obj.get("value").and_then(Value::as_i64),
        )),
        // "Struct" => {
        //     let fields = obj
        //         .get("fields")
        //         .and_then(Value::as_array)
        //         .ok_or("Missing or invalid 'fields'")?;
        //     let values = obj
        //         .get("values")
        //         .and_then(Value::as_array)
        //         .ok_or("Missing or invalid 'values'")?;

        //     let field_vec: Vec<Field> = fields
        //         .iter()
        //         .map(|f| {
        //             let name = f[0].as_str().unwrap().to_string();
        //             let data_type = f[1].as_str().unwrap().parse().unwrap();
        //             Field::new(name, data_type, true)
        //         })
        //         .collect();

        //     let value_vec: Vec<ArrayRef> = values
        //         .iter()
        //         .map(|v| {
        //             let scalar = json_to_scalar(v).unwrap();
        //             scalar.to_array().unwrap()
        //         })
        //         .collect();

        //     let struct_array = StructArray::from((field_vec, value_vec));
        //     Ok(ScalarValue::Struct(Arc::new(struct_array)))
        // }
        _ => Err(format!("Unsupported type: {}", typ).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;

    fn test_roundtrip(scalar: ScalarValue) {
        let json = scalar_to_json(&scalar);
        let roundtrip = json_to_scalar(&json).unwrap();
        assert_eq!(scalar, roundtrip, "Failed roundtrip for {:?}", scalar);
    }

    #[test]
    fn test_null() {
        test_roundtrip(ScalarValue::Null);
    }

    #[test]
    fn test_boolean() {
        test_roundtrip(ScalarValue::Boolean(Some(true)));
        test_roundtrip(ScalarValue::Boolean(Some(false)));
        test_roundtrip(ScalarValue::Boolean(None));
    }

    #[test]
    fn test_float() {
        test_roundtrip(ScalarValue::Float32(Some(3.24)));
        test_roundtrip(ScalarValue::Float32(None));
        test_roundtrip(ScalarValue::Float64(Some(3.24159265359)));
        test_roundtrip(ScalarValue::Float64(None));
    }

    #[test]
    fn test_int() {
        test_roundtrip(ScalarValue::Int8(Some(42)));
        test_roundtrip(ScalarValue::Int8(None));
        test_roundtrip(ScalarValue::Int16(Some(-1000)));
        test_roundtrip(ScalarValue::Int16(None));
        test_roundtrip(ScalarValue::Int32(Some(1_000_000)));
        test_roundtrip(ScalarValue::Int32(None));
        test_roundtrip(ScalarValue::Int64(Some(-1_000_000_000)));
        test_roundtrip(ScalarValue::Int64(None));
    }

    #[test]
    fn test_uint() {
        test_roundtrip(ScalarValue::UInt8(Some(255)));
        test_roundtrip(ScalarValue::UInt8(None));
        test_roundtrip(ScalarValue::UInt16(Some(65535)));
        test_roundtrip(ScalarValue::UInt16(None));
        test_roundtrip(ScalarValue::UInt32(Some(4_294_967_295)));
        test_roundtrip(ScalarValue::UInt32(None));
        test_roundtrip(ScalarValue::UInt64(Some(18_446_744_073_709_551_615)));
        test_roundtrip(ScalarValue::UInt64(None));
    }

    #[test]
    fn test_utf8() {
        test_roundtrip(ScalarValue::Utf8(Some("Hello, World!".to_string())));
        test_roundtrip(ScalarValue::Utf8(None));
        test_roundtrip(ScalarValue::LargeUtf8(Some("大きな文字列".to_string())));
        test_roundtrip(ScalarValue::LargeUtf8(None));
    }

    #[test]
    fn test_binary() {
        test_roundtrip(ScalarValue::Binary(Some(vec![0, 1, 2, 3, 4])));
        test_roundtrip(ScalarValue::Binary(None));
        test_roundtrip(ScalarValue::LargeBinary(Some(vec![
            255, 254, 253, 252, 251,
        ])));
        test_roundtrip(ScalarValue::LargeBinary(None));
        test_roundtrip(ScalarValue::FixedSizeBinary(
            5,
            Some(vec![10, 20, 30, 40, 50]),
        ));
        test_roundtrip(ScalarValue::FixedSizeBinary(5, None));
    }

    // #[test]
    // fn test_list() {
    //     let inner = ScalarValue::Int32(Some(42));
    //     let list = ScalarValue::List(Arc::new(inner.to_array().unwrap()));
    //     test_roundtrip(list);
    // }

    #[test]
    fn test_timestamp() {
        test_roundtrip(ScalarValue::TimestampSecond(
            Some(1625097600),
            Some("UTC".into()),
        ));
        test_roundtrip(ScalarValue::TimestampMillisecond(Some(1625097600000), None));
        test_roundtrip(ScalarValue::TimestampMicrosecond(
            Some(1625097600000000),
            Some("America/New_York".into()),
        ));
        test_roundtrip(ScalarValue::TimestampNanosecond(
            Some(1625097600000000000),
            None,
        ));
    }

    #[test]
    fn test_serializable_scalar_value() {
        let original = ScalarValue::Int32(Some(42));
        let serializable = SerializableScalarValue::from(original.clone());

        // Serialize
        let serialized = serde_json::to_string(&serializable).unwrap();

        // Deserialize
        let deserialized: SerializableScalarValue = serde_json::from_str(&serialized).unwrap();

        // Convert back to ScalarValue
        let result: ScalarValue = deserialized.into();

        assert_eq!(original, result);
    }
}
