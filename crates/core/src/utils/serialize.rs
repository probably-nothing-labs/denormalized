use arrow::datatypes::DataType;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

use datafusion_common::scalar::ScalarStructBuilder;
use datafusion_common::ScalarValue;

use arrow::array::*;
use arrow::datatypes::*;
use serde::Serializer;
use serde_json::json;

pub fn serialize_scalar_value<S>(value: &ScalarValue, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let json_value = match value {
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
            "value": v.as_ref().map(|b| base64::encode(b))
        }),
        ScalarValue::LargeBinary(v) => json!({
            "type": "LargeBinary",
            "value": v.as_ref().map(|b| base64::encode(b))
        }),
        ScalarValue::FixedSizeBinary(size, v) => json!({
            "type": "FixedSizeBinary",
            "size": size,
            "value": v.as_ref().map(|b| base64::encode(b))
        }),
        ScalarValue::List(v) => json!({
            "type": "List",
            "value": serialize_array(v)?
        }),
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
        ScalarValue::Struct(v) => json!({
            "type": "Struct",
            "fields": v.as_ref().fields().iter().map(|f| (f.name().to_string(), f.data_type().to_string())).collect::<Vec<_>>(),
            "value": v.as_ref().fields().iter().enumerate().map(|(i, field)| ScalarValue::try_from_array(field, 0).ok()).collect::<Vec<_>>(),

        }),
        // Add other variants as needed...
    };

    json_value.serialize(serializer)
}

fn serialize_array(arr: &Arc<dyn Array>) -> Result<Vec<Option<ScalarValue>>, S::Error> {
    (0..arr.len())
        .map(|i| {
            ScalarValue::try_from_array(arr.as_ref(), i)
                .map(Some)
                .map_err(serde::ser::Error::custom)
        })
        .collect()
}
use std::str::FromStr;

impl<'de> Deserialize<'de> for ScalarValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type")]
        enum ScalarValueHelper {
            Null,
            Boolean {
                value: Option<bool>,
            },
            Float16 {
                value: Option<f32>,
            },
            Float32 {
                value: Option<f32>,
            },
            Float64 {
                value: Option<f64>,
            },
            Decimal128 {
                value: Option<String>,
                precision: u8,
                scale: i8,
            },
            Decimal256 {
                value: Option<String>,
                precision: u8,
                scale: i8,
            },
            Int8 {
                value: Option<i8>,
            },
            Int16 {
                value: Option<i16>,
            },
            Int32 {
                value: Option<i32>,
            },
            Int64 {
                value: Option<i64>,
            },
            UInt8 {
                value: Option<u8>,
            },
            UInt16 {
                value: Option<u16>,
            },
            UInt32 {
                value: Option<u32>,
            },
            UInt64 {
                value: Option<u64>,
            },
            Utf8 {
                value: Option<String>,
            },
            LargeUtf8 {
                value: Option<String>,
            },
            Binary {
                value: Option<String>,
            },
            LargeBinary {
                value: Option<String>,
            },
            FixedSizeBinary {
                size: i32,
                value: Option<String>,
            },
            List {
                child_type: String,
                value: Option<Vec<Option<ScalarValue>>>,
            },
            LargeList {
                child_type: String,
                value: Option<Vec<Option<ScalarValue>>>,
            },
            FixedSizeList {
                child_type: String,
                size: usize,
                value: Option<Vec<Option<ScalarValue>>>,
            },
            Date32 {
                value: Option<i32>,
            },
            Date64 {
                value: Option<i64>,
            },
            Time32Second {
                value: Option<i32>,
            },
            Time32Millisecond {
                value: Option<i32>,
            },
            Time64Microsecond {
                value: Option<i64>,
            },
            Time64Nanosecond {
                value: Option<i64>,
            },
            Timestamp {
                value: Option<i64>,
                timezone: Option<String>,
                unit: String,
            },
            IntervalYearMonth {
                value: Option<i32>,
            },
            IntervalDayTime {
                value: Option<(i32, i32)>,
            },
            IntervalMonthDayNano {
                value: Option<(i32, i32, i64)>,
            },
            DurationSecond {
                value: Option<i64>,
            },
            DurationMillisecond {
                value: Option<i64>,
            },
            DurationMicrosecond {
                value: Option<i64>,
            },
            DurationNanosecond {
                value: Option<i64>,
            },
            Union {
                value: Option<(i8, Box<ScalarValue>)>,
                fields: Vec<(i8, String, String)>,
                mode: String,
            },
            Dictionary {
                key_type: String,
                value: Box<ScalarValue>,
            },
            Utf8View {
                value: Option<String>,
            },
            BinaryView {
                value: Option<String>,
            },
            Struct {
                fields: Vec<(String, String)>,
                value: Option<Vec<Option<ScalarValue>>>,
            },
        }

        let helper = ScalarValueHelper::deserialize(deserializer)?;

        Ok(match helper {
            ScalarValueHelper::Null => ScalarValue::Null,
            ScalarValueHelper::Boolean { value } => ScalarValue::Boolean(value),
            ScalarValueHelper::Float16 { value } => {
                ScalarValue::Float16(value.map(half::f16::from_f32))
            }
            ScalarValueHelper::Float32 { value } => ScalarValue::Float32(value),
            ScalarValueHelper::Float64 { value } => ScalarValue::Float64(value),
            ScalarValueHelper::Decimal128 {
                value,
                precision,
                scale,
            } => ScalarValue::Decimal128(
                value.map(|s| s.parse().unwrap()), //TODO: fix me
                precision,
                scale,
            ),
            ScalarValueHelper::Decimal256 {
                value,
                precision,
                scale,
            } => ScalarValue::Decimal256(value.map(|s| s.parse().unwrap()), precision, scale),
            ScalarValueHelper::Int8 { value } => ScalarValue::Int8(value),
            ScalarValueHelper::Int16 { value } => ScalarValue::Int16(value),
            ScalarValueHelper::Int32 { value } => ScalarValue::Int32(value),
            ScalarValueHelper::Int64 { value } => ScalarValue::Int64(value),
            ScalarValueHelper::UInt8 { value } => ScalarValue::UInt8(value),
            ScalarValueHelper::UInt16 { value } => ScalarValue::UInt16(value),
            ScalarValueHelper::UInt32 { value } => ScalarValue::UInt32(value),
            ScalarValueHelper::UInt64 { value } => ScalarValue::UInt64(value),
            ScalarValueHelper::Utf8 { value } => ScalarValue::Utf8(value),
            ScalarValueHelper::LargeUtf8 { value } => ScalarValue::LargeUtf8(value),
            ScalarValueHelper::Binary { value } => {
                ScalarValue::Binary(value.map(|s| base64::decode(s).unwrap()))
            }
            ScalarValueHelper::LargeBinary { value } => {
                ScalarValue::LargeBinary(value.map(|s| base64::decode(s).unwrap()))
            }
            ScalarValueHelper::FixedSizeBinary { size, value } => {
                ScalarValue::FixedSizeBinary(size, value.map(|s| base64::decode(s).unwrap()))
            }
            ScalarValueHelper::List { child_type, value } => {
                let field = Arc::new(Field::new(
                    "item",
                    DataType::from_str(&child_type).map_err(serde::de::Error::custom)?,
                    true,
                ));
                let values: Vec<Option<ScalarValue>> = value.unwrap_or_default();
                let scalar_values: Vec<ScalarValue> = values
                    .into_iter()
                    .map(|v| v.unwrap_or(ScalarValue::Null))
                    .collect();
                let data = ScalarValue::new_list(&scalar_values, &field.data_type());
                ScalarValue::List(data)
            }
            ScalarValueHelper::LargeList { child_type, value } => {
                let field = Arc::new(Field::new(
                    "item",
                    DataType::from_str(&child_type).map_err(serde::de::Error::custom)?,
                    true,
                ));
                let values: Vec<Option<ScalarValue>> = value.unwrap_or_default();
                let scalar_values: Vec<ScalarValue> = values
                    .into_iter()
                    .map(|v| v.unwrap_or(ScalarValue::Null))
                    .collect();
                ScalarValue::LargeList(ScalarValue::new_large_list(
                    &scalar_values,
                    &field.data_type(),
                ))
            }
            ScalarValueHelper::FixedSizeList {
                child_type,
                size,
                value,
            } => {
                let field = Arc::new(Field::new(
                    "item",
                    DataType::from_str(&child_type).map_err(serde::de::Error::custom)?,
                    true,
                ));
                let values: Vec<Option<ScalarValue>> = value.unwrap_or_default();
                let scalar_values: Vec<ScalarValue> = values
                    .into_iter()
                    .map(|v| v.unwrap_or(ScalarValue::Null))
                    .collect();
                let data_type = DataType::FixedSizeList(field, scalar_values.len() as i32);
                let value_data = ScalarValue::new_list(&scalar_values, &data_type).to_data();
                let list_array = FixedSizeListArray::from(value_data);
                ScalarValue::FixedSizeList(Arc::new(list_array))
            }
            ScalarValueHelper::Date32 { value } => ScalarValue::Date32(value),
            ScalarValueHelper::Date64 { value } => ScalarValue::Date64(value),
            ScalarValueHelper::Time32Second { value } => ScalarValue::Time32Second(value),
            ScalarValueHelper::Time32Millisecond { value } => ScalarValue::Time32Millisecond(value),
            ScalarValueHelper::Time64Microsecond { value } => ScalarValue::Time64Microsecond(value),
            ScalarValueHelper::Time64Nanosecond { value } => ScalarValue::Time64Nanosecond(value),
            ScalarValueHelper::Timestamp {
                value,
                timezone,
                unit,
            } => match unit.as_str() {
                "Second" => ScalarValue::TimestampSecond(value, timezone.map(Arc::from)),
                "Millisecond" => ScalarValue::TimestampMillisecond(value, timezone.map(Arc::from)),
                "Microsecond" => ScalarValue::TimestampMicrosecond(value, timezone.map(Arc::from)),
                "Nanosecond" => ScalarValue::TimestampNanosecond(value, timezone.map(Arc::from)),
                _ => return Err(serde::de::Error::custom("Invalid timestamp unit")),
            },
            ScalarValueHelper::IntervalYearMonth { value } => ScalarValue::IntervalYearMonth(value),
            ScalarValueHelper::IntervalDayTime { value } => ScalarValue::IntervalDayTime(
                value.map(|(days, millis)| IntervalDayTime::new(days, millis)),
            ),
            ScalarValueHelper::IntervalMonthDayNano { value } => ScalarValue::IntervalMonthDayNano(
                value.map(|(months, days, nanos)| IntervalMonthDayNano::new(months, days, nanos)),
            ),
            ScalarValueHelper::DurationSecond { value } => ScalarValue::DurationSecond(value),
            ScalarValueHelper::DurationMillisecond { value } => {
                ScalarValue::DurationMillisecond(value)
            }
            ScalarValueHelper::DurationMicrosecond { value } => {
                ScalarValue::DurationMicrosecond(value)
            }
            ScalarValueHelper::DurationNanosecond { value } => {
                ScalarValue::DurationNanosecond(value)
            }
            ScalarValueHelper::Union {
                value,
                fields,
                mode,
            } => {
                let union_fields = fields
                    .into_iter()
                    .map(|(i, name, type_str)| {
                        (
                            i,
                            Arc::new(Field::new(
                                name,
                                DataType::from_str(&type_str).unwrap(),
                                true,
                            )),
                        )
                    })
                    .collect();
                let union_mode = match mode.as_str() {
                    "Sparse" => UnionMode::Sparse,
                    "Dense" => UnionMode::Dense,
                    _ => return Err(serde::de::Error::custom("Invalid union mode")),
                };
                ScalarValue::Union(value, union_fields, union_mode)
            }
            ScalarValueHelper::Dictionary { key_type, value } => {
                ScalarValue::Dictionary(Box::new(DataType::from_str(&key_type).unwrap()), value)
            }
            ScalarValueHelper::Utf8View { value } => ScalarValue::Utf8View(value),
            ScalarValueHelper::BinaryView { value } => {
                ScalarValue::BinaryView(value.map(|s| base64::decode(s).unwrap()))
            }
            ScalarValueHelper::Struct { fields, value } => {
                let struct_fields: Vec<Field> = fields
                    .into_iter()
                    .map(|(name, type_str)| {
                        Field::new(name, DataType::from_str(&type_str).unwrap(), true)
                    })
                    .collect();
                let values: Vec<Option<ScalarValue>> = value.unwrap_or_default();
                let scalar_values: Vec<ScalarValue> = values
                    .into_iter()
                    .map(|v| v.unwrap_or(ScalarValue::Null))
                    .collect();
                let mut builder = ScalarStructBuilder::new();
                for (field, value) in struct_fields.clone().into_iter().zip(scalar_values) {
                    builder = builder.with_scalar(field, value);
                }
                let struct_array = builder.build().unwrap();

                ScalarValue::Struct(Arc::new(StructArray::new(
                    Fields::from(struct_fields),
                    vec![struct_array.to_array().unwrap()],
                    None,
                )))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::sync::Arc;

    fn test_serde_roundtrip(scalar: ScalarValue) {
        let serialized = serde_json::to_string(&scalar).unwrap();
        let deserialized: ScalarValue = serde_json::from_str(&serialized).unwrap();
        assert_eq!(scalar, deserialized);
    }

    #[test]
    fn test_large_utf8() {
        test_serde_roundtrip(ScalarValue::LargeUtf8(Some("hello".to_string())));
        test_serde_roundtrip(ScalarValue::LargeUtf8(None));
    }

    #[test]
    fn test_binary() {
        test_serde_roundtrip(ScalarValue::Binary(Some(vec![1, 2, 3])));
        test_serde_roundtrip(ScalarValue::Binary(None));
    }

    #[test]
    fn test_large_binary() {
        test_serde_roundtrip(ScalarValue::LargeBinary(Some(vec![1, 2, 3])));
        test_serde_roundtrip(ScalarValue::LargeBinary(None));
    }

    #[test]
    fn test_fixed_size_binary() {
        test_serde_roundtrip(ScalarValue::FixedSizeBinary(3, Some(vec![1, 2, 3])));
        test_serde_roundtrip(ScalarValue::FixedSizeBinary(3, None));
    }

    #[test]
    fn test_list() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
        ])]);
        test_serde_roundtrip(ScalarValue::List(Arc::new(list)));
    }

    #[test]
    fn test_large_list() {
        let list = LargeListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
        ])]);
        test_serde_roundtrip(ScalarValue::LargeList(Arc::new(list)));
    }

    // #[test]
    // fn test_fixed_size_list() {
    //     let list = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
    //         vec![Some(vec![Some(1), Some(2), Some(3)])],
    //         3,
    //     );
    //     test_serde_roundtrip(ScalarValue::FixedSizeList(Arc::new(list)));
    // }

    #[test]
    fn test_date32() {
        test_serde_roundtrip(ScalarValue::Date32(Some(1000)));
        test_serde_roundtrip(ScalarValue::Date32(None));
    }

    #[test]
    fn test_date64() {
        test_serde_roundtrip(ScalarValue::Date64(Some(86400000)));
        test_serde_roundtrip(ScalarValue::Date64(None));
    }

    #[test]
    fn test_time32_second() {
        test_serde_roundtrip(ScalarValue::Time32Second(Some(3600)));
        test_serde_roundtrip(ScalarValue::Time32Second(None));
    }

    #[test]
    fn test_time32_millisecond() {
        test_serde_roundtrip(ScalarValue::Time32Millisecond(Some(3600000)));
        test_serde_roundtrip(ScalarValue::Time32Millisecond(None));
    }

    #[test]
    fn test_time64_microsecond() {
        test_serde_roundtrip(ScalarValue::Time64Microsecond(Some(3600000000)));
        test_serde_roundtrip(ScalarValue::Time64Microsecond(None));
    }

    #[test]
    fn test_time64_nanosecond() {
        test_serde_roundtrip(ScalarValue::Time64Nanosecond(Some(3600000000000)));
        test_serde_roundtrip(ScalarValue::Time64Nanosecond(None));
    }

    #[test]
    fn test_timestamp() {
        test_serde_roundtrip(ScalarValue::TimestampSecond(
            Some(1625097600),
            Some(Arc::from("UTC")),
        ));
        test_serde_roundtrip(ScalarValue::TimestampMillisecond(Some(1625097600000), None));
        test_serde_roundtrip(ScalarValue::TimestampMicrosecond(
            Some(1625097600000000),
            Some(Arc::from("UTC")),
        ));
        test_serde_roundtrip(ScalarValue::TimestampNanosecond(
            Some(1625097600000000000),
            None,
        ));
    }

    #[test]
    fn test_interval_year_month() {
        test_serde_roundtrip(ScalarValue::IntervalYearMonth(Some(14)));
        test_serde_roundtrip(ScalarValue::IntervalYearMonth(None));
    }

    #[test]
    fn test_interval_day_time() {
        test_serde_roundtrip(ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
            5, 43200000,
        ))));
        test_serde_roundtrip(ScalarValue::IntervalDayTime(None));
    }

    #[test]
    fn test_interval_month_day_nano() {
        test_serde_roundtrip(ScalarValue::IntervalMonthDayNano(Some(
            IntervalMonthDayNano::new(1, 15, 1000000000),
        )));
        test_serde_roundtrip(ScalarValue::IntervalMonthDayNano(None));
    }

    #[test]
    fn test_duration() {
        test_serde_roundtrip(ScalarValue::DurationSecond(Some(3600)));
        test_serde_roundtrip(ScalarValue::DurationMillisecond(Some(3600000)));
        test_serde_roundtrip(ScalarValue::DurationMicrosecond(Some(3600000000)));
        test_serde_roundtrip(ScalarValue::DurationNanosecond(Some(3600000000000)));
    }

    // #[test]
    // fn test_union() {
    //     let fields = vec![
    //         (0, Arc::new(Field::new("f1", DataType::Int32, true))),
    //         (1, Arc::new(Field::new("f2", DataType::Utf8, true))),
    //     ];
    //     test_serde_roundtrip(ScalarValue::Union(
    //         Some((0, Box::new(ScalarValue::Int32(Some(42))))),
    //         fields.clone(),
    //         UnionMode::Sparse,
    //     ));
    //     test_serde_roundtrip(ScalarValue::Union(None, fields, UnionMode::Dense));
    // }

    #[test]
    fn test_dictionary() {
        test_serde_roundtrip(ScalarValue::Dictionary(
            Box::new(DataType::Int8),
            Box::new(ScalarValue::Utf8(Some("hello".to_string()))),
        ));
    }

    #[test]
    fn test_utf8_view() {
        test_serde_roundtrip(ScalarValue::Utf8View(Some("hello".to_string())));
        test_serde_roundtrip(ScalarValue::Utf8View(None));
    }

    #[test]
    fn test_binary_view() {
        test_serde_roundtrip(ScalarValue::BinaryView(Some(vec![1, 2, 3])));
        test_serde_roundtrip(ScalarValue::BinaryView(None));
    }

    /* #[test]
    fn test_struct() {
        let fields = vec![
            Field::new("f1", DataType::Int32, true),
            Field::new("f2", DataType::Utf8, true),
        ];
        let values = vec![
            Some(ScalarValue::Int32(Some(42))),
            Some(ScalarValue::Utf8(Some("hello".to_string()))),
        ];
        let struct_array = StructArray::from(values);
        test_serde_roundtrip(ScalarValue::Struct(Arc::new(struct_array)));
    } */
}
