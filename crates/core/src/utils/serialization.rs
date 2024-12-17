use std::sync::Arc;

use arrow::{array::ArrayData, buffer::Buffer};
use arrow_array::{make_array, Array, ArrayRef};
use arrow_schema::{DataType, Field};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct SerializedArrayData {
    data_type: SerializedDataType,
    len: usize,
    null_bit_buffer: Option<Vec<u8>>,
    offset: usize,
    buffers: Vec<Vec<u8>>,
    child_data: Vec<SerializedArrayData>,
}

#[derive(Serialize, Deserialize, Clone)]
enum SerializedDataType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Utf8,
    List(Box<SerializedField>),
    Struct(Vec<SerializedField>),
    Dictionary(Box<SerializedDataType>, Box<SerializedDataType>),
}

#[derive(Serialize, Deserialize, Clone)]
struct SerializedField {
    name: String,
    data_type: SerializedDataType,
    nullable: bool,
}

impl From<&DataType> for SerializedDataType {
    fn from(dt: &DataType) -> Self {
        match dt {
            DataType::Null => SerializedDataType::Null,
            DataType::Boolean => SerializedDataType::Boolean,
            DataType::Int8 => SerializedDataType::Int8,
            DataType::Int16 => SerializedDataType::Int16,
            DataType::Int32 => SerializedDataType::Int32,
            DataType::Int64 => SerializedDataType::Int64,
            DataType::UInt8 => SerializedDataType::UInt8,
            DataType::UInt16 => SerializedDataType::UInt16,
            DataType::UInt32 => SerializedDataType::UInt32,
            DataType::UInt64 => SerializedDataType::UInt64,
            DataType::Float16 => SerializedDataType::Float16,
            DataType::Float32 => SerializedDataType::Float32,
            DataType::Float64 => SerializedDataType::Float64,
            DataType::Utf8 => SerializedDataType::Utf8,
            DataType::List(field) => {
                SerializedDataType::List(Box::new(SerializedField::from(field.as_ref())))
            }
            DataType::Struct(fields) => SerializedDataType::Struct(
                fields
                    .iter()
                    .map(|f| SerializedField {
                        name: f.name().clone(),
                        data_type: SerializedDataType::from(f.data_type()),
                        nullable: f.is_nullable(),
                    })
                    .collect(),
            ),
            DataType::Dictionary(key_type, value_type) => SerializedDataType::Dictionary(
                Box::new(SerializedDataType::from(key_type.as_ref())),
                Box::new(SerializedDataType::from(value_type.as_ref())),
            ),
            // Add other types as needed
            _ => unimplemented!("Serialization not implemented for this data type"),
        }
    }
}

impl From<SerializedDataType> for DataType {
    fn from(sdt: SerializedDataType) -> Self {
        match sdt {
            SerializedDataType::Null => DataType::Null,
            SerializedDataType::Boolean => DataType::Boolean,
            SerializedDataType::Int8 => DataType::Int8,
            SerializedDataType::Int16 => DataType::Int16,
            SerializedDataType::Int32 => DataType::Int32,
            SerializedDataType::Int64 => DataType::Int64,
            SerializedDataType::UInt8 => DataType::UInt8,
            SerializedDataType::UInt16 => DataType::UInt16,
            SerializedDataType::UInt32 => DataType::UInt32,
            SerializedDataType::UInt64 => DataType::UInt64,
            SerializedDataType::Float16 => DataType::Float16,
            SerializedDataType::Float32 => DataType::Float32,
            SerializedDataType::Float64 => DataType::Float64,
            SerializedDataType::Utf8 => DataType::Utf8,
            SerializedDataType::List(field) => DataType::List(Arc::new(Field::from(*field))),
            SerializedDataType::Struct(fields) => {
                DataType::Struct(fields.into_iter().map(Field::from).collect())
            }
            SerializedDataType::Dictionary(key_type, value_type) => {
                DataType::Dictionary(Box::new((*key_type).into()), Box::new((*value_type).into()))
            }
        }
    }
}

impl From<&Field> for SerializedField {
    fn from(field: &Field) -> Self {
        SerializedField {
            name: field.name().to_string(),
            data_type: SerializedDataType::from(field.data_type()),
            nullable: field.is_nullable(),
        }
    }
}

impl From<SerializedField> for Field {
    fn from(sf: SerializedField) -> Self {
        Field::new(sf.name, sf.data_type.into(), sf.nullable)
    }
}

pub fn serialize_array(array: &ArrayRef) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let array_data = array.to_data();
    let serialized = serialize_array_data(&array_data)?;
    bincode::serialize(&serialized).map_err(|e| e.into())
}

fn serialize_array_data(
    array_data: &ArrayData,
) -> Result<SerializedArrayData, Box<dyn std::error::Error>> {
    Ok(SerializedArrayData {
        data_type: SerializedDataType::from(array_data.data_type()),
        len: array_data.len(),
        null_bit_buffer: array_data.nulls().map(|n| n.buffer().as_slice().to_vec()),
        offset: array_data.offset(),
        buffers: array_data
            .buffers()
            .iter()
            .map(|b| b.as_slice().to_vec())
            .collect(),
        child_data: array_data
            .child_data()
            .iter()
            .map(serialize_array_data)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub fn deserialize_array(bytes: &[u8]) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    let serialized: SerializedArrayData = bincode::deserialize(bytes)?;
    let array_data = deserialize_array_data(&serialized)?;
    Ok(make_array(array_data))
}

fn deserialize_array_data(
    serialized: &SerializedArrayData,
) -> Result<ArrayData, Box<dyn std::error::Error>> {
    let data_type: DataType = serialized.data_type.clone().into();

    if serialized.len == 0 {
        return Ok(ArrayData::new_empty(&data_type));
    }

    let buffers: Vec<Buffer> = serialized
        .buffers
        .iter()
        .map(|buf| Buffer::from_vec(buf.clone()))
        .collect();

    let child_data: Vec<ArrayData> = serialized
        .child_data
        .iter()
        .map(deserialize_array_data)
        .collect::<Result<Vec<_>, _>>()?;

    let null_buffer = serialized
        .null_bit_buffer
        .as_ref()
        .map(|buf| Buffer::from_vec(buf.clone()));

    ArrayData::try_new(
        data_type,
        serialized.len,
        null_buffer,
        serialized.offset,
        buffers,
        child_data,
    )
    .map_err(|e| e.into())
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ArrayContainer {
    #[serde(with = "array_ref_serialization")]
    pub arrays: Vec<ArrayRef>,
}
impl ArrayContainer {
    fn new(arrays: Vec<ArrayRef>) -> Self {
        Self { arrays }
    }
}

mod array_ref_serialization {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(arrays: &Vec<ArrayRef>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let serialized_arrays: Vec<Vec<u8>> = arrays
            .iter()
            .map(|arr| serialize_array(arr).expect("Failed to serialize array"))
            .collect();
        serialized_arrays.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<ArrayRef>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let serialized_arrays: Vec<Vec<u8>> = Vec::deserialize(deserializer)?;
        serialized_arrays
            .into_iter()
            .map(|bytes| deserialize_array(&bytes).map_err(serde::de::Error::custom))
            .collect()
    }
}

#[derive(Serialize, Deserialize)]
struct SerializedArrayContainer {
    serialized_arrays: Vec<Vec<u8>>,
}

impl From<&ArrayContainer> for SerializedArrayContainer {
    fn from(container: &ArrayContainer) -> Self {
        let serialized_arrays = container
            .arrays
            .iter()
            .map(|arr| serialize_array(arr).expect("Failed to serialize array"))
            .collect();

        SerializedArrayContainer { serialized_arrays }
    }
}

impl TryFrom<SerializedArrayContainer> for ArrayContainer {
    type Error = Box<dyn std::error::Error>;

    fn try_from(serialized: SerializedArrayContainer) -> Result<Self, Self::Error> {
        let arrays = serialized
            .serialized_arrays
            .into_iter()
            .map(|bytes| deserialize_array(&bytes))
            .collect::<Result<Vec<ArrayRef>, _>>()?;

        Ok(ArrayContainer::new(arrays))
    }
}

pub fn serialize_array_container(
    container: &ArrayContainer,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let serialized = SerializedArrayContainer::from(container);
    bincode::serialize(&serialized).map_err(|e| e.into())
}

pub fn deserialize_array_container(
    bytes: &[u8],
) -> Result<ArrayContainer, Box<dyn std::error::Error>> {
    let serialized: SerializedArrayContainer = bincode::deserialize(bytes)?;
    ArrayContainer::try_from(serialized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::AsArray,
        datatypes::{
            ArrowDictionaryKeyType, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
            UInt16Type, UInt32Type, UInt64Type, UInt8Type,
        },
    };
    use arrow_array::{
        Array, ArrowPrimitiveType, BooleanArray, DictionaryArray, Float64Array, Int32Array,
        ListArray, StringArray, StructArray,
    };
    use arrow_schema::{Field, Fields};
    use datafusion::{
        functions_aggregate::average::AvgAccumulator, logical_expr::Accumulator,
        scalar::ScalarValue,
    };
    use std::sync::Arc;

    fn test_roundtrip<A: Array + Clone + 'static>(array: A) {
        let array_ref: ArrayRef = Arc::new(array.clone());
        let serialized = serialize_array(&array_ref).unwrap();
        let deserialized: ArrayRef = deserialize_array(&serialized).unwrap();

        assert_eq!(array_ref.len(), deserialized.len());
        assert_eq!(array_ref.data_type(), deserialized.data_type());
        assert_eq!(array_ref.null_count(), deserialized.null_count());

        // Compare the actual data
        compare_arrays(&array_ref, &deserialized);
    }

    fn compare_arrays(left: &ArrayRef, right: &ArrayRef) {
        assert_eq!(left.len(), right.len());
        assert_eq!(left.data_type(), right.data_type());

        for i in 0..left.len() {
            assert_eq!(left.is_null(i), right.is_null(i));
            if !left.is_null(i) {
                compare_array_values(left, right, i);
            }
        }
    }

    fn compare_dictionary<K: ArrowPrimitiveType + ArrowDictionaryKeyType>(
        left: &ArrayRef,
        right: &ArrayRef,
        index: usize,
    ) {
        let l = left.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
        let r = right.as_any().downcast_ref::<DictionaryArray<K>>().unwrap();
        assert_eq!(l.key(index), r.key(index));
        compare_arrays(l.values(), r.values());
    }

    fn compare_array_values(left: &ArrayRef, right: &ArrayRef, index: usize) {
        match left.data_type() {
            DataType::Boolean => {
                let l = left.as_boolean();
                let r = right.as_boolean();
                assert_eq!(l.value(index), r.value(index));
            }
            DataType::Int32 => {
                let l = left.as_primitive::<Int32Type>();
                let r = right.as_primitive::<Int32Type>();
                assert_eq!(l.value(index), r.value(index));
            }
            DataType::UInt64 => {
                let l = left.as_primitive::<UInt64Type>();
                let r = right.as_primitive::<UInt64Type>();
                assert_eq!(l.value(index), r.value(index));
            }
            DataType::Float64 => {
                let l = left.as_primitive::<Float64Type>();
                let r = right.as_primitive::<Float64Type>();
                assert!((l.value(index) - r.value(index)).abs() < f64::EPSILON);
            }
            DataType::Utf8 => {
                let l = left.as_string::<i32>();
                let r = right.as_string::<i32>();
                assert_eq!(l.value(index), r.value(index));
            }
            DataType::LargeUtf8 => {
                let l = left.as_string::<i64>();
                let r = right.as_string::<i64>();
                assert_eq!(l.value(index), r.value(index));
            }
            DataType::List(_) => {
                let l = left.as_list::<i32>();
                let r = right.as_list::<i32>();
                compare_arrays(&l.value(index), &r.value(index));
            }
            DataType::LargeList(_) => {
                let l = left.as_list::<i64>();
                let r = right.as_list::<i64>();
                compare_arrays(&l.value(index), &r.value(index));
            }
            DataType::Struct(_) => {
                let l = left.as_struct();
                let r = right.as_struct();
                for j in 0..l.num_columns() {
                    compare_arrays(l.column(j), r.column(j));
                }
            }
            DataType::Dictionary(key_type, _) => match key_type.as_ref() {
                DataType::Int8 => compare_dictionary::<Int8Type>(left, right, index),
                DataType::Int16 => compare_dictionary::<Int16Type>(left, right, index),
                DataType::Int32 => compare_dictionary::<Int32Type>(left, right, index),
                DataType::Int64 => compare_dictionary::<Int64Type>(left, right, index),
                DataType::UInt8 => compare_dictionary::<UInt8Type>(left, right, index),
                DataType::UInt16 => compare_dictionary::<UInt16Type>(left, right, index),
                DataType::UInt32 => compare_dictionary::<UInt32Type>(left, right, index),
                DataType::UInt64 => compare_dictionary::<UInt64Type>(left, right, index),
                _ => panic!("Unsupported dictionary key type: {:?}", key_type),
            },
            // Add other data types as needed
            _ => panic!(
                "Unsupported data type for comparison: {:?}",
                left.data_type()
            ),
        }
    }

    #[test]
    fn test_boolean_array() {
        let array = BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]);
        test_roundtrip(array);
    }

    #[test]
    fn test_int32_array() {
        let array = Int32Array::from(vec![Some(1), None, Some(3), Some(-5)]);
        test_roundtrip(array);
    }

    #[test]
    fn test_float64_array() {
        let array = Float64Array::from(vec![Some(1.1), None, Some(3.3), Some(-5.5)]);
        test_roundtrip(array);
    }

    #[test]
    fn test_string_array() {
        let array = StringArray::from(vec![Some("hello"), None, Some("world"), Some("")]);
        test_roundtrip(array);
    }

    #[test]
    fn test_list_array() {
        let data = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];
        let array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
        test_roundtrip(array);
    }

    #[test]
    fn test_struct_array() {
        let boolean = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let int = Int32Array::from(vec![Some(1), Some(2), None]);
        let fields = Fields::from(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int32, true),
        ]);
        let array =
            StructArray::try_new(fields, vec![Arc::new(boolean), Arc::new(int)], None).unwrap();
        test_roundtrip(array);
    }

    #[test]
    fn test_dictionary_array() {
        let keys = Int32Array::from(vec![Some(0), Some(1), Some(2), None, Some(1)]);
        let values = StringArray::from(vec!["foo", "bar", "baz"]);
        let array = DictionaryArray::try_new(keys, Arc::new(values)).unwrap();
        test_roundtrip(array);
    }

    #[test]
    fn test_empty_array() {
        let array_data = ArrayData::new_empty(&DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            true,
        ))));
        let array = make_array(array_data);
        test_roundtrip(array);
    }

    #[test]
    fn test_all_nulls_array() {
        let array = Int32Array::from(vec![None, None, None]);
        test_roundtrip(array);
    }

    #[test]
    fn test_large_array() {
        let array = Int32Array::from_iter((0..10000).map(Some));
        test_roundtrip(array);
    }

    #[test]
    fn test_nested_array() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref([0, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        arrow::util::bit_util::set_bit(&mut null_bits, 0);
        arrow::util::bit_util::set_bit(&mut null_bits, 3);
        arrow::util::bit_util::set_bit(&mut null_bits, 4);
        arrow::util::bit_util::set_bit(&mut null_bits, 6);
        arrow::util::bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let outer = make_array(list_data);

        test_roundtrip(outer);
    }

    #[test]
    fn test_serialized_size() {
        let int_array = Int32Array::from_iter((0..1000).map(Some));
        let array = Arc::new(int_array) as ArrayRef;
        let serialized = serialize_array(&array).unwrap();
        assert!(serialized.len() < 5000, "Serialized size is too large");
    }

    #[test]
    fn test_error_handling() {
        let invalid_bytes = vec![0, 1, 2, 3];
        assert!(deserialize_array(&invalid_bytes).is_err());
    }

    #[test]
    fn test_avg_accumulator_serialization() {
        let mut accumulator = Box::new(AvgAccumulator::default());

        let float_array = Float64Array::from_value(56.0, 56);
        let array = Arc::new(float_array) as ArrayRef;

        accumulator.update_batch(&[array]).unwrap();
        let state = accumulator.state().unwrap();
        let arrays: Vec<ArrayRef> = state
            .into_iter()
            .map(|sv| sv.to_array().unwrap())
            .collect::<Vec<ArrayRef>>();

        let array_container = ArrayContainer { arrays };
        let serialized = serialize_array_container(&array_container).unwrap();

        let deserialized = deserialize_array_container(&serialized).unwrap();
        accumulator = Box::new(AvgAccumulator::default());
        let _ = accumulator.merge_batch(&deserialized.arrays);

        let result = accumulator.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Float64(Some(56.0)));
    }
}
