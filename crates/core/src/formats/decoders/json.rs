use std::sync::Arc;

use arrow_schema::Schema;
use serde_json::Value;

use crate::utils::arrow_helpers::json_records_to_arrow_record_batch;

use super::Decoder;

#[derive(Clone)]
pub struct JsonDecoder {
    schema: Arc<Schema>,
    cache: Vec<Vec<u8>>,
}

impl JsonDecoder {
    pub fn new(schema: Arc<Schema>) -> Self {
        JsonDecoder {
            schema: schema.clone(),
            cache: Vec::new(),
        }
    }
}

impl Decoder for JsonDecoder {
    fn to_record_batch(
        &mut self,
    ) -> Result<arrow_array::RecordBatch, denormalized_common::DenormalizedError> {
        let mut combined_json = Vec::new();
        combined_json.push(b'[');
        for (index, row) in self.cache.iter().enumerate() {
            if index > 0 {
                combined_json.push(b',');
            }
            combined_json.extend_from_slice(row.as_slice());
        }
        combined_json.push(b']');
        self.cache.clear();
        let result: Vec<Value> = serde_json::from_slice(&combined_json)?;
        Ok(json_records_to_arrow_record_batch(
            result,
            self.schema.clone(),
        ))
    }

    fn push_to_buffer(&mut self, bytes: Vec<u8>) {
        self.cache.push(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_test_json(id: i64, name: &str) -> Vec<u8> {
        format!(r#"{{"id": {}, "name": "{}"}}"#, id, name).into_bytes()
    }

    #[test]
    fn test_json_decoder_new() {
        let schema = create_test_schema();
        let decoder = JsonDecoder::new(schema.clone());
        assert_eq!(decoder.schema, schema);
        assert!(decoder.cache.is_empty());
    }

    #[test]
    fn test_push_to_buffer() {
        let schema = create_test_schema();
        let mut decoder = JsonDecoder::new(schema);
        let json1 = create_test_json(1, "Alice");
        let json2 = create_test_json(2, "Bob");

        decoder.push_to_buffer(json1.clone());
        decoder.push_to_buffer(json2.clone());

        assert_eq!(decoder.cache, vec![json1, json2]);
    }

    #[test]
    fn test_to_record_batch() -> Result<(), denormalized_common::DenormalizedError> {
        let schema = create_test_schema();
        let mut decoder = JsonDecoder::new(schema.clone());

        decoder.push_to_buffer(create_test_json(1, "Alice"));
        decoder.push_to_buffer(create_test_json(2, "Bob"));
        decoder.push_to_buffer(create_test_json(3, "Charlie"));

        let record_batch = decoder.to_record_batch()?;

        assert_eq!(record_batch.num_columns(), 2);
        assert_eq!(record_batch.num_rows(), 3);

        let id_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let name_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);

        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");

        Ok(())
    }

    #[test]
    fn test_to_record_batch_empty() -> Result<(), denormalized_common::DenormalizedError> {
        let schema = create_test_schema();
        let mut decoder = JsonDecoder::new(schema.clone());

        let record_batch = decoder.to_record_batch()?;

        assert_eq!(record_batch.num_columns(), 2);
        assert_eq!(record_batch.num_rows(), 0);

        Ok(())
    }

    #[test]
    fn test_to_record_batch_invalid_json() {
        let schema = create_test_schema();
        let mut decoder = JsonDecoder::new(schema.clone());

        decoder.push_to_buffer(b"{invalid_json}".to_vec());

        let result = decoder.to_record_batch();
        assert!(result.is_err());
    }
}
