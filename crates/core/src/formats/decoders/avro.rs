use std::{io::Cursor, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use datafusion::datasource::avro_to_arrow::ReaderBuilder;
use denormalized_common::DenormalizedError;

use super::Decoder;

#[derive(Clone)]
pub struct AvroDecoder {
    schema: Arc<Schema>,
    cache: Vec<Vec<u8>>,
    size: usize,
}

impl Decoder for AvroDecoder {
    fn push_to_buffer(&mut self, bytes: Vec<u8>) {
        self.cache.push(bytes);
        self.size += 1;
    }

    fn to_record_batch(&mut self) -> Result<arrow_array::RecordBatch, DenormalizedError> {
        if self.size == 0 {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }
        let all_bytes: Vec<u8> = self.cache.iter().flatten().cloned().collect();
        // Create a cursor from the concatenated bytes
        let cursor = Cursor::new(all_bytes);

        // Build the reader
        let mut reader = ReaderBuilder::new()
            .with_batch_size(self.size)
            .with_schema(self.schema.clone())
            .build(cursor)?;

        // Read the batch
        match reader.next() {
            Some(Ok(batch)) => Ok(batch),
            Some(Err(e)) => Err(DenormalizedError::Arrow(e)),
            None => Ok(RecordBatch::new_empty(self.schema.clone())),
        }
    }
}

impl AvroDecoder {
    pub fn new(schema: Arc<Schema>) -> Self {
        AvroDecoder {
            schema,
            cache: Vec::new(),
            size: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{types::Record, Schema as AvroSchema, Writer};
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field};

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn create_avro_data(records: Vec<(i32, &str)>) -> Vec<u8> {
        let avro_schema = AvroSchema::parse_str(
            r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"}
                ]
            }
        "#,
        )
        .unwrap();

        let mut writer = Writer::new(&avro_schema, Vec::new());

        for (id, name) in records {
            let mut record: Record<'_> = Record::new(writer.schema()).unwrap();
            record.put("id", id);
            record.put("name", name);
            writer.append(record).unwrap();
        }

        writer.into_inner().unwrap()
    }

    #[test]
    fn test_push_to_buffer() {
        let schema = create_test_schema();
        let mut decoder = AvroDecoder::new(schema);

        decoder.push_to_buffer(vec![1, 2, 3]);
        decoder.push_to_buffer(vec![4, 5, 6]);

        assert_eq!(decoder.size, 2);
        assert_eq!(decoder.cache, vec![vec![1, 2, 3], vec![4, 5, 6]]);
    }

    #[test]
    fn test_empty_record_batch() {
        let schema = create_test_schema();
        let mut decoder = AvroDecoder::new(schema.clone());

        let result = decoder.to_record_batch().unwrap();

        assert_eq!(result.schema(), schema);
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_record_batch_with_data() {
        let schema = create_test_schema();
        let mut decoder = AvroDecoder::new(schema.clone());

        let avro_data = create_avro_data(vec![(1, "Alice")]);
        decoder.push_to_buffer(avro_data);

        let result = decoder.to_record_batch().unwrap();

        assert_eq!(result.schema(), schema);
        assert_eq!(result.num_rows(), 1);

        let id_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name_array = result
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(id_array.value(0), 1);
        assert_eq!(name_array.value(0), "Alice");
    }

    #[test]
    fn test_invalid_avro_data() {
        let schema = create_test_schema();
        let mut decoder = AvroDecoder::new(schema);

        decoder.push_to_buffer(vec![1, 2, 3]);

        let result = decoder.to_record_batch();

        assert!(matches!(result, Err(DenormalizedError::DataFusion(_))));
    }
}
