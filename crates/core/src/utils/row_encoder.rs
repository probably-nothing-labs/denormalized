use arrow::json::writer::{JsonFormat, Writer};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;

pub trait RowEncoder {
    fn encode(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>>;
}

#[derive(Debug, Default)]
// Formats json without any characting separating items.
pub struct NoDelimiter {}
impl JsonFormat for NoDelimiter {}
// writes rows as json without any character separating them
type JsonWriter<W> = Writer<W, NoDelimiter>;

pub struct JsonRowEncoder {}

impl JsonRowEncoder {
    pub fn batch_to_json(&self, batch: &RecordBatch) -> Result<Vec<u8>> {
        let buf = Vec::new();
        let mut writer = JsonWriter::new(buf);
        writer.write(batch)?;
        writer.finish()?;
        let buf = writer.into_inner();

        Ok(buf)
    }
}

impl RowEncoder for JsonRowEncoder {
    fn encode(&self, batch: &RecordBatch) -> Result<Vec<Vec<u8>>> {
        if batch.num_rows() == 0 {
            return Ok(vec![]);
        }

        let mut buffer = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let row = batch.slice(i, 1);
            buffer.push(self.batch_to_json(&row)?);
        }

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::{JsonRowEncoder, RowEncoder};

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn serialize_record_batch_to_json() {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Utf8, false),
            Field::new("col2", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(Int32Array::from(vec![1, 10, 20, 100])),
            ],
        )
        .unwrap();

        let encoder = JsonRowEncoder {};
        let buf = encoder.encode(&batch).unwrap();

        let res: Vec<&[u8]> = [
            "{\"col1\":\"a\",\"col2\":1}",
            "{\"col1\":\"b\",\"col2\":10}",
            "{\"col1\":\"c\",\"col2\":20}",
            "{\"col1\":\"d\",\"col2\":100}",
        ]
        .iter()
        .map(|v| v.as_bytes())
        .collect::<_>();

        assert_eq!(buf, res);
    }
}
