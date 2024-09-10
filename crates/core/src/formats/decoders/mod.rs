use arrow_array::RecordBatch;
use denormalized_common::DenormalizedError;

pub trait Decoder {
    fn push_to_buffer(&mut self, bytes: Vec<u8>);

    fn to_record_batch(&mut self) -> Result<RecordBatch, DenormalizedError>;
}

pub mod json;
