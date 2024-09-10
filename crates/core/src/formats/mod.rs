use std::str::FromStr;

use denormalized_common::DenormalizedError;

#[derive(Debug, Clone, Copy)]
pub enum StreamEncoding {
    Avro,
    Json,
}

impl FromStr for StreamEncoding {
    type Err = DenormalizedError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "avro" => Ok(Self::Avro),
            "json" => Ok(Self::Json),
            _ => Err(Self::Err::KafkaConfig(format!(
                "Unrecognised StreamEncoding {}",
                s
            ))),
        }
    }
}

pub mod decoders;
