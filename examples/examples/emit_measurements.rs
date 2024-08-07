use datafusion::error::Result;
use rdkafka::producer::FutureProducer;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;

#[derive(Serialize, Deserialize)]
pub struct Measurment {
    occurred_at_ms: u64,
    temperature: f64,
}

/// docker run -p 9092:9092 --name kafka apache/kafka
#[tokio::main]
async fn main() -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", String::from("localhost:9092"))
        .set("message.timeout.ms", "60000")
        .create()
        .expect("Producer creation error");

    let topic = "temperature".to_string();

    loop {
        let msg = serde_json::to_vec(&Measurment {
            occurred_at_ms: get_timestamp_ms(),
            temperature: rand::random::<f64>() * 115.0,
        })
        .unwrap();

        producer
            .send(
                FutureRecord::<(), Vec<u8>>::to(topic.as_str()).payload(&msg),
                Timeout::Never,
            )
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    }
}

fn get_timestamp_ms() -> u64 {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    since_the_epoch.as_millis() as u64
}
