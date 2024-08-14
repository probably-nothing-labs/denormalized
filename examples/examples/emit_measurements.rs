use datafusion::error::Result;
use rand::seq::SliceRandom;
use rdkafka::producer::FutureProducer;
use std::time::{SystemTime, UNIX_EPOCH};

use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;

use df_streams_examples::Measurment;

/// This script emits test data to a kafka cluster
///
/// To run start kafka in docker run `docker run -p 9092:9092 --name kafka apache/kafka`
/// Sample sensor data will then be emitted to two topics: `temperature` and `humidity`
/// This data is read processed by other exmpales
#[tokio::main]
async fn main() -> Result<()> {
    let mut rng = rand::thread_rng();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", String::from("localhost:9092"))
        .set("message.timeout.ms", "60000")
        .create()
        .expect("Producer creation error");

    let sensors = ["sensor_0", "sensor_1", "sensor_2", "sensor_3", "sensor_4"];

    loop {
        let sensor_name = sensors.choose(&mut rng).unwrap().to_string();

        // Alternate between sending random temperature and humidity readings
        let (topic, msg) = if rand::random::<f64>() < 0.4 {
            (
                "temperature".to_string(),
                serde_json::to_vec(&Measurment {
                    occurred_at_ms: get_timestamp_ms(),
                    sensor_name,
                    reading: rand::random::<f64>() * 115.0,
                })
                .unwrap(),
            )
        } else {
            (
                "humidity".to_string(),
                serde_json::to_vec(&Measurment {
                    occurred_at_ms: get_timestamp_ms(),
                    sensor_name,
                    reading: rand::random::<f64>(),
                })
                .unwrap(),
            )
        };

        producer
            .send(
                FutureRecord::<(), Vec<u8>>::to(topic.as_str()).payload(&msg),
                Timeout::Never,
            )
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_micros(1)).await;
    }
}

fn get_timestamp_ms() -> u64 {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    since_the_epoch.as_millis() as u64
}
