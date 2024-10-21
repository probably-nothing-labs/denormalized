use rand::seq::SliceRandom;
use rdkafka::producer::FutureProducer;
use std::time::{SystemTime, UNIX_EPOCH};

use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureRecord;

use denormalized::prelude::*;
use denormalized_examples::Measurment;

/// This script emits test data to a kafka cluster
///
/// To run start kafka in docker run `docker run -p 9092:9092 --name kafka apache/kafka`
/// Sample sensor data will then be emitted to two topics: `temperature` and `humidity`
/// This data is read processed by other exmpales
#[tokio::main]
async fn main() -> Result<()> {
    let mut tasks = tokio::task::JoinSet::new();

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", String::from("localhost:9092"))
        .set("message.timeout.ms", "100")
        .create()
        .expect("Producer creation error");

    for _ in 0..2048 {
        let producer = producer.clone();

        tasks.spawn(async move {
            let sensors = [
                "sensor_0",
                "sensor_1",
                "sensor_2",
                "sensor_3",
                "sensor_4",
                "sensor_10",
                "sensor_11",
                "sensor_12",
                "sensor_13",
                "sensor_14",
            ];

            loop {
                let sensor_name = sensors.choose(&mut rand::thread_rng()).unwrap().to_string();

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

                let _ = producer
                    .send_result(FutureRecord::<(), Vec<u8>>::to(topic.as_str()).payload(&msg))
                    .unwrap()
                    .await
                    .unwrap();

                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        });
    }

    while let Some(res) = tasks.join_next().await {
        let _ = res;
    }

    Ok(())
}

fn get_timestamp_ms() -> u64 {
    let now = SystemTime::now();
    let since_the_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

    since_the_epoch.as_millis() as u64
}
