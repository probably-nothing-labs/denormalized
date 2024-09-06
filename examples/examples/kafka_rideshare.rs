use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::functions_aggregate::count::count;
use datafusion::functions_aggregate::expr_fn::{max, min};
use datafusion::logical_expr::col;

use denormalized::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use denormalized::prelude::*;

use std::time::Duration;
use tracing_subscriber::{fmt::format::FmtSpan, FmtSubscriber};

/// An example job that processes sample rideshare data. See [docs/kafka_rideshare_example.md] for
/// more details
#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_log::LogTracer::init().expect("Failed to set up log tracer");

    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::CLOSE | FmtSpan::ENTER)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let sample_event = r#"
        {
            "driver_id": "690c119e-63c9-479b-b822-872ee7d89165",
            "occurred_at_ms": 1715201766763,
            "imu_measurement": {
                "timestamp": "2024-05-08T20:56:06.763260Z",
                "accelerometer": {
                    "x": 1.4187794,
                    "y": -0.13967037,
                    "z": 0.5483732
                },
                "gyroscope": {
                    "x": 0.005840948,
                    "y": 0.0035944171,
                    "z": 0.0041645765
                },
                "gps": {
                    "latitude": 72.3492587464122,
                    "longitude": 144.85596244550095,
                    "altitude": 2.9088259,
                    "speed": 57.96137
                }
            },
            "meta": {
                "nonsense": "MMMMMMMMMM"
            }
        }"#;

    let bootstrap_servers = String::from("localhost:9092");

    let ctx = Context::new()?;

    let mut topic_builder = KafkaTopicBuilder::new(bootstrap_servers.clone());
    topic_builder
        .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
        .with_encoding("json")?;

    let source_topic = topic_builder
        .with_topic(String::from("driver-imu-data"))
        .infer_schema_from_json(sample_event)?
        .build_reader(ConnectionOpts::from([
            ("auto.offset.reset".to_string(), "earliest".to_string()),
            ("group.id".to_string(), "test".to_string()),
        ]))
        .await?;

    let ds = ctx.from_topic(source_topic).await?.window(
        vec![col("driver_id")],
        vec![
            max(col("imu_measurement").field("gps").field("speed")),
            min(col("imu_measurement").field("gps").field("altitude")),
            count(col("imu_measurement")).alias("count"),
        ],
        Duration::from_millis(5_000),       // 5 second window
        Some(Duration::from_millis(1_000)), // 1 second slide
    )?;

    // ds.clone().print_stream().await?;
    ds.sink_kafka(bootstrap_servers.clone(), String::from("out_topic"))
        .await?;

    Ok(())
}
