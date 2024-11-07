use std::time::Duration;

use datafusion::common::JoinType;
use datafusion::functions_aggregate::average::avg;
use datafusion::logical_expr::col;

use denormalized::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use denormalized::prelude::*;

use denormalized_examples::get_sample_json;

/// Demonstrates a simple stream join on data generated via the `emit_measurements.rs`
/// example script.
#[tokio::main]
async fn main() -> Result<()> {
    let sample_event = get_sample_json();

    let bootstrap_servers = String::from("localhost:9092");

    let ctx = Context::new()?;
    let mut topic_builder = KafkaTopicBuilder::new(bootstrap_servers.clone());

    let source_topic_builder = topic_builder
        .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
        .with_encoding("json")?
        .infer_schema_from_json(sample_event.as_str())?;

    let temperature_topic = source_topic_builder
        .clone()
        .with_topic(String::from("temperature"))
        .build_reader(ConnectionOpts::from([
            ("auto.offset.reset".to_string(), "latest".to_string()),
            ("group.id".to_string(), "sample_pipeline".to_string()),
        ]))
        .await?;

    let humidity_ds = ctx
        .from_topic(
            source_topic_builder
                .clone()
                .with_topic(String::from("humidity"))
                .build_reader(ConnectionOpts::from([
                    ("auto.offset.reset".to_string(), "latest".to_string()),
                    ("group.id".to_string(), "sample_pipeline".to_string()),
                ]))
                .await?,
        )
        .await?
        .with_column("humidity_sensor", col("sensor_name"))?
        .drop_columns(&["sensor_name"])?
        .window(
            vec![col("humidity_sensor")],
            vec![avg(col("reading")).alias("avg_humidity")],
            Duration::from_millis(1_000),
            None,
        )?
        .with_column("humidity_window_start_time", col("window_start_time"))?
        .with_column("humidity_window_end_time", col("window_end_time"))?
        .drop_columns(&["window_start_time", "window_end_time"])?;

    let joined_ds = ctx
        .from_topic(temperature_topic)
        .await?
        .window(
            vec![col("sensor_name")],
            vec![avg(col("reading")).alias("avg_temperature")],
            Duration::from_millis(1_000),
            None,
        )?
        .join(
            humidity_ds,
            JoinType::Inner,
            &["sensor_name", "window_start_time", "window_end_time"],
            &[
                "humidity_sensor",
                "humidity_window_start_time",
                "humidity_window_end_time",
            ],
            None,
        )?;

    joined_ds.clone().print_stream().await?;

    Ok(())
}
