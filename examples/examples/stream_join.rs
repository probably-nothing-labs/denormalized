use std::time::Duration;

use datafusion::common::JoinType;
use datafusion::error::Result;
use datafusion::functions_aggregate::average::avg;
use datafusion::logical_expr::col;

use denormalized::context::Context;
use denormalized::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use denormalized::physical_plan::utils::time::TimestampUnit;

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
            ("auto.offset.reset".to_string(), "earliest".to_string()),
            ("group.id".to_string(), "sample_pipeline".to_string()),
        ]))
        .await?;

    let humidity_ds = ctx
        .from_topic(
            source_topic_builder
                .clone()
                .with_topic(String::from("humidity"))
                .build_reader(ConnectionOpts::from([
                    ("auto.offset.reset".to_string(), "earliest".to_string()),
                    ("group.id".to_string(), "sample_pipeline".to_string()),
                ]))
                .await?,
        )
        .await?;

    let joined_ds = ctx
        .from_topic(temperature_topic)
        .await?
        .join(
            humidity_ds,
            JoinType::Inner,
            &["sensor_name"],
            &["sensor_name"],
            None,
        )?
        .window(
            vec![],
            vec![
                avg(col("temperature.reading")).alias("avg_temperature"),
                avg(col("humidity.reading")).alias("avg_humidity"),
            ],
            Duration::from_millis(1_000),
            None,
        )?;

    joined_ds.clone().print_stream().await?;

    Ok(())
}
