use std::time::Duration;

use datafusion::error::Result;
use datafusion::functions_aggregate::average::avg;
use datafusion::functions_aggregate::count::count;
use datafusion::functions_aggregate::expr_fn::{max, min};
use datafusion::logical_expr::{col, lit};

use denormalized::context::Context;
use denormalized::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use denormalized::physical_plan::utils::time::TimestampUnit;

use denormalized_examples::get_sample_json;

/// Demonstrates a simple stream aggregate job on data generated via the `emit_measurements.rs`
/// example script.
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Warn)
        .init();

    let sample_event = get_sample_json();

    let bootstrap_servers = String::from("localhost:9092");

    let ctx = Context::new()?;
    let mut topic_builder = KafkaTopicBuilder::new(bootstrap_servers.clone());

    let source_topic = topic_builder
        .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
        .with_encoding("json")?
        .with_topic(String::from("temperature"))
        .infer_schema_from_json(sample_event.as_str())?
        .build_reader(ConnectionOpts::from([
            ("auto.offset.reset".to_string(), "latest".to_string()),
            ("group.id".to_string(), "sample_pipeline".to_string()),
        ]))
        .await?;

    let ds = ctx
        .from_topic(source_topic)
        .await?
        .window(
            vec![],
            vec![
                count(col("reading")).alias("count"),
                min(col("reading")).alias("min"),
                max(col("reading")).alias("max"),
                avg(col("reading")).alias("average"),
            ],
            Duration::from_millis(1_000),
            None,
        )?
        .filter(col("max").gt(lit(113)))?;

    ds.clone().print_stream().await?;

    Ok(())
}
