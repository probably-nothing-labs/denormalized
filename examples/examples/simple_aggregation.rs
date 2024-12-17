use std::time::Duration;

use datafusion::functions_aggregate::count::count;
use datafusion::functions_aggregate::expr_fn::{avg, max, min};
use datafusion::logical_expr::col;

use denormalized::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use denormalized::prelude::*;

use denormalized_examples::get_sample_json;

/// Demonstrates a simple stream aggregate job on data generated via the `emit_measurements.rs`
/// example script.
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let bootstrap_servers = String::from("localhost:19092");

    let config = Context::default_config().set_bool("denormalized_config.checkpoint", false);

    let mut topic_builder = KafkaTopicBuilder::new(bootstrap_servers.clone());

    // Connect to source topic
    let source_topic = topic_builder
        .with_topic(String::from("temperature"))
        .infer_schema_from_json(get_sample_json().as_str())?
        .with_encoding("json")?
        //.with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
        .build_reader(ConnectionOpts::from([
            ("auto.offset.reset".to_string(), "latest".to_string()),
            ("group.id".to_string(), "sample_pipeline".to_string()),
        ]))
        .await?;

    let ds = Context::with_config(config)?
        // .with_slatedb_backend(String::from(
        //     "/tmp/checkpoints/simple-aggregation-example",
        // ))
        // .await
        .from_topic(source_topic)
        .await?
        .window(
            vec![col("sensor_name")],
            vec![
                count(col("reading")).alias("count"),
                min(col("reading")).alias("min"),
                max(col("reading")).alias("max"),
                avg(col("reading")).alias("average"),
            ],
            Duration::from_millis(5_000), // Window length
            None,                         // Slide duration. None defaults to a tumbling window.
        )?;
    ds.print_stream().await?;
    //ds.sink_kafka(bootstrap_servers, String::from("checkpointed-output"))
    //    .await?;
    Ok(())
}
