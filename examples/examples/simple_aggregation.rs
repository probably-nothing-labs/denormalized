use std::time::Duration;

use datafusion::functions_aggregate::average::avg;
use datafusion::functions_aggregate::count::count;
use datafusion::functions_aggregate::expr_fn::{max, min};
use datafusion::logical_expr::{col, lit};

use denormalized::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use denormalized::prelude::*;

use denormalized::state_backend::slatedb::initialize_global_slatedb;
use denormalized_examples::get_sample_json;
use log::debug;
use tracing::instrument::WithSubscriber;
use tracing::Level;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, EnvFilter, FmtSubscriber};

/// Demonstrates a simple stream aggregate job on data generated via the `emit_measurements.rs`
/// example script.
#[tokio::main]
async fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    // let filter = "tokio=trace,runtime=trace"
    //     .parse::<filter::Targets>()
    //     .unwrap();

    // //tracing::subscriber::set_global_default(subscriber);
    // tracing_subscriber::registry().with(filter).init();

    let sample_event = get_sample_json();

    let bootstrap_servers = String::from("localhost:19092,localhost:29092,localhost:39092");

    let ctx = Context::new()?;
    let mut topic_builder =
        KafkaTopicBuilder::new("localhost:19092,localhost:29092,localhost:39092".to_string());

    // Connect to source topic
    let source_topic = topic_builder
        .with_topic(String::from("temperature"))
        .infer_schema_from_json(get_sample_json().as_str())?
        .with_encoding("json")?
        .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
        .build_reader(ConnectionOpts::from([
            ("auto.offset.reset".to_string(), "latest".to_string()),
            ("group.id".to_string(), "sample_pipeline".to_string()),
        ]))
        .await?;

    let _ = initialize_global_slatedb("/tmp/agg1234").await;
    ctx.from_topic(source_topic)
        .await?
        .window(
            vec![col("sensor_name")],
            vec![
                count(col("reading")).alias("count"),
                min(col("reading")).alias("min"),
                max(col("reading")).alias("max"),
                //avg(col("reading")).alias("average"),
            ],
            Duration::from_millis(1_000), // aggregate every 1 second
            None,                         // None means tumbling window
        )?
        .filter(col("max").gt(lit(113)))?
        .print_stream() // Print out the results
        .await?;

    Ok(())
}
