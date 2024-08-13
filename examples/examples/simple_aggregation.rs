use std::time::Duration;

use datafusion::error::Result;
use datafusion::functions_aggregate::average::avg;
use datafusion::functions_aggregate::count::count;
use datafusion::logical_expr::lit;
use datafusion::logical_expr::{col, max, min};

use df_streams_core::context::Context;
use df_streams_core::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use df_streams_core::physical_plan::utils::time::TimestampUnit;

use df_streams_examples::get_sample_json;

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
        // .filter(col("reading").gt(lit(70)))?
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
        .filter(col("max").lt(lit(113)))?;

    ds.clone().print_stream().await?;

    Ok(())
}
