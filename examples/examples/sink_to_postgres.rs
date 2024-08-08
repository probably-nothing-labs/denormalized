use std::time::Duration;

use datafusion::error::Result;
use datafusion::functions_aggregate::average::avg;
use datafusion::logical_expr::{col, max, min};

use df_streams_core::context::Context;
use df_streams_core::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use df_streams_core::physical_plan::utils::time::TimestampUnit;

#[tokio::main]
async fn main() -> Result<()> {
    let sample_event = r#"{"occurred_at_ms": 1715201766763, "temperature": 87.2}"#;

    let bootstrap_servers = String::from("localhost:9092");

    let ctx = Context::new()?;

    let mut topic_builder = KafkaTopicBuilder::new(bootstrap_servers.clone());

    let source_topic = topic_builder
        .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
        .with_encoding("json")?
        .with_topic(String::from("temperature"))
        .infer_schema_from_json(sample_event)?
        .build_reader(ConnectionOpts::from([
            ("auto.offset.reset".to_string(), "earliest".to_string()),
            ("group.id".to_string(), "sample_pipeline".to_string()),
        ]))
        .await?;

    let ds = ctx.from_topic(source_topic).await?.streaming_window(
        vec![],
        vec![
            min(col("temperature")).alias("min"),
            max(col("temperature")).alias("max"),
            avg(col("temperature")).alias("average"),
        ],
        Duration::from_millis(1_000), // 5 second window
        None,
    )?;

    // ds.clone().print_stream().await?;

    println!("{}", ds.df.schema());

    Ok(())
}
