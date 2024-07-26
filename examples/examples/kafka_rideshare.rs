#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::{config::ConfigOptions, dataframe::DataFrame};

use datafusion::execution::{
    config::SessionConfig, context::SessionContext, runtime_env::RuntimeEnv,
    session_state::SessionState,
};

use datafusion_expr::{col, max, min, LogicalPlanBuilder};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions_aggregate::count::count;

use df_streams_core::dataframe::StreamingDataframe;
use df_streams_core::datasource::kafka::{
    ConnectionOpts, KafkaTopicBuilder, TopicReader, TopicWriter,
};
use df_streams_core::physical_optimizer::CoaslesceBeforeStreamingAggregate;
use df_streams_core::utils::arrow_helpers::json_records_to_arrow_record_batch;
use df_streams_core::physical_plan::utils::time::TimestampUnit;

use std::{sync::Arc, time::Duration};
use tracing_subscriber::{fmt::format::FmtSpan, FmtSubscriber};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_log::LogTracer::init().expect("Failed to set up log tracer");

    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        .with_span_events(FmtSpan::CLOSE | FmtSpan::ENTER)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let config = SessionConfig::new().set(
        "datafusion.execution.batch_size",
        datafusion_common::ScalarValue::UInt64(Some(32)),
    );
    let runtime = Arc::new(RuntimeEnv::default());
    let mut state = SessionState::new_with_config_rt(config, runtime);

    // state.add_optimizer_rule();
    // state = state.with_query_planner(Arc::new(.. todo))

    state = state.add_physical_optimizer_rule(Arc::new(CoaslesceBeforeStreamingAggregate::new()));


    let ctx = SessionContext::new_with_state(state);

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

    let bootstrap_servers = String::from("localhost:19092,localhost:29092,localhost:39092");

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

    ctx.register_table("kafka_imu_data", Arc::new(source_topic))?;

    let df = ctx
        .clone()
        .table("kafka_imu_data")
        .await?
        .streaming_window(
            vec![],
            vec![
                max(col("imu_measurement").field("gps").field("speed")),
                min(col("imu_measurement").field("gps").field("altitude")),
                count(col("imu_measurement")).alias("count"),
            ],
            Duration::from_millis(5_000),       // 5 second window
            Some(Duration::from_millis(1_000)), // 1 second slide
        )?;

    df.clone().print_stream().await?;

    let processed_schema = Arc::new(datafusion::common::arrow::datatypes::Schema::from(
        df.schema(),
    ));

    println!("{}", processed_schema);

    let sink_topic = topic_builder
        .with_topic(String::from("out_topic"))
        .with_schema(processed_schema)
        .build_writer(ConnectionOpts::new())
        .await?;

    ctx.register_table("out", Arc::new(sink_topic))?;

    df.write_table("out", DataFrameWriteOptions::default())
        .await?;

    Ok(())
}
