#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::Result;
use datafusion::{
    config::ConfigOptions, dataframe::DataFrame, datasource::provider_as_source,
    execution::context::SessionContext, physical_plan::time::TimestampUnit,
};
use datafusion_common::franz_arrow::infer_arrow_schema_from_json_value;
use datafusion_expr::{col, max, min, LogicalPlanBuilder};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions_aggregate::count::count;

use df_streams_core::datasource::kafka::{
    ConnectionOpts, KafkaTopic, KafkaTopicConfig, KafkaTopicConfigBuilder,
};
use df_streams_core::sinkable::Sinkable;

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
    let read_config =
        KafkaTopicConfigBuilder::new(bootstrap_servers.clone(), String::from("driver-imu-data"))
            .infer_schema_from_json(sample_event)?
            .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
            .with_encoding("json")?
            .with_consumer_opts(ConnectionOpts::from([(
                "auto.offset.reset".to_string(),
                "earliest".to_string(),
            )]))
            .build()
            .await?;

    // Create a new streaming table
    let source_topic = KafkaTopic(Arc::new(read_config));

    let mut datafusion_config = ConfigOptions::default();
    let _ = datafusion_config.set("datafusion.execution.batch_size", "32")?;

    // Create the context object with a source from kafka
    let ctx = SessionContext::new_with_config(datafusion_config.into());

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

    let processed_schema = Arc::new(datafusion::common::arrow::datatypes::Schema::from(
        df.schema(),
    ));

    // println!("{}", processed_schema);

    let write_config =
        KafkaTopicConfigBuilder::new(bootstrap_servers.clone(), String::from("out_topic"))
            .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
            .with_encoding("json")?
            .with_schema(processed_schema)
            .with_producer_opts(ConnectionOpts::from([]))
            .build()
            .await?;

    let sink_topic = KafkaTopic(Arc::new(write_config));
    ctx.register_table("out", Arc::new(sink_topic))?;

    df.write_table("out", DataFrameWriteOptions::default())
        .await?;

    // use df_streams_sinks::{
    //     FileSink, FranzSink, KafkaSink, KafkaSinkSettings, PrettyPrinter, StdoutSink,
    // };

    // use datafusion_franz::{RocksDBBackend, StreamMonitor, StreamMonitorConfig};

    // let fname = "/tmp/out.jsonl";
    // println!("Writing results to file {}", fname);
    // let writer = FileSink::new(fname)?;
    // let file_writer = Box::new(writer) as Box<dyn FranzSink>;
    // let _ = windowed_df.sink(file_writer).await;

    // let writer = StdoutSink::new()?;
    // let sink = Box::new(writer) as Box<dyn FranzSink>;
    // let _ = windowed_df.sink(sink).await;

    // Write pretty output to the terminal
    // let writer = PrettyPrinter::new()?;
    // let sink = Box::new(writer) as Box<dyn FranzSink>;
    // let _ = df.sink(sink).await;

    //// Write Messages to Kafka topic
    // let config = KafkaSinkSettings {
    //     topic: "out_topic".to_string(),
    //     bootstrap_servers: bootstrap_servers.clone(),
    // };
    // let writer = KafkaSink::new(&config)?;
    // let sink = Box::new(writer) as Box<dyn FranzSink>;
    // let _ = windowed_df.sink(sink).await;

    Ok(())
}
