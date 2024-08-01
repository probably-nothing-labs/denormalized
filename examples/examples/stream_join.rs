#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use datafusion::error::Result;
use datafusion_expr::{col, max, min};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions_aggregate::count::count;

use df_streams_core::context::Context;
use df_streams_core::datasource::kafka::{ConnectionOpts, KafkaTopicBuilder};
use df_streams_core::physical_plan::utils::time::TimestampUnit;

use std::time::Duration;
use tracing_subscriber::{fmt::format::FmtSpan, FmtSubscriber};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // tracing_log::LogTracer::init().expect("Failed to set up log tracer");
    //
    // let subscriber = FmtSubscriber::builder()
    //     .with_max_level(tracing::Level::INFO)
    //     .with_span_events(FmtSpan::CLOSE | FmtSpan::ENTER)
    //     .finish();
    // tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
    //
    // let bootstrap_servers = String::from("localhost:19092,localhost:29092,localhost:39092");
    //
    // // Configure Kafka source for IMU data
    // let imu_stream = create_kafka_source(
    //     &r#"{
    //         "driver_id": "690c119e-63c9-479b-b822-872ee7d89165",
    //         "occurred_at_ms": 1715201766763,
    //         "imu_measurement": {
    //             "timestamp": "2024-05-08T20:56:06.763260Z",
    //             "accelerometer": {
    //                 "x": 1.4187794,
    //                 "y": -0.13967037,
    //                 "z": 0.5483732
    //             },
    //             "gyroscope": {
    //                 "x": 0.005840948,
    //                 "y": 0.0035944171,
    //                 "z": 0.0041645765
    //             },
    //             "gps": {
    //                 "latitude": 72.3492587464122,
    //                 "longitude": 144.85596244550095,
    //                 "altitude": 2.9088259,
    //                 "speed": 57.96137
    //             }
    //         },
    //         "meta": {
    //             "nonsense": "MMMMMMMMMM"
    //         }
    //     }"#
    //     .to_string(),
    //     bootstrap_servers.clone(),
    //     "driver-imu-data".to_string(),
    //     "kafka_rideshare".to_string(),
    // );
    //
    // // Configure Kafka source for Trip data
    // let trip_stream = create_kafka_source(
    //     &r#"{
    //         "event_name": "TRIP_START",
    //         "trip_id": "b005922a-4ba5-4678-b0e6-bcb5ca2abe3e",
    //         "driver_id": "788fb395-96d0-4bc8-8ed9-bcf4e11e7543",
    //         "occurred_at_ms": 1718752555452,
    //         "meta": {
    //             "nonsense": "MMMMMMMMMM"
    //         }
    //     }"#
    //     .to_string(),
    //     bootstrap_servers.clone(),
    //     "trips".to_string(),
    //     "kafka_rideshare".to_string(),
    // );
    //
    // let mut config = ConfigOptions::default();
    // let _ = config.set("datafusion.execution.batch_size", "32");
    //
    // // Create the context object with a source from kafka
    // let ctx = SessionContext::new_with_config(config.into());
    //
    // let imu_stream_plan = LogicalPlanBuilder::scan_with_filters(
    //     "imu_data",
    //     provider_as_source(Arc::new(imu_stream)),
    //     None,
    //     vec![],
    // )
    // .unwrap()
    // .build()
    // .unwrap();
    //
    // let logical_plan = LogicalPlanBuilder::scan_with_filters(
    //     "trips",
    //     provider_as_source(Arc::new(trip_stream)),
    //     None,
    //     vec![],
    // )
    // .unwrap()
    // .join_on(
    //     imu_stream_plan,
    //     JoinType::Left,
    //     vec![col("trips.driver_id").eq(col("imu_data.driver_id"))],
    // )
    // .unwrap()
    // .build()
    // .unwrap();
    //
    // let df = DataFrame::new(ctx.state(), logical_plan);
    // let windowed_df = df
    //     .clone()
    //     .select(vec![
    //         col("trips.trip_id"),
    //         col("trips.driver_id"),
    //         col("trips.event_name"),
    //         col("imu_measurement").field("gps").field("speed"),
    //     ])
    //     .unwrap();
    //
    // let writer = PrettyPrinter::new().unwrap();
    // let sink = Box::new(writer) as Box<dyn FranzSink>;
    // //let _ = windowed_df.sink(sink).await;
}
