#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use datafusion::error::Result;
use df_streams_core::datasource::kafka::{ConnectionOpts, KafkaReadConfig, KafkaTopicBuilder};
use std::{sync::Arc, time::Duration};

use rdkafka::admin::AdminClient;
use rdkafka::admin::AdminOptions;
use rdkafka::admin::ConfigResource;
use rdkafka::admin::ConfigResourceResult;
use rdkafka::admin::ResourceSpecifier;
use rdkafka::config::ClientConfig;
use rdkafka::config::FromClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::metadata::MetadataTopic;
use tracing::field::debug;

#[tokio::main]
async fn main() -> Result<()> {
    let bootstrap_servers = String::from("localhost:19092,localhost:29092,localhost:39092");
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", bootstrap_servers.to_string());

    let admin = AdminClient::from_config(&client_config).unwrap();

    // let res: Vec<ConfigResource> = admin
    //     .describe_configs(
    //         &vec![ResourceSpecifier::Topic("out_topic")],
    //         &AdminOptions::default(),
    //     )
    //     .await
    //     .unwrap()
    //     .into_iter()
    //     .map(|v| v.unwrap())
    //     .collect();
    //
    // for (k, v) in res[0].entry_map().into_iter() {
    //     println!("{}: {:?}", k, v.value);
    // }

    let mut client_config = ClientConfig::new();

    client_config.set("bootstrap.servers", bootstrap_servers.to_string());

    let consumer: StreamConsumer = client_config.create().expect("Consumer creation failed");

    let data = consumer
        .fetch_metadata(Some("out_topic"), Duration::from_millis(5_000))
        .unwrap();
    let topic_metadata = data.topics();
    let md = &topic_metadata[0];
    let partitions = md.partitions();

    println!("{:?}", partitions.len());

    Ok(())
}
