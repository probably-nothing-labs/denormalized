#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use datafusion::error::Result;
use df_streams_core::datasource::kafka::{
    ConnectionOpts, KafkaTopic, KafkaTopicConfig, KafkaTopicConfigBuilder,
};

use rdkafka::admin::AdminClient;
use rdkafka::admin::AdminOptions;
use rdkafka::admin::ResourceSpecifier;
use rdkafka::config::ClientConfig;
use rdkafka::config::FromClientConfig;
use rdkafka::admin::ConfigResourceResult;
use rdkafka::admin::ConfigResource;

#[tokio::main]
async fn main() -> Result<()> {
    let bootstrap_servers = String::from("localhost:19092,localhost:29092,localhost:39092");
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers", bootstrap_servers.to_string());

    let admin = AdminClient::from_config(&client_config).unwrap();

    let res: Vec<ConfigResource> = admin
        .describe_configs(
            &vec![ResourceSpecifier::Topic("out_topic")],
            &AdminOptions::default(),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.unwrap())
        .collect();

    for (k, v) in res[0].entry_map().into_iter() {
        println!("{}: {:?}", k, v.value);
    }

    Ok(())
}
