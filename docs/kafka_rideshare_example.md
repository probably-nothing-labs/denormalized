# Kafka Rideshare Example

This example application aggregates data across a more involved example setup.

### Configure Kafka Cluster

Clone our [docker compose files for running kafka](https://github.com/probably-nothing-labs/kafka-monitoring-stack-docker-compose). If you already have a different kafka cluster running, you can skip this step.
```sh
git clone git@github.com:probably-nothing-labs/kafka-monitoring-stack-docker-compose.git
cd kafka-monitoring-stack-docker-compose
docker compose -f denormalized-benchmark-cluster.yml up
```

This will spin up a 3 node kafka cluster in docker along with an instance of kafka-ui that can be viewed at http://localhost:8080/

### Generate some sample data to the kafka cluster

We wrote a [small rust tool](https://github.com/probably-nothing-labs/benchmarking) that will send fake traffic to the locally run rust program.
```sh
git clone git@github.com:probably-nothing-labs/benchmarking.git
cd benchmarking
cargo run -- -d 60 -a 1000
```

This will start a simulation for 60s and will create two topics: `driver-imu-data` and `trips` which should have around ~58k and ~500 messages accordingly.
There are several other knobs that can be tuned to change the amount of traffic which can be viewed with `cargo run -- --help`.
There are also several other knobs that are not exposes but can be changed in the [src/main.rs](https://github.com/probably-nothing-labs/benchmarking/blob/main/src/main.rs#L104-L108) file

### Run a Streaming Datafusion job

```sh
cargo run --example kafka_rideshare
```

Once everything is setup and one of the two streaming jobs is running, it is recommend to re-run the kafka data generation tool so that live data is produced. This is because watermark tracking of streaming data makes it difficult to properly aggregate older data that lives in the kafka topic.
