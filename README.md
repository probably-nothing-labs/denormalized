# Denormalized

Denormalized is a fast embeddable stream processing engine built on Apache DataFusion.

While this repo is very much a *work-in-progress*, we currently support windowed aggregations and joins on streams of data with a 
connector available for Kafka.

## Building Denormalized

Simply run `cargo build`

## Running Examples

See our [benchmarking repo](https://github.com/probably-nothing-labs/benchmarking) for local Kafka setup and data generation.

With the data generation in place, run -

`cargo run --example kafka_rideshare`

## Credits

Denormalized is built and maintained by [Denormalized Inc](www.denormalized.io) from San Francisco. Please drop in a line to 
hello@denormalized.io or simply open up a GitHub Issue.
