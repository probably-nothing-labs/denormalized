<h1>
  <a href="https://www.denormalized.io">
    <img src="./docs/images/denormalized_dark.png" alt="Denormalized Logo" width="512">
  </a>
</h1>

Denormalized is a fast embeddable stream processing engine built on Apache DataFusion.
It currently supports sourcing and sinking to kafka, windowed aggregations, and stream joins.

This repo is still a *work-in-progress* and we are actively seeking design partners. If you have have a specific use-case you'd like to discuss please drop us a line via a github issue or email hello@denormalized.io.

## Building Denormalized

Simply run `cargo build`

## Running Examples

See our [benchmarking repo](https://github.com/probably-nothing-labs/benchmarking) for local Kafka setup and data generation.

With the data generation in place, run -

`cargo run --example kafka_rideshare`

## Credits

Denormalized is built and maintained by [Denormalized](https://www.denormalized.io) in San Francisco. Please drop in a line to
hello@denormalized.io or simply open up a GitHub Issue!
