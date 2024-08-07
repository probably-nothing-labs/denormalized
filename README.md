<h1>
  <a href="https://www.denormalized.io">
    <img src="./docs/images/denormalized_dark.png" alt="Denormalized Logo" width="512">
  </a>
</h1>

Denormalized is a fast embeddable stream processing engine built on Apache DataFusion.
It currently supports sourcing and sinking to kafka, windowed aggregations, and stream joins.

This repo is still a *work-in-progress* and we are actively seeking design partners. If you have have a specific use-case you'd like to discuss please drop us a line via a github issue or email hello@denormalized.io.

## Quickstart

### Prerequisites
- Docker
- Rust/Cargo installed

### Running an example
1. Start kafka in docker `docker run -p 9092:9092 --name kafka apache/kafka`
2. Start emitting some sample data: `cargo run --example emit_measurements`
3. Run a [simple streaming aggregation](./examples/examples/simple_aggregation.rs) on the data using denormalized: `cargo run --example emit_measurements`

## More examples

A more powerful example can be seen in our [kafka ridesharing example](./docs/kafka_rideshare_example.md)

## Roadmap
- [x] Stream aggregation
- [x] Stream joins
- [ ] Checkpointing / restoration
- [ ] Session windows
- [ ] Stateful UDF API
- [ ] DuckDB support
- [ ] Reading/writing from Postgres
- [ ] Python bindings
- [ ] Typescript bindings
- [ ] UI

## Credits

Denormalized is built and maintained by [Denormalized](https://www.denormalized.io) in San Francisco. Please drop in a line to
hello@denormalized.io or simply open up a GitHub Issue!
