<h1>
  <a href="https://www.denormalized.io">
    <img src="./docs/images/denormalized_dark.png" alt="Denormalized Logo" width="512">
  </a>
</h1>

Denormalized is a fast embeddable stream processing engine built on Apache DataFusion.
It currently supports kafka as a real-time source and sink, windowed aggregations, and stream joins.

Denormalized is *work-in-progress* and we are actively seeking design partners. If you have have a specific use-case you'd like to discuss please drop us a line via a [github issue](https://github.com/probably-nothing-labs/denormalized/issues) or email `hello@denormalized.io`.


Here's an example job that aggregates sensor values from a kafka topic:

```rust
// Connect to source topic
let source_topic = topic_builder
    .with_topic(String::from("temperature"))
    .infer_schema_from_json(get_sample_json().as_str())?
    .with_encoding("json")?
    .with_timestamp(String::from("occurred_at_ms"), TimestampUnit::Int64Millis)
    .build_reader(ConnectionOpts::from([
        ("auto.offset.reset".to_string(), "latest".to_string()),
        ("group.id".to_string(), "sample_pipeline".to_string()),
    ]))
    .await?;

ctx.from_topic(source_topic)
    .await?
    .window(
        vec![col("sensor_name")],
        vec![
            count(col("reading")).alias("count"),
            min(col("reading")).alias("min"),
            max(col("reading")).alias("max"),
            avg(col("reading")).alias("average"),
        ],
        Duration::from_millis(1_000), // aggregate every 1 second
        None,                         // None means tumbling window
    )?
    .filter(col("max").gt(lit(113)))?
    .print_stream() // Print out the results
    .await?;
```

Denormalized also has python bindings in the [py-denormalized/](py-denormalized/) folder. Here is the same example using python:

```python
import json
from denormalized import Context
from denormalized.datafusion import col
from denormalized.datafusion import functions as f
from denormalized.datafusion import lit

sample_event = {
    "occurred_at_ms": 100,
    "sensor_name": "foo",
    "reading": 0.0,
}

def print_batch(rb):
    print(rb)

ds = Context().from_topic(
    "temperature",
    json.dumps(sample_event),
    "localhost:9092",
    "occurred_at_ms",
)

ds.window(
    [col("sensor_name")],
    [
        f.count(col("reading"), distinct=False, filter=None).alias("count"),
        f.min(col("reading")).alias("min"),
        f.max(col("reading")).alias("max"),
        f.avg(col("reading")).alias("average"),
    ],
    1000,
    None,
).filter(col("max") > (lit(113))).sink(print_batch)
```

The python version is available on [pypi](https://pypi.org/project/denormalized/0.0.4/): `pip install denormalized`

Details about developing the python bindings can be found in [py-denormalized/README.md](py-denormalized/README.md)

## Rust Quick Start

### Prerequisites

- Docker
- Rust/Cargo installed

### Running an example

1. Start the custom docker image that contains an instance of kafka along with with a script that emits some sample data to kafka `docker run --rm -p 9092:9092 --name emit_measuremetns emgeee/kafka_emit_measurements:latest`
2. Run a [simple streaming aggregation](./examples/examples/simple_aggregation.rs) on the data using denormalized: `cargo run --example simple_aggregation`

### Checkpointing

We use SlateDB for state backend. Initialize your Job Context with a custom config and a path for SlateDB backend to store state -

```
    let config = Context::default_config().set_bool("denormalized_config.checkpoint", true);
    let ctx = Context::with_config(config)?
        .with_slatedb_backend(String::from("/tmp/checkpoints/simple-agg/job1"))
        .await;
```

The job with automatically recover from state if a previous checkpoint exists.

## More examples

A more powerful example can be seen in our [Kafka ridesharing example](./docs/kafka_rideshare_example.md)

## Credits

Denormalized is built and maintained by [Denormalized](https://www.denormalized.io) in San Francisco.
