"""
[Denormalized](https://www.denormalized.io/) is a single node stream processing engine written in Rust and powered by Apache DataFusion 🚀

1. Install denormalized `pip install denormalized`
2. Start the custom docker image that contains an instance of kafka along with with a script that emits some sample data to kafka `docker run --rm -p 9092:9092 emgeee/kafka_emit_measurements:latest`

```python
sample_event = {
    "occurred_at_ms": 100,
    "sensor_name": "foo",
    "reading": 0.0,
}

def print_batch(rb):
    pp.pprint(rb.to_pydict())

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
    ],
    1000,
    None,
).sink(print_batch)
```


Head on over to the [examples folder](https://github.com/probably-nothing-labs/denormalized/tree/main/py-denormalized/python/examples) to see more examples that demonstrate additional functionality including stream joins and user defined (aggregate) functions.

"""

from .context import Context
from .data_stream import DataStream
from .datafusion import col, column
from .datafusion import functions as Functions
from .datafusion import lit, literal, udaf, udf
from .datafusion.expr import Expr

__all__ = [
    "Context",
    "DataStream",
    "col",
    "column",
    "Expr",
    "Functions",
    "lit",
    "literal",
    "udaf",
    "udf",
]

__docformat__ = "google"

try:
    from .feast_data_stream import FeastDataStream

    __all__.append("FeastDataStream")
except ImportError:
    pass
