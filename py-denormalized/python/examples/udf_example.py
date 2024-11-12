"""stream_aggregate example."""

import json
import signal
import sys

import pyarrow as pa
import pyarrow.compute as pc
from denormalized import Context
from denormalized.datafusion import col
from denormalized.datafusion import functions as f
from denormalized.datafusion import lit, udf


def signal_handler(sig, frame):
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

bootstrap_server = "localhost:9092"
timestamp_column = "occurred_at_ms"

sample_event = {
    "occurred_at_ms": 100,
    "sensor_name": "foo",
    "reading": 0.0,
}


def gt(lhs: pa.Array, rhs: pa.Scalar) -> pa.Array:
    return pc.greater(lhs, rhs)


greater_than_udf = udf(gt, [pa.float64(), pa.float64()], pa.bool_(), "stable")


def print_batch(rb: pa.RecordBatch):
    if not len(rb):
        return
    print(rb)


ds = Context().from_topic(
    "temperature",
    json.dumps(sample_event),
    bootstrap_server,
    timestamp_column,
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
).with_column(
    "greater_than",
    greater_than_udf(
        col("count"),
        lit(1400.0),
    ),
).sink(
    print_batch
)
