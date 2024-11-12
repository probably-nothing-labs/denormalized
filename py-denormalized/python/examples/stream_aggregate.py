"""stream_aggregate example.

docker build -t emgeee/kafka_emit_measurements:latest .
"""

import json
import pprint as pp
import signal
import sys

from denormalized import Context
from denormalized.datafusion import col
from denormalized.datafusion import functions as f
from denormalized.datafusion import lit


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


def print_batch(rb):
    pp.pprint(rb.to_pydict())


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
        f.median(col("reading")).alias("median"),
        f.stddev(col("reading")).alias("stddev"),
    ],
    1000,
    None,
).filter(col("max") > (lit(113))).sink(print_batch)
