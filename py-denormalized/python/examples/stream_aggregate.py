"""stream_aggregate example."""

import json
import signal
import sys

from denormalized import Context
from denormalized.datafusion import col
from denormalized.datafusion import functions as f
from denormalized.datafusion import lit


def signal_handler(sig, frame):
    print("You pressed Ctrl+C!")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

bootstrap_server = "localhost:9092"

sample_event = {
    "occurred_at_ms": 100,
    "sensor_name": "foo",
    "reading": 0.0,
}


def print_batch(rb):
    print(rb)


ctx = Context()
ds = ctx.from_topic("temperature", json.dumps(sample_event), bootstrap_server)


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
