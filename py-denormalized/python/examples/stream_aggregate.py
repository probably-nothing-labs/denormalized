"""stream_aggregate example."""
import json

import pyarrow as pa
from denormalized import Context
from denormalized.datafusion import Expr
from denormalized.datafusion import functions as f
# from denormalized._internal import expr
# from denormalized._internal import functions as f

import signal
import sys

def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

bootstrap_server = "localhost:9092"

sample_event = {
    "occurred_at_ms": 100,
    "sensor_name": "foo",
    "reading": 0.0,
}

def sample_sink_func(rb):
    print(rb)

ctx = Context()
ds = ctx.from_topic("temperature", json.dumps(sample_event), bootstrap_server)


ds.window(
    [Expr.column("sensor_name")],
    [
        f.count(Expr.column("reading"), distinct=False, filter=None).alias(
            "count"
        ),
        f.min(Expr.column("reading")).alias("min"),
        f.max(Expr.column("reading")).alias("max"),
        f.avg(Expr.column("reading")).alias("average"),
    ],
    1000,
    None,
).filter(
    Expr.column("max") > (Expr.literal(pa.scalar(113)))
).sink_python(sample_sink_func)
