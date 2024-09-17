"""stream_aggregate example."""
import json

import pyarrow as pa
from denormalized import Context
from denormalized._internal import expr
from denormalized._internal import functions as f

bootstrap_server = "localhost:9092"

sample_event = {
    "occurred_at_ms": 100,
    "sensor_name": "foo",
    "reading": 0.0,
}

ctx = Context()
ds = ctx.from_topic("temperature", json.dumps(sample_event), bootstrap_server)


ds.window(
    [expr.Expr.column("sensor_name")],
    [
        f.count(expr.Expr.column("reading"), distinct=False, filter=None).alias(
            "count"
        ),
        f.min(expr.Expr.column("reading")).alias("min"),
        f.max(expr.Expr.column("reading")).alias("max"),
        f.avg(expr.Expr.column("reading")).alias("average"),
    ],
    1000,
    None,
).filter(
    expr.Expr.column("max") > (expr.Expr.literal(pa.scalar(113)))
).sink_kafka(bootstrap_server, "out_py_topic")
