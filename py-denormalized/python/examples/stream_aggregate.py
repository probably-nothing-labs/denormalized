import json

from denormalized import Context, DataStream
from datafusion import Expr

sample_event = {
    "occurred_at_ms": 100,
    "sensor_name": "foo",
    "reading": 0.0,
}

ctx = Context()
print(ctx)
# ds = ctx.from_topic("temperature", json.dumps(sample_event), "localhost:9092")

# expr = Expr.literal(4)
# print(expr)

# print(ds.schema())


from denormalized._internal import PyContext

# ctx_internal = PyContext()
# ctx_internal.foo()
