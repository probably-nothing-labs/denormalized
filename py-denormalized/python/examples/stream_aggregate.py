import json

from denormalized import Context

sample_event = {
    "occurred_at_ms": 100,
    "sensor_name": "foo",
    "reading": 0.0,
}

ctx = Context()

ds = ctx.from_topic("temperature", json.dumps(sample_event), "localhost:9092")
