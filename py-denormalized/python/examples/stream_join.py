"""stream_aggregate example."""

import json
import signal
import sys

from denormalized import Context
from denormalized.datafusion import col, expr
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
    print(rb.to_pydict())


ctx = Context()
temperature_ds = ctx.from_topic(
    "temperature", json.dumps(sample_event), bootstrap_server
).window(
    [],
    [
        f.count(col("reading"), distinct=False, filter=None).alias("temp_count"),
    ],
    4000,
    None,
)

humidity_ds = ctx.from_topic(
    "humidity", json.dumps(sample_event), bootstrap_server
).window(
    [],
    [
        f.count(col("reading"), distinct=False, filter=None).alias("temp_count"),
    ],
    4000,
    None,
)

temperature_ds = temperature_ds.join(
    humidity_ds, "left", ["sensor_name"], ["sensor_name"]
).sink(print_batch)

# temperature_ds = temperature_ds.window(
#     [],
#     [
#         f.count(col("temperature.reading"), distinct=False, filter=None).alias(
#             "temperature_count"
#         ),
#         f.count(col("humidity.reading"), distinct=False, filter=None).alias(
#             "humidity_count"
#         ),
#     ],
#     4000,
#     None,
# ).sink(print_batch)
