"""stream_aggregate example.

docker run --rm -p 9092:9092 emgeee/kafka_emit_measurements:latest
"""

import json
import pprint as pp
import signal
import sys

from denormalized import Context
from denormalized.datafusion import col
from denormalized.datafusion import functions as f


def signal_handler(_sig, _frame):
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


ctx = Context()
temperature_ds = ctx.from_topic(
    "temperature", json.dumps(sample_event), bootstrap_server, timestamp_column
)

humidity_ds = (
    ctx.from_topic(
        "humidity",
        json.dumps(sample_event),
        bootstrap_server,
        timestamp_column,
    )
    .with_column("humidity_sensor", col("sensor_name"))
    .drop_columns(["sensor_name"])
    .window(
        [col("humidity_sensor")],
        [
            f.count(col("reading")).alias("avg_humidity"),
        ],
        4000,
        None,
    )
    .with_column("humidity_window_start_time", col("window_start_time"))
    .with_column("humidity_window_end_time", col("window_end_time"))
    .drop_columns(["window_start_time", "window_end_time"])
)

joined_ds = (
    temperature_ds.window(
        [col("sensor_name")],
        [
            f.avg(col("reading")).alias("avg_temperature"),
        ],
        4000,
        None,
    )
    .join(
        humidity_ds,
        "inner",
        ["sensor_name", "window_start_time"],
        ["humidity_sensor", "humidity_window_start_time"],
    )
    .sink(print_batch)
)
