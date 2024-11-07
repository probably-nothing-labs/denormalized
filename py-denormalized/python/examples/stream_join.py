"""stream_aggregate example."""

import json
import signal
import sys
import pprint as pp

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
    pp.pprint(rb.to_pydict())


ctx = Context()
temperature_ds = ctx.from_topic(
    "temperature", json.dumps(sample_event), bootstrap_server
)

humidity_ds = (
    ctx.from_topic("humidity", json.dumps(sample_event), bootstrap_server)
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
