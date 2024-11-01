"""stream_aggregate example."""

import json
import signal
import sys
from collections import Counter

import pyarrow as pa
from denormalized import Context
from denormalized.datafusion import Accumulator, col
from denormalized.datafusion import functions as f
from denormalized.datafusion import lit, udaf


def signal_handler(sig, frame):
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

bootstrap_server = "localhost:9092"

sample_event = {
    "occurred_at_ms": 100,
    "sensor_name": "foo",
    "reading": 0.0,
}


class TotalValuesRead(Accumulator):
    # Define the state type as a struct containing a map
    acc_state_type = pa.struct([("counts", pa.map_(pa.string(), pa.int64()))])

    def __init__(self):
        self.counts = Counter()

    def update(self, values: pa.Array) -> None:
        # Update counter with new values
        if values is not None:
            self.counts.update(values.to_pylist())

    def merge(self, states: pa.Array) -> None:
        # Merge multiple states into this accumulator
        for state in states:
            if state is not None:
                count_map = state["counts"]
                # Iterate through the map's keys and values
                for k, v in zip(count_map.keys(), count_map.values()):
                    self.counts[k.as_py()] += v.as_py()

    def state(self) -> pa.Array:
        # Convert current state to Arrow array format
        if not self.counts:
            # Handle empty state
            return pa.array(
                [{"counts": pa.array([], type=pa.map_(pa.string(), pa.int64()))}],
                type=self.acc_state_type,
            )

        # Convert counter to key-value pairs
        keys, values = zip(*self.counts.items())

        # Create a single-element array containing our state struct
        return pa.array(
            [
                {
                    "counts": pa.array(
                        list(zip(keys, values)), type=pa.map_(pa.string(), pa.int64())
                    )
                }
            ],
            type=self.acc_state_type,
        )

    def evaluate(self) -> pa.Array:
        # Convert final state to output format
        if not self.counts:
            return pa.array([], type=pa.map_(pa.string(), pa.int64()))

        keys, values = zip(*self.counts.items())
        return pa.array(list(zip(keys, values)), type=pa.map_(pa.string(), pa.int64()))


input_type = [pa.string()]
return_type = pa.string()
state_type = [TotalValuesRead.acc_state_type]
sample_udaf = udaf(TotalValuesRead, input_type, return_type, state_type, "stable")


def print_batch(rb: pa.RecordBatch):
    if not len(rb):
        return
    print(rb)


ctx = Context()
ds = ctx.from_topic("temperature", json.dumps(sample_event), bootstrap_server)

ds.window(
    [],
    [
        sample_udaf(col("sensor_name")).alias("count"),
    ],
    1000,
    None,
).sink(print_batch)
