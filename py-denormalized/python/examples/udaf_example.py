"""stream_aggregate example."""

import json
import signal
import sys
from collections import Counter
from typing import List
import pyarrow as pa

from denormalized import Context
from denormalized.datafusion import Accumulator, col
from denormalized.datafusion import functions as f
from denormalized.datafusion import udaf


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
        if states is None or len(states) == 0:
            return
        for state in states:
            if state is not None:
                counts_map = state.to_pylist()[0] # will always be one element struct
                for k, v in counts_map["counts"]:
                    self.counts[k] += v

    def state(self) -> List[pa.Scalar]:
        # Convert current state to Arrow array format
        result = {"counts": dict(self.counts.items())}
        return [pa.scalar(result, type=pa.struct([("counts", pa.map_(pa.string(), pa.int64()))]))]

    def evaluate(self) -> pa.Scalar:
        return self.state()[0]


input_type = [pa.string()]
return_type = TotalValuesRead.acc_state_type
state_type = [TotalValuesRead.acc_state_type]
sample_udaf = udaf(TotalValuesRead, input_type, return_type, state_type, "stable")


def print_batch(rb: pa.RecordBatch):
    if not len(rb):
        return
    print(rb)

ctx = Context()
ds = ctx.from_topic("temperature", json.dumps(sample_event), bootstrap_server, "occurred_at_ms")

ds = ds.window(
    [],
    [
        sample_udaf(col("sensor_name")).alias("count"),
    ],
    2000,
    None,
).sink(print_batch)
