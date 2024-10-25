from denormalized._internal import PyContext
from .data_stream import DataStream

class Context:
    """Context."""

    def __init__(self) -> None:
        """__init__."""
        self.ctx = PyContext()

    def __repr__(self):
        return self.ctx.__repr__()

    def __str__(self):
        return self.ctx.__str__()

    def from_topic(self, topic: str, sample_json: str, bootstrap_servers: str) -> DataStream:
        """Create a new context from a topic."""
        py_ds = self.ctx.from_topic(topic, sample_json, bootstrap_servers)
        ds = DataStream(py_ds)

        return ds

