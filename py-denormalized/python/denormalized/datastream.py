from denormalized._internal import PyDataStream

import pyarrow as pa


class DataStream:
    """DataStream."""

    def __init__(self, ds: PyDataStream) -> None:
        """__init__."""
        self.ds = ds

    def __repr__(self):
        return self.ds.__repr__()

    def __str__(self):
        return self.ds.__str__()

    def schema(self) -> pa.Schema:
        """Schema."""
        return self.ds.schema()
