from denormalized._internal import PyDataStream

import pyarrow as pa


class DataStream:
    """DataStream."""

    def __init__(self, ds: PyDataStream) -> None:
        """__init__."""
        self.ds = ds

    def schema(self) -> pa.Schema:
        """Schema."""
        return self.ds.schema()
