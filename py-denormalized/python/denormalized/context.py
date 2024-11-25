from denormalized._d_internal import PyContext
from .data_stream import DataStream


class Context:
    """A context manager for handling data stream operations.

    This class provides functionality to create and manage data streams
    from various sources like Kafka topics.
    """

    def __init__(self) -> None:
        """Initializes a new Context instance with PyContext."""
        self.ctx = PyContext()

    def __repr__(self):
        """Returns the string representation of the PyContext object.

        Returns:
            str: String representation of the underlying PyContext.
        """
        return self.ctx.__repr__()

    def __str__(self):
        """Returns the string representation of the PyContext object.

        Returns:
            str: String representation of the underlying PyContext.
        """
        return self.ctx.__str__()

    def from_topic(
        self,
        topic: str,
        sample_json: str,
        bootstrap_servers: str,
        timestamp_column: str | None = None,
        group_id: str = "default_group",
    ) -> DataStream:
        """Creates a new DataStream from a Kafka topic.

        Args:
            topic: The name of the Kafka topic to consume from.
            sample_json: A sample JSON string representing the expected message format.
            bootstrap_servers: Comma-separated list of Kafka broker addresses.
            timestamp_column: Optional column name containing message timestamps.
            group_id: Kafka consumer group ID, defaults to "default_group".

        Returns:
            DataStream: A new DataStream instance connected to the specified topic.
        """
        py_ds = self.ctx.from_topic(
            topic,
            sample_json,
            bootstrap_servers,
            group_id,
            timestamp_column,
        )
        ds = DataStream(py_ds)
        return ds
