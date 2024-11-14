from denormalized._d_internal import PyContext
from .data_stream import DataStream


class Context:
    """A context manager for handling data stream operations.

    This class provides an interface for creating and managing data streams,
    particularly for working with Kafka topics and stream processing.

    Attributes:
        ctx: Internal PyContext instance managing Rust-side operations
    """

    def __init__(self) -> None:
        """Initialize a new Context instance."""
        self.ctx = PyContext()

    def __repr__(self):
        """Return a string representation of the Context object.

        Returns:
            str: A detailed string representation of the context
        """
        return self.ctx.__repr__()

    def __str__(self):
        """Return a readable string description of the Context object.

        Returns:
            str: A human-readable string description
        """
        return self.ctx.__str__()

    def from_topic(
        self,
        topic: str,
        sample_json: str,
        bootstrap_servers: str,
        timestamp_column: str,
        group_id: str = "default_group",
    ) -> DataStream:
        """Create a new DataStream from a Kafka topic.

        Args:
            topic: Name of the Kafka topic to consume from
            sample_json: Sample JSON string representing the expected message format
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            timestamp_column: Column name containing event timestamps
            group_id: Kafka consumer group ID (defaults to "default_group")

        Returns:
            DataStream: A new DataStream instance configured for the specified topic

        Raises:
            ValueError: If the topic name is empty or invalid
            ConnectionError: If unable to connect to Kafka brokers
        """
        py_ds = self.ctx.from_topic(
            topic,
            sample_json,
            bootstrap_servers,
            timestamp_column,
            group_id,
        )
        ds = DataStream(py_ds)
        return ds
