from typing import Callable

import pyarrow as pa
from denormalized._d_internal import PyDataStream
from denormalized.datafusion import Expr
from denormalized.utils import to_internal_expr, to_internal_exprs


class DataStream:
    """Represents a stream of data that can be manipulated using various operations.

    This class provides a high-level interface for stream processing operations, including
    filtering, joining, windowing, and sinking data to various destinations. It wraps
    a Rust-implemented PyDataStream object.

    Attributes:
        ds (PyDataStream): The underlying Rust-side DataStream implementation
    """

    def __init__(self, ds: PyDataStream) -> None:
        """Initialize a new DataStream object.

        Args:
            ds: The underlying PyDataStream object from the Rust implementation
        """
        self.ds = ds

    def __repr__(self):
        """Return a string representation of the DataStream object.

        Returns:
            str: A string representation of the DataStream
        """
        return self.ds.__repr__()

    def __str__(self):
        """Return a string description of the DataStream object.

        Returns:
            str: A human-readable description of the DataStream
        """
        return self.ds.__str__()

    def schema(self) -> pa.Schema:
        """Get the schema of the DataStream.

        Returns:
            pa.Schema: The PyArrow schema describing the structure of the data
        """
        return self.ds.schema()

    def select(self, expr_list: list[Expr]) -> "DataStream":
        """Select specific columns or expressions from the DataStream.

        Args:
            expr_list: List of expressions defining the columns or computations to select

        Returns:
            DataStream: A new DataStream containing only the selected expressions

        Example:
            >>> ds.select([col("name"), col("age") + 1])
        """
        return DataStream(self.ds.select(to_internal_exprs(expr_list)))

    def filter(self, predicate: Expr) -> "DataStream":
        """Filter the DataStream based on a predicate.

        Args:
            predicate: Boolean expression used to filter rows

        Returns:
            DataStream: A new DataStream containing only rows that satisfy the predicate

        Example:
            >>> ds.filter(col("age") > 18)
        """
        return DataStream(self.ds.filter(to_internal_expr(predicate)))

    def with_column(self, name: str, predicate: Expr) -> "DataStream":
        """Add a new column to the DataStream.

        Args:
            name: Name of the new column
            predicate: Expression defining the values for the new column

        Returns:
            DataStream: A new DataStream with the additional column

        Example:
            >>> ds.with_column("adult", col("age") >= 18)
        """
        return DataStream(self.ds.with_column(name, to_internal_expr(predicate)))

    def drop_columns(self, columns: list[str]) -> "DataStream":
        """Drops specified columns from the DataStream.

        Args:
            columns: List of column names to remove

        Returns:
            DataStream: A new DataStream without the specified columns
        """
        return DataStream(self.ds.drop_columns(columns))

    def join_on(
        self, right: "DataStream", join_type: str, on_exprs: list[Expr]
    ) -> "DataStream":
        """Join this DataStream with another one based on join expressions.

        Args:
            right: The right DataStream to join with
            join_type: Type of join ('inner', 'left', 'right', 'full')
            on_exprs: List of expressions defining the join conditions

        Returns:
            DataStream: A new DataStream resulting from the join operation

        Example:
            >>> left.join_on(right, "inner", [col("id") == col("right.id")])
        """
        return DataStream(self.ds.join_on(right.ds, join_type, on_exprs))

    def join(
        self,
        right: "DataStream",
        join_type: str,
        left_cols: list[str],
        right_cols: list[str],
        filter: Expr | None = None,
    ) -> "DataStream":
        """Join this DataStream with another one based on column names.

        Args:
            right: The right DataStream to join with
            join_type: Type of join ('inner', 'left', 'right', 'full')
            left_cols: Column names from the left DataStream to join on
            right_cols: Column names from the right DataStream to join on
            filter: Optional additional join filter expression

        Returns:
            DataStream: A new DataStream resulting from the join operation

        Example:
            >>> left.join(right, "inner", ["id"], ["right_id"])
        """
        return DataStream(
            self.ds.join(right.ds, join_type, left_cols, right_cols, filter)
        )

    def window(
        self,
        group_exprs: list[Expr],
        aggr_exprs: list[Expr],
        window_length_millis: int,
        slide_millis: int | None = None,
    ) -> "DataStream":
        """Apply a windowing operation to the DataStream.

        If `slide_millis` is `None` a tumbling window will be created otherwise a sliding window will be created.

        Args:
            group_exprs: List of expressions to group by
            aggr_exprs: List of aggregation expressions to apply
            window_length_millis: Length of the window in milliseconds
            slide_millis: Optional slide interval in milliseconds (defaults to None)

        Returns:
            DataStream: A new DataStream with the windowing operation applied

        Example:
            >>> ds.window([col("user_id")], [sum(col("value"))], 60000)  # 1-minute window
        """
        return DataStream(
            self.ds.window(
                to_internal_exprs(group_exprs),
                to_internal_exprs(aggr_exprs),
                window_length_millis,
                slide_millis,
            )
        )

    def print_stream(self) -> None:
        """Print the contents of the DataStream to stdout."""
        self.ds.print_stream()

    def print_schema(self) -> "DataStream":
        """Print the schema of the DataStream to stdout.

        Returns:
            DataStream: Self for method chaining
        """
        return DataStream(self.ds.print_schema())

    def print_plan(self) -> "DataStream":
        """Print the logical execution plan of the DataStream to stdout.

        Returns:
            DataStream: Self for method chaining
        """
        return DataStream(self.ds.print_plan())

    def print_physical_plan(self) -> "DataStream":
        """Print the physical execution plan of the DataStream to stdout.

        Returns:
            DataStream: Self for method chaining
        """
        return DataStream(self.ds.print_physical_plan())

    def sink_kafka(self, bootstrap_servers: str, topic: str) -> None:
        """Sink the DataStream to a Kafka topic.

        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            topic: Name of the Kafka topic to write to

        Raises:
            ConnectionError: If unable to connect to Kafka brokers
        """
        self.ds.sink_kafka(bootstrap_servers, topic)

    def sink(self, func: Callable[[pa.RecordBatch], None]) -> None:
        """Sink the DataStream to a Python callback function.

        Args:
            func: Callback function that receives PyArrow RecordBatches

        Example:
            >>> ds.sink(lambda batch: print(f"Received batch with {len(batch)} rows"))
        """
        self.ds.sink_python(func)
