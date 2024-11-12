from typing import Callable

import pyarrow as pa
from denormalized._d_internal import PyDataStream
from denormalized.datafusion import Expr
from denormalized.utils import to_internal_expr, to_internal_exprs


class DataStream:
    """Represents a stream of data that can be manipulated using various operations."""

    def __init__(self, ds: PyDataStream) -> None:
        """Initialize a new DataStream object.

        Args:
            ds (PyDataStream): The underlying PyDataStream object.
        """
        self.ds = ds

    def __repr__(self):
        """Return a string representation of the DataStream object.

        Returns:
            str: A string representation of the DataStream.
        """
        return self.ds.__repr__()

    def __str__(self):
        """Return a string description of the DataStream object.

        Returns:
            str: A string description of the DataStream.
        """
        return self.ds.__str__()

    def schema(self) -> pa.Schema:
        """Get the schema of the DataStream.

        Returns:
            pa.Schema: The PyArrow schema of the DataStream.
        """
        return self.ds.schema()

    def select(self, expr_list: list[Expr]) -> "DataStream":
        """Select specific columns or expressions from the DataStream.

        Args:
            expr_list (list[Expr]): A list of expressions to select.

        Returns:
            DataStream: A new DataStream with the selected columns/expressions.
        """
        return DataStream(self.ds.select(to_internal_exprs(expr_list)))

    def filter(self, predicate: Expr) -> "DataStream":
        """Filter the DataStream based on a predicate.

        Args:
            predicate (Expr): The filter predicate.

        Returns:
            DataStream: A new DataStream with the filter applied.
        """
        return DataStream(self.ds.filter(to_internal_expr(predicate)))

    def with_column(self, name: str, predicate: Expr) -> "DataStream":
        """Add a new column to the DataStream.

        Args:
            name (str): The name of the new column.
            predicate (Expr): The expression that defines the column's values.

        Returns:
            DataStream: A new DataStream with the additional column.
        """
        return DataStream(self.ds.with_column(name, to_internal_expr(predicate)))

    def drop_columns(self, columns: list[str]) -> "DataStream":
        """Drops columns from the DataStream."""
        return DataStream(self.ds.drop_columns(columns))

    def join_on(
        self, right: "DataStream", join_type: str, on_exprs: list[Expr]
    ) -> "DataStream":
        """Join this DataStream with another one based on join expressions.

        Args:
            right (DataStream): The right DataStream to join with.
            join_type (str): The type of join to perform.
            on_exprs (list[Expr]): The expressions to join on.

        Returns:
            DataStream: A new DataStream resulting from the join operation.
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
            right (DataStream): The right DataStream to join with.
            join_type (str): The type of join to perform.
            left_cols (list[str]): The columns from the left DataStream to join on.
            right_cols (list[str]): The columns from the right DataStream to join on.
            filter (Expr, optional): An additional filter to apply to the join.

        Returns:
            DataStream: A new DataStream resulting from the join operation.
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

        Args:
            group_exprs (list[Expr]): The expressions to group by.
            aggr_exprs (list[Expr]): The aggregation expressions to apply.
            window_length_millis (int): The length of the window in milliseconds.
            slide_millis (int, optional): The slide interval of the window in
                milliseconds.

        Returns:
            DataStream: A new DataStream with the windowing operation applied.
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
        """Print the contents of the DataStream."""
        self.ds.print_stream()

    def print_schema(self) -> "DataStream":
        """Print the schema of the DataStream.

        Returns:
            DataStream: This DataStream object for method chaining.
        """
        return DataStream(self.ds.print_schema())

    def print_plan(self) -> "DataStream":
        """Print the execution plan of the DataStream.

        Returns:
            DataStream: This DataStream object for method chaining.
        """
        return DataStream(self.ds.print_plan())

    def print_physical_plan(self) -> "DataStream":
        """Print the physical execution plan of the DataStream.

        Returns:
            DataStream: This DataStream object for method chaining.
        """
        return DataStream(self.ds.print_physical_plan())

    def sink_kafka(self, bootstrap_servers: str, topic: str) -> None:
        """Sink the DataStream to a Kafka topic.

        Args:
            bootstrap_servers (str): The Kafka bootstrap servers.
            topic (str): The Kafka topic to sink the data to.
        """
        self.ds.sink_kafka(bootstrap_servers, topic)

    def sink(self, func: Callable[[pa.RecordBatch], None]) -> None:
        """Sink the DataStream to a Python function."""
        self.ds.sink_python(func)
