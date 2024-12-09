import inspect
import logging
from typing import Any, TypeVar, Union, cast, get_type_hints

import pyarrow as pa
from denormalized._d_internal import PyDataStream
from feast import FeatureStore, Field
from feast.data_source import PushMode
from feast.type_map import pa_to_feast_value_type
from feast.types import from_value_type

from .data_stream import DataStream

logger = logging.getLogger(__name__)

T = TypeVar("T")


class FeastDataStreamMeta(type):
    """Metaclass that modifies DataStream return types to FeastDataStream.

    This metaclass wraps methods from DataStream that return DataStream types
    to instead return FeastDataStream types.
    """

    def __new__(cls, name: str, bases: tuple, attrs: dict) -> Any:
        """Create a new class with modified return types.

        Args:
            name: Name of the class being created
            bases: Tuple of base classes
            attrs: Dictionary of class attributes

        Returns:
            Any: New class with modified method return types
        """
        # Get all methods from DataStream that return DataStream
        datastream_methods = inspect.getmembers(
            DataStream,
            predicate=lambda x: (
                inspect.isfunction(x) and get_type_hints(x).get("return") == DataStream
            ),
        )
        # For each method that returns DataStream, create a wrapper that returns FeastDataStream
        for method_name, method in datastream_methods:
            if method_name not in attrs:  # Only wrap if not already defined

                def create_wrapper(method_name):
                    def wrapper(self, *args, **kwargs):
                        result = getattr(
                            super(cast(type, self.__class__), self), method_name
                        )(*args, **kwargs)
                        return self.__class__(result)

                    # Copy original method's signature but change return type
                    hints = get_type_hints(getattr(DataStream, method_name))
                    hints["return"] = (
                        "FeastDataStream"  # Use string to handle forward reference
                    )
                    wrapper.__annotations__ = hints
                    return wrapper

                attrs[method_name] = create_wrapper(method_name)
        return super().__new__(cls, name, bases, attrs)


class FeastDataStream(DataStream, metaclass=FeastDataStreamMeta):
    """A DataStream subclass with additional Feast-specific functionality.

    This class extends DataStream to provide integration with Feast feature stores
    and related functionality.

    You must install with the extra 'feast': `pip install denormalized[feast]`
    """

    def __init__(self, stream: Union[PyDataStream, DataStream]) -> None:
        """Initialize a FeastDataStream from either a PyDataStream or DataStream.

        Args:
            stream: Either a PyDataStream object or a DataStream object
        """
        if isinstance(stream, DataStream):
            super().__init__(stream.ds)
        else:
            super().__init__(stream)

    def get_feast_schema(self) -> list[Field]:
        """Get the Feast schema for this DataStream.

        Returns:
            list[Field]: List of Feast Field objects representing the schema
        """
        return [
            Field(
                name=s.name, dtype=from_value_type(pa_to_feast_value_type(str(s.type)))
            )
            for s in self.schema()
        ]

    def write_feast_feature(
        self, feature_store: FeatureStore, source_name: str
    ) -> None:
        """Write the DataStream to a Feast feature store.

        Args:
            feature_store: The target Feast feature store
            source_name: Name of the feature source in Feast

        Note:
            Any exceptions during the push operation will be printed but not raised.
        """

        def _sink_to_feast(rb: pa.RecordBatch):
            if len(rb):
                df = rb.to_pandas()
                try:
                    feature_store.push(source_name, df, to=PushMode.ONLINE)
                except Exception as e:
                    logger.error(f"Failed to push to Feast feature store: {e}", exc_info=True)

        self.ds.sink_python(_sink_to_feast)
