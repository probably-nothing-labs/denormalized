"""
.. include:: ../../README.md
   :start-line: 1
   :end-before: Development
"""

from .context import Context
from .data_stream import DataStream
from .datafusion import col, column, lit, literal, udf, udaf
from .datafusion.expr import Expr
from .datafusion import functions as Functions

__all__ = [
    "Context",
    "DataStream",
    "col",
    "column",
    "Expr",
    "Functions",
    "lit",
    "literal",
    "udaf",
    "udf",
]

__docformat__ = "google"

try:
    from .feast_data_stream import FeastDataStream

    __all__.append("FeastDataStream")
except ImportError:
    pass
