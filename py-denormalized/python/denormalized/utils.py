from denormalized._d_internal import expr as internal_exprs
from denormalized.datafusion import Expr


def to_internal_expr(expr: Expr | str) -> internal_exprs:
    """Convert a single Expr or string to internal expression.

    Args:
        expr (Union[Expr, str]): The expression to convert, either an Expr object
            or a string column name.

    Returns:
        internal_exprs: The converted internal expression.
    """
    return Expr.column(expr).expr if isinstance(expr, str) else expr.expr


def to_internal_exprs(exprs: list[Expr] | list[str]) -> list[internal_exprs]:
    """Convert a list of expressions or strings to internal expressions.

    Args:
        exprs (Union[list[Expr], list[str]]): List of expressions to convert,
            can be either Expr objects or string column names.

    Returns:
        list[internal_exprs]: List of converted internal expressions.
    """
    return [to_internal_expr(arg) for arg in exprs]
