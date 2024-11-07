from denormalized._d_internal import expr as internal_exprs
from denormalized.datafusion import Expr


def to_internal_expr(expr: Expr | str) -> internal_exprs:
    """Convert a single Expr or string to internal exprs."""
    return Expr.column(expr).expr if isinstance(expr, str) else expr.expr

def to_internal_exprs(exprs: list[Expr] | list[str]) -> list[internal_exprs]:
    """Convert a list of Expr or string to a list of internal exprs."""
    return [
         to_internal_expr(arg) for arg in exprs
    ]
