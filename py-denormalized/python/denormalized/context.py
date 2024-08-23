from denormalized._internal import PyContext

class Context:
    """Context."""

    def __init__(self) -> None:
        """__init__."""
        self.ctx = PyContext()
