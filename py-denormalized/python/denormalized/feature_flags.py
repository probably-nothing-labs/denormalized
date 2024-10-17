import importlib
from functools import wraps

def is_feast_available():
    return importlib.util.find_spec("feast") is not None

USE_FEAST = is_feast_available()

def feast_feature(func):
    """Decorator to mark functions that require feast."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if USE_FEAST:
            return func(*args, **kwargs)
        else:
            raise NotImplementedError("This feature requires feast to be installed.")
    return wrapper
