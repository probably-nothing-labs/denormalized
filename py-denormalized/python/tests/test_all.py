import pytest
import denormalized_python


def test_sum_as_string():
    assert denormalized_python.sum_as_string(1, 1) == "2"
