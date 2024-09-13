import pytest
import denormalized


def test_sum_as_string():
    assert denormalized._internal.sum_as_string(1, 1) == "2"
