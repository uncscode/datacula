"""Test the convert module."""

import numpy as np
from datacula import convert


def test_coerce_type():
    """Test the coerce_type function."""
    # Test int input
    assert convert.coerce_type(42, int) == 42

    # Test float input
    assert convert.coerce_type(3.14, float) == 3.14

    # Test string input
    assert convert.coerce_type("hello", str) == "hello"

    # Test list input
    assert np.array_equal(convert.coerce_type([1, 2, 3], np.ndarray), np.array([1, 2, 3]))

    # Test tuple input
    assert np.array_equal(convert.coerce_type((1, 2, 3), np.ndarray), np.array([1, 2, 3]))

    # Test array input
    assert np.array_equal(convert.coerce_type(np.array([1, 2, 3]), np.ndarray), np.array([1, 2, 3]))

    # Test invalid conversion
    try:
        convert.coerce_type("hello", int)
    except ValueError:
        pass
    else:
        assert False, "Expected ValueError"


def test_round():
    # Test single float value
    assert convert.round(3.14, base=0.5, mode='round') == 3.0

    # Test list of float values
    assert np.array_equiv(
            convert.round([1.2, 2.5, 3.8], base=1.0, mode='round'),
            np.array([1.0, 2.0, 4.0])
        )

    # Test NumPy array of float values
    assert np.array_equal(convert.round(
            np.array([1.2, 2.5, 3.8]), base=1.0, mode='floor'),
            np.array([1.0, 2.0, 3.0]))

    # Test NumPy array of ceil values
    assert np.array_equal(convert.round(
            np.array([1.2, 2.5, 3.8]), base=1.0, mode='ceil'),
            np.array([2.0, 3.0, 4.0]))

    # Test rounding to non-integer base
    assert convert.round(3.14, base=0.1, mode='round') == 3.1

    # Test rounding mode "round_nonzero"
    assert np.array_equal(convert.round(
            [0, 0.2, 0.3, 0.6, 3], base=1.0, mode='round', nonzero_edge=True),
            np.array([0, 0.2, 0.3, 1, 3]))

    # Test invalid mode parameter
    try:
        convert.round([1.2, 2.5, 3.8], base=1.0, mode='invalid')
    except ValueError:
        pass
    else:
        assert False, "Expected ValueError"


