"""Test the convert module."""

import numpy as np
from datacula import stats


def test_merge_format_str_headers():
    """Test the merge_different_headers function."""
    # Create example input data
    data = np.array([[1, 2, 4], [3, 4, 5]]).T
    header = ['a', 'b', 'c']
    data_add = np.array([[5, 5], [8, 8]]).T
    header_add = ['c', 'd']

    # Call function to merge the data and headers
    data, header, merged_data, merged_header = stats.merge_formating(
        data_current=data,
        header_current=header,
        data_new=data_add,
        header_new=header_add
    )

    # Test merged data shape
    assert merged_data.shape == (4, 2)

    # Test merged header
    assert merged_header == ['a', 'b', 'c', 'd']


def test_merge_format_num_headers():
    """Test the merge_different_headers function."""
    # Create example input data
    data = np.array([[1, 2, 4], [3, 4, 5]]).T
    header = ['1', '2', '3']
    data_add = np.array([[5, 5], [8, 8]]).T
    header_add = ['3', '4']

    # Call function to merge the data and headers
    data, header, merged_data, merged_header = stats.merge_formating(
        data_current=data,
        header_current=header,
        data_new=data_add,
        header_new=header_add
    )

    # Test merged data shape
    assert merged_data.shape == (4, 2)

    # Test merged header
    assert merged_header == ['1', '2', '3', '4']


def test_average_to_interval():
    """Test the average_to_interval function."""
    # Set up test data
    time_stream = np.arange(0, 1000, 10)
    average_base_sec = 10
    average_base_time = np.arange(0, 1000, 60)
    data_stream = np.linspace(0, 1000, len(time_stream))
    data_stream = np.vstack((data_stream, data_stream, data_stream))
    average_base_data = np.zeros((3, len(average_base_time)))
    average_base_data_std = np.zeros((3, len(average_base_time)))

    # Call the function
    average_base_data, average_base_data_std = stats.average_to_interval(
        time_stream, average_base_sec, average_base_time,
        data_stream, average_base_data, average_base_data_std
    )
    expected_data = np.array(
            [
                [0.0, 25.252, 85.858, 146.464,
                    207.070, 267.676, 328.282, 388.888,
                    449.494, 510.101, 570.707, 631.313,
                    691.919, 752.525, 813.131, 873.737,
                    934.3434],
                [0.0, 25.252, 85.858, 146.464,
                    207.070, 267.676, 328.282, 388.888,
                    449.494, 510.101, 570.707, 631.313,
                    691.919, 752.525, 813.131, 873.737,
                    934.3434],
                [0.0, 25.252, 85.858, 146.464,
                    207.070, 267.676, 328.282, 388.888,
                    449.494, 510.101, 570.707, 631.313,
                    691.919, 752.525, 813.131, 873.737,
                    934.34343434]
            ]
        )

    expeced_std = np.array(
            [
                [0.0, 17.250, 17.250, 17.250, 17.250,
                    17.250, 17.250, 17.250, 17.250, 17.250,
                    17.250, 17.250, 17.250, 17.250, 17.250,
                    17.250, 17.250],
                [0.0, 17.250, 17.250, 17.250, 17.250,
                    17.250, 17.250, 17.250, 17.250, 17.250,
                    17.250, 17.250, 17.250, 17.250, 17.250,
                    17.250, 17.250],
                [0.0, 17.250, 17.250, 17.250, 17.250,
                    17.250, 17.250, 17.250, 17.250, 17.250,
                    17.250, 17.250, 17.250, 17.250, 17.250,
                    17.250, 17.250]
            ]
        )

    assert np.allclose(average_base_data, expected_data, rtol=1e-3)
    assert np.allclose(average_base_data_std, expeced_std, rtol=1e-3)
