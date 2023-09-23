"""Test the loader module."""

import numpy as np
from datacula import merger


def test_add_processed_data_different_time():
    # Create some sample data
    data = np.array([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]])
    time = np.array([0, 1, 2, 3, 4])
    header_list = ['header1', 'header2']
    data_new = np.array([[7, 8], [7, 8]])
    time_new = np.array([1, 4])
    header_new = ['header3', 'header4']

    # Call the function with the sample data
    merged_data, merged_header_list, _ = merger.add_processed_data(
        data, time, header_list, data_new, time_new, header_new)

    # Check that the merged data has the correct shape and values
    expected_data = np.array([
        [1, 2, 7, 8], [3, 4, 7, 8], [5, 6, 7, 8], [7, 8, 7, 8], [9, 10, 7, 8]
        ])
    assert np.array_equal(merged_data, expected_data)

    # Check that the merged header list has the correct values
    expected_header_list = ['header1', 'header2', 'header3', 'header4']
    assert np.all(merged_header_list == expected_header_list)

    # call the function with the sample data using only one data column
    data_new = np.array([7, 7])
    header_new = 'header3'
    merged_data, merged_header_list, _ = merger.add_processed_data(
        data, time, header_list, data_new, time_new, header_new)
    
    # Check that the merged data has the correct shape and values
    expected_data = np.array([
        [1, 2, 7], [3, 4, 7], [5, 6, 7], [7, 8, 7], [9, 10, 7]
        ])
    assert np.array_equal(merged_data, expected_data)

    # Check that the merged header list has the correct values
    expected_header_list = ['header1', 'header2', 'header3']
    assert np.all(merged_header_list == expected_header_list)


def test_add_processed_data_same_time():
    # Create some sample data
    data = np.array([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]])
    time = np.array([0, 1, 2, 3, 4])
    header_list = ['header1', 'header2']
    data_new = np.array([[7, 8], [7, 8], [7, 8], [7, 8], [7, 8]]).transpose()
    time_new = np.array([0, 1, 2, 3, 4])
    header_new = ['header3', 'header4']

    # Call the function with the sample data
    merged_data, merged_header_list, _ = merger.add_processed_data(
        data, time, header_list, data_new, time_new, header_new)

    # Check that the merged data has the correct shape and values
    expected_data = np.array([
        [1, 2, 7, 8], [3, 4, 7, 8], [5, 6, 7, 8], [7, 8, 7, 8], [9, 10, 7, 8]
        ])
    # print(merged_data)
    assert np.array_equal(merged_data, expected_data)

    # Check that the merged header list has the correct values
    expected_header_list = ['header1', 'header2', 'header3', 'header4']
    # print(merged_header_list)
    assert np.all(merged_header_list == expected_header_list)

    # call the function with the sample data using only one data column
    merged_data, merged_header_list, _ = merger.add_processed_data(
        data, time, header_list, data_new[0, :], time_new, header_new[0])

    # Check that the merged data has the correct shape and values
    expected_data = np.array([
        [1, 2, 7], [3, 4, 7], [5, 6, 7], [7, 8, 7], [9, 10, 7]
        ])

    assert np.array_equal(merged_data, expected_data)

    # Check that the merged header list has the correct values
    expected_header_list = ['header1', 'header2', 'header3']
    assert np.all(merged_header_list == expected_header_list)
