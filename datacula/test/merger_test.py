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

    # check for a 3 time point data set with a nan value and 
    # a full row of nan values
    data_new = np.array([[np.nan, 7, np.nan], [6, 7, np.nan], [6, np.nan, np.nan]])
    time_new = np.array([1, 2, 3])
    header_new = ['header3', 'header4', 'header5']
    merged_data, merged_header_list, _ = merger.add_processed_data(
        data, time, header_list, data_new, time_new, header_new)
    
    # Check that the merged data has the correct shape and values
    # all nan values should be replaced except for full rows of nan values
    expected_data = np.array([
        [1, 2, 6, 7, np.nan], [3, 4, 6, 7, np.nan], [5, 6, 6, 7, np.nan],
        [7, 8, 6, 7, np.nan], [9, 10, 6, 7, np.nan]
        ])
    assert np.array_equal(np.nan_to_num(merged_data), np.nan_to_num(expected_data))

    # Check that the merged header list has the correct values
    expected_header_list = ['header1', 'header2', 'header3', 'header4', 'header5']
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
