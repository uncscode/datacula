"""
Merge or adds processed data to the data stream. Accounts for data shape mis
matches and duplicate timestamps. If the data is a different shape than the
data stream, it will interpolate the data to the data stream's time array.
If the data has duplicate timestamps, it will remove the duplicates and
interpolate the data to the data stream's time array.
"""
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file


import numpy as np
from datacula import convert

def add_processed_data(
        data: np.array,
        time: np.array,
        header_list: list,
        data_new: np.array,
        time_new: np.array,
        header_new: list,
    ) ->[np.array, np.array, list, dict]:
    """
    Merge or adds processed data to the data stream. Accounts for data shape mis
    matches and duplicate timestamps. If the data is a different shape than the

    Parameters:
    -----------
    data_new : np.array
        Processed data to add to the data stream.
    time_new : np.array
        Time array for the new data.
    header_new : list
        List of headers for the new data.
    """

    # check if the data_new is 2d or 1d
    if len(data_new.shape) == 2:
        if len(time_new) == data_new.shape[0] and len(time_new) == data_new.shape[1]:
            concatanate_axis_new = -1
        else:
            concatanate_axis_new = np.argwhere(
                np.array(data_new.shape) != len(time_new)).flatten()[0]
        # resphae new data to concatanate axis is the last axis
        data_new = np.moveaxis(data_new, concatanate_axis_new, -1)
    else:
        # resphae new data to concatanate axis is the last axis
        data_new = np.expand_dims(data_new, -1)


    # check if the time arrays are the same sze and values are the same
    if np.array_equal(time, time_new):
        # no need to interpolate the data_new before adding
        # it to the data
        header_updated = np.append(header_list, header_new)
        data_updated = np.concatenate(
            (
                data,
                data_new,
            ),
            axis=-1,
        )
    else: # interpolate the data_new before adding it to the data_stream
        # interpolate the data_new to the data_stream time array
        data_interp = np.apply_along_axis(
            lambda x: np.interp(time, time_new, x),
                 axis=1,
                 arr=data_new,
             )
        # update the data array
        data_updated = np.concatenate(
            (
                data,
                data_interp,
            ),
            axis=-1,
        )

    header_updated = np.append(header_list, header_new)
    header_dict = convert.list_to_dict(header_updated)

    return data_updated, header_updated, header_dict
