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

import warnings
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

    # Check if data_new is 2D or 1D
    if len(data_new.shape) == 2:
        # Check if time_new matches the dimensions of data_new
        if len(time_new) == data_new.shape[0] and \
            len(time_new) == data_new.shape[1]:
            concatenate_axis_new = -1  # Default to the last axis
            # Check if the last axis of data matches the length of time
            if data.shape[-1] != len(time):
                if data.shape[0] == len(time):
                    warnings.warn("Square data with time shape assumes time \
                                  axis is the last axis in data.")
                else:
                    warnings.warn("Inconsistent shapes between data and time.")
        else:
            # Find the axis that doesn't match the length of time_new
            concatenate_axis_new = np.argwhere(np.array(data_new.shape) != len(time_new)).flatten()[0]
        
        # Reshape new data so the concatenate axis is the last axis
        data_new = np.moveaxis(data_new, concatenate_axis_new, -1)
    else:
        # Reshape new data so the concatenate axis is the last axis
        data_new = np.expand_dims(data_new, -1)

    # Check if time_new matches the dimensions of data_new
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
        data_interp = np.empty((len(time), data_new.shape[1]))
        for i in range(data_new.shape[1]):
            mask = ~np.isnan(data_new[:, i])
            if not mask.any():
                data_interp[:, i] = np.nan
            else:
                left_value = data_new[mask, i][0]
                right_value = data_new[mask, i][-1]
                data_interp[:, i] = np.interp(
                    time, 
                    time_new[mask],
                    data_new[mask, i],
                    left=left_value,
                    right=right_value,
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
