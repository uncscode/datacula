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
import warnings
from typing import List, Tuple, Dict
from datacula import convert

def add_processed_data(
        data: np.array,
        time: np.array,
        header_list: List[str],
        data_new: np.array,
        time_new: np.array,
        header_new: List[str],
    ) -> Tuple[np.array, List[str], Dict[str, int]]:
    """
    Merge or adds processed data to the data stream. Accounts for data shape
    miss matches and duplicate timestamps. If the data is a different shape than
    the existing data, it will be reshaped to match the existing data.

    Parameters:
    -----------
    data : np.array
        Existing data stream.
    time : np.array
        Time array for the existing data.
    header_list : List[str]
        List of headers for the existing data.
    data_new : np.array
        Processed data to add to the data stream.
    time_new : np.array
        Time array for the new data.
    header_new : List[str]
        List of headers for the new data.

    Returns:
    --------
    Tuple[np.array, List[str], Dict[str, int]]
        A tuple containing the updated data stream, the updated header list, and
        a dictionary mapping the header names to their corresponding indices in
        the data stream.
    """

    # Check if data_new is 2D or 1D
    if len(data_new.shape) == 2:
        # Check if time_new matches the dimensions of data_new
        if len(time_new) == data_new.shape[0] and \
            len(time_new) == data_new.shape[1]:
            concatenate_axis_new = 0  # Default to the first axis
            # Check if the last axis of data matches the length of time
            if data.shape[-1] != len(time):
                if data.shape[0] == len(time):
                    warnings.warn("Square data with time shape assumes time \
                                  axis is the first axis in data.")
                else:
                    warnings.warn("Inconsistent shapes between data and time.")
        else:
            # Find the axis that doesn't match the length of time_new
            concatenate_axis_new = np.argwhere(
                np.array(data_new.shape) != len(time_new)).flatten()[0]
        
        # Reshape new data so the concatenate axis is the first axis
        data_new = np.moveaxis(data_new, concatenate_axis_new, 0)

        # check header list length matches data_new shape
        if len(header_new) != data_new.shape[0]:
            print(f'header_new len: {len(header_new)} vs. data_new.shape: \
                  {data_new.shape}')
            print(header_new)
            raise ValueError("Header list length must match the first \
                              dimension of data_new.")
    else:
        # check if header is a single entry
        if len(header_new) != 1:
            raise ValueError("Header list must be a single entry if data_new \
                              is 1D.")
        # Reshape new data so the concatenate axis is the first axis
        data_new = np.expand_dims(data_new, 0)


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
            axis=0,
        )
    else: # interpolate the data_new before adding it to the data_stream
        data_interp = np.empty((data_new.shape[0], len(time)))
        for i in range(data_new.shape[0]):
            mask = ~np.isnan(data_new[i, :])
            if not mask.any():
                data_interp[i, :] = np.nan
            else:
                left_value = data_new[i, mask][0]
                right_value = data_new[i, mask][-1]
                data_interp[i, :] = np.interp(
                    time, 
                    time_new[mask],
                    data_new[i, mask],
                    left=left_value,
                    right=right_value,
                )
        # update the data array
        data_updated = np.concatenate(
            (
                data,
                data_interp,
            ),
            axis=0,
        )

    header_updated = np.append(header_list, header_new)
    header_dict = convert.list_to_dict(header_updated)

    return data_updated, header_updated, header_dict
