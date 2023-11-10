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

    data_new = convert.data_shape_check(
        time=time_new,
        data=data_new,
        header=header_new)

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
