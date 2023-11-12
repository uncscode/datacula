"""Processes that can act on a stream of data."""

from typing import List, Union
import numpy as np
from datacula import convert, stats, merger




# def add_processed_data(
#             stream,
#             data_new: np.array,
#             time_new: np.array,
#             header_new: list,
#         ) -> object:
#     """
#     Adds processed data to the data stream. This data has the same time array
#     as the existing data, but we are adding additional data and headers.
#     This is using merger.add_processed_data to merge the new data with the 
#     existing data.

#     Parameters:
#     -----------
#     data_new : np.array
#         Processed data to add to the data stream.
#     time_new : np.array
#         Time array for the new data.
#     header_new : list
#         List of headers for the new data.
#     """
#     stream.data, stream.header, stream.header = \
#         merger.add_processed_data(
#             data=stream.data_stream,
#             time=stream.time_stream,
#             header_list=stream.header_list,
#             data_new=data_new,
#             time_new=time_new,
#             header_new=header_new,
#         )
#     return stream


# def check_average_data(
#         stream,
#         ) -> None:
#     """
#     Checks if the average data is present. If not, it creates it.
#     Also if there is not enough average time to cover the next averaging,
#     this adds another hour to the average time.

#     do: should be split into add_average_data, check_average_data, and
#     extend_average_data
#     """
#     base_rounding_interval_sec = stream.average_base_sec
#     if stream.average_base_time.size == 0:  # average base initialisation
#         if stream.average_epoch_start is None:
#             average_time_start = int(
#                 convert.round_arbitrary(
#                     stream.time_stream[0],
#                     base=base_rounding_interval_sec,
#                     mode='floor'
#                 )
#             )
#             average_time_end = int(
#                 convert.round_arbitrary(
#                     stream.time_stream[-1],
#                     base=base_rounding_interval_sec,
#                     mode='ceil'
#                 )
#             )
#         else:
#             average_time_start = stream.average_epoch_start
#             average_time_end = stream.average_epoch_end

#         stream.average_base_time = np.arange(
#                 average_time_start,
#                 average_time_end,
#                 stream.average_base_sec
#             )
#         stream.average_base_data = np.zeros((
#                 len(stream.header_list),
#                 len(stream.average_base_time)
#                 ))*np.nan
#         stream.average_base_data_std = np.zeros((
#                 len(stream.header_list),
#                 len(stream.average_base_time)
#                 ))*np.nan

#     # check if the average time is long enough to cover the next averaging,
#     # if not, add another base_rounding_interval_sec to the average time
#     if (stream.average_base_time[-1] <= stream.time_stream[-1]
#             and stream.average_epoch_start is None):

#         average_time_end = int(convert.round_arbitrary(
#             stream.time_stream[-1],
#             base=base_rounding_interval_sec, mode='ceil'
#             ))
#         base_time_to_add = np.arange(
#             stream.average_base_time[-1],
#             average_time_end, stream.average_base_sec
#             )
#         stream.average_base_time = np.append(
#                 stream.average_base_time,
#                 base_time_to_add
#             )
#         stream.average_base_data = np.append(
#                 stream.average_base_data,
#                 np.zeros((
#                     len(stream.header_list),
#                     len(base_time_to_add)
#                 ))*np.nan,
#                 axis=1
#             )  # +1 for the last element
#         stream.average_base_data_std = np.append(
#                 stream.average_base_data_std,
#                 np.zeros((
#                     len(stream.header_list),
#                     len(base_time_to_add)
#                 ))*np.nan,
#                 axis=1
#             )  # +1 for the last element

# def average_to_base_interval(stream) -> np.ndarray:
#     """
#     Calculate the average of the data stream over specified time intervals.

#     This method calculates the average of the data stream over a series of
#     time intervals specified by `stream.average_base_time`. The average and
#     standard deviation of the data are calculated for each interval,
#     and the results are stored in `stream.average_base_data` and
#     `stream.average_base_data_std`, respectively.

#     Returns:
#     -------
#         None: It updates the `stream.average_base_data` and
#         `stream.average_base_data_std` attributes.
#     """
#     # Check that the data is sufficient to cover the averaging period
#     stream.check_average_data()

#     # Call the stats.average_to_interval function to calculate the averages
#     stream.average_base_data, stream.average_base_data_std = \
#         stats.average_to_interval(
#             time_stream=stream.time_stream,
#             average_base_sec=stream.average_base_sec,
#             average_base_time=stream.average_base_time,
#             data_stream=stream.data_stream,
#             average_base_data=stream.average_base_data,
#             average_base_data_std=stream.average_base_data_std,
#         )

# def reaverage(
#             stream,
#             reaverage_base_sec: Optional[float] = None,
#             epoch_start: Optional[float] = None,
#             epoch_end: Optional[float] = None
#         ) -> None:
#     """
#     Re-calculate the average of the data stream with a new base time.

#     This method re-calculates the average of the data stream using a new
#     base time interval specified by `reaverage_base_sec`. If `epoch_start`
#     and `epoch_end` are specified, only data within the specified time
#     range will be included in the averaging calculation.

#     Parameters:
#         reaverage_base_sec (float, optional): The length of the new time
#             interval for calculating the average. If None, the current
#             value of `stream.average_base_sec` will be used.
#         epoch_start (float, optional): The start time of the epoch to use
#             for calculating the average. If None, the current value of
#             `stream.average_epoch_start` will be used.
#         epoch_end (float, optional): The end time of the epoch to use for
#             calculating the average. If None, the current value of
#             `stream.average_epoch_end` will be used.

#     Returns:
#     -------
#         None: It updates the `stream.average_base_data` and
#         `stream.average_base_data_std` attributes.
#     """
#     # Clear the current average data

#     stream.average_base_time = np.array([])
#     stream.average_base_data = np.array([])
#     stream.average_base_data_std = np.array([])

#     # Set the new averaging parameters if they are specified
#     if reaverage_base_sec is not None:
#         stream.average_base_sec = reaverage_base_sec
#     if epoch_start is not None:
#         stream.average_epoch_start = epoch_start
#     if epoch_end is not None:
#         stream.average_epoch_end = epoch_end

#     # Calculate the new average data
#     stream.average_to_base_interval()

# def average_data(stream) -> None:
#     """
#     Wraper to average the data to the base time.
#     """
#     stream.average_to_base_interval()


# def return_data(
#             self,
#             keys: Optional[Union[str, list]] = None,
#             raw: bool = False
#         ) -> np.ndarray:
#     """
#     Returns the data stream, averaged to the base time interval.

#     This method returns the data stream, averaged to the base time interval
#     specified by `self.average_base_sec` and `self.average_base_time`. If
#     `keys` is not None, only the data corresponding to the specified
#     channels will be returned. If `raw` is True, the raw data stream will
#     be returned instead of the averaged data.

#     Parameters:
#     ----------
#         keys (str or list, optional): A channel name or list of channel
#             names specifying the channels to include in the returned data.
#             If None, all channels will be included. Default is None.
#         raw (bool, optional): If True, the raw data stream will be returned
#             instead of the averaged data. Default is False.

#     Returns:
#     --------
#         np.ndarray: The data stream, averaged to the base time interval and
#             optionally filtered by channel and/or returned as raw data.

#     Example usage:
#     --------------
#         # Return all data averaged to the base time interval
#         data = analyzer.return_data()

#         # Return only the data for channels 'ch1' and 'ch2'
#         data = analyzer.return_data(keys=['ch1', 'ch2'])

#         # Return the raw data for all channels
#         raw_data = analyzer.return_data(raw=True)
#     """
#     # Calculate the average data if it hasn't been done already
#     if not raw and self.average_base_data.size == 0:
#         self.average_to_base_interval()

#     # Filter the data by channel if keys is not None
#     if keys is not None:
#         key_values = convert.get_values_in_dict(keys, self.header_dict)
#     else:
#         key_values = slice(None)  # may not work?

#     # Return either the raw data or the averaged data
#     if raw:
#         return self.data_stream[key_values, :]
#     else:
#         return self.average_base_data[key_values, :]

# def return_std(
#             self,
#             keys: Optional[Union[str, list]] = None,
#             raw: bool = False
#         ) -> np.ndarray:
#     """
#     Returns the standard deviation of the data stream, on the base time
#     interval.

#     This method returns the standard deviation of the data stream,
#     averaged to the base time interval specified by `self.average_base_sec`
#     and `self.average_base_time`. If `keys` is not None, only the standard
#     deviation of the data corresponding to the specified channels will be
#     returned. If `raw` is True, the raw data stream will be returned
#     instead of the averaged standard deviation.

#     Parameters:
#     ----------
#         keys (str or list, optional): A channel name or list of channel
#             names specifying the channels to include in the returned data.
#             If one, all channels will be included. Default is None.
#         raw (bool, optional): If True, the raw data stream will be
#             returned instead of the averaged standard deviation.
#             Default is False.

#     Returns:
#     -------
#         np.ndarray: The standard deviation of the data stream, averaged
#             to the base time interval and optionally filtered by channel
#             and/or returned as raw data.

#     Example usage:
#     -------------
#         # Return the standard deviation of all data averaged to the
#             base time interval
#         std_data = datastream.return_std()

#         # Return only the standard deviation for channels 'ch1' and 'ch2'
#         std_data = datastream.return_std(keys=['ch1', 'ch2'])

#         # Return the raw data for all channels
#         raw_data = datastream.return_std(raw=True)
#     """
#     # Calculate the average data if it hasn't been done already
#     if not raw and self.average_base_data_std.size == 0:
#         self.average_to_base_interval()

#     # Filter the data by channel if keys is not None
#     if keys is not None:
#         key_values = convert.get_values_in_dict(keys, self.header_dict)
#     else:
#         key_values = slice(None)

#     # Return either the raw data or the averaged standard deviation
#     if raw:
#         return self.data_stream[key_values, :]
#     else:
#         return self.average_base_data_std[key_values, :]

