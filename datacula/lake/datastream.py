"""creates the DataStream class"""

import warnings
from typing import Union, Optional, List
import numpy as np
from datacula import convert, stats

# pylint: disable=too-many-instance-attributes
# pylint: disable=no-else-return


class DataStream():
    """A class for calculating time averages of data streams on-the-fly.

    Parameters:
    ----------
        header_list (list of str): A list of column headers for the data.
        average_times (list of float): A list of time intervals (in seconds)
            over which to calculate the data averages.
        average_base (float, optional): The base for the exponential moving
            average window used to smooth the data. Defaults to 2.0.

    Example:
    ----------
        # Create a DataStream object with two headers and two average intervals
        stream = DataStream(['time', 'value'], [10.0, 60.0])

        # Add data to the stream and calculate the rolling averages
        for data_point in my_data_stream:
            stream.add_data_point(data_point)
            rolling_averages = stream.calculate_averages()

    Attributes:
    ----------
        header_list (list of str): (n,) A list of column headers for the data.
        header_dict (dict): A dictionary mapping column headers to column
            indices.
        average_int_sec (list of float): A list of time intervals (in seconds)
            over which to calculate the data averages.
        data_stream (np.ndarray): (n, m) An array of data points.
            Rows match header indexes and columns match time.
        time_stream (np.ndarray): (m,) An array of timestamps corresponding
            to the data points.
        average_base_sec (float): The base for the exponential moving average
            window used to smooth the data.
        average_base_time (np.ndarray): An array of timestamps corresponding to
            the beginning of each average interval.
        average_base_data (np.ndarray): An array of average data values for
            each average interval.
        average_base_data_std (np.ndarray): An array of standard deviations of
            the data values for each average interval.
        average_epoch_start (int or None): The Unix epoch time (in seconds) of
            the start of the first average interval.
        average_epoch_end (int or None): The Unix epoch time (in seconds) of
            the end of the last average interval.
        average_dict (dict): A dictionary mapping average interval lengths to
            corresponding arrays of average data values.
    """

    def __init__(
            self,
            header_list: list,
            average_times: list,
            average_base: float = 2.0
            ) -> None:
        """
    Initialize a DataStream object with the given header list and average
    times.

    Parameters:
    ----------
            header
    list (list of str): A list of column headers for the data.
        Each header must be a string.
    average_times (list of float): A list of time intervals (in seconds)
        over which to calculate the data averages.
        Each time interval must be a positive float.
    average_base (float, optional): The base for the exponential moving
        average window used to smooth the data. Defaults to 2.0.

    Returns:
    ----------
        None

    Raises:
    ----------
        TypeError: If header_list or average_times is not a list.
        ValueError: If header_list or average_times is an empty list,
            or if any element of average_times is not a positive float.

    """
        if not isinstance(header_list, list):
            raise TypeError("header_list must be a list")
        if not isinstance(average_times, list):
            raise TypeError("average_times must be a list")
        if len(header_list) == 0:
            raise ValueError("header_list must not be empty")
        if len(average_times) == 0:
            raise ValueError("average_times must not be empty")
        if not all(isinstance(t, (int, float))
                   and t > 0 for t in average_times):
            raise ValueError("average_times must be a list of positive floats")

        self.header_list = header_list
        self.header_dict = convert.list_to_dict(header_list)
        self.average_int_sec = average_times
        self.data_stream = np.array([])
        self.time_stream = np.array([])
        self.average_base_sec = average_base
        self.average_base_time = np.array([])
        self.average_base_data = np.array([])
        self.average_base_data_std = np.array([])
        self.average_epoch_start = None
        self.average_epoch_end = None
        self.average_dict = {}

    def add_data(
                self,
                time_stream: np.ndarray,
                data_stream: np.ndarray,
                header_check: bool = False,
                header: List[str] = None
            ) -> None:
        """
        Adds a new data stream and corresponding time stream to the
        existing data.

        Parameters
        ----------
        time_stream : np.ndarray (m,)
            An array of time values for the new data stream.
        data_stream : np.ndarray
            An array of data values for the new data stream.
        header_check : bool, optional
            If True, checks whether the header in the new data matches the
            header in the existing data. Defaults to False.
        header : list of str, optional
            A list of header names for the new data stream. Required if
            header_check is True.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If header_check is True and header is not provided or
            header does not match the existing header.

        Notes
        -----
        If self.data_stream is empty, the method initializes self.data_stream
        and self.time_stream with the input data.

        If header_check is True, the method checks whether the header in the
        new data matches the header in the existing data. If they do not match,
        the method attempts to merge the headers and updates the header
        dictionary.

        If header_check is False or the headers match, the new data is
        appended to the existing data.

        The method also checks whether the time stream is increasing, and if
        not, sorts the time stream and corresponding data.
        """

        if self.data_stream.size == 0:
            self.data_stream = data_stream
            self.time_stream = time_stream
        elif header_check:
            data_stream, self.header_list = self.merge_different_headers(
                data_stream,
                header,
            )
            # updates header_dict
            self.header_dict = convert.list_to_dict(self.header_list)
            self.data_stream = np.hstack((self.data_stream, data_stream))
            self.time_stream = np.concatenate((self.time_stream, time_stream))
        else:
            self.data_stream = np.hstack((self.data_stream, data_stream))
            self.time_stream = np.concatenate((self.time_stream, time_stream))

        # check if the time stream added is increasing
        increasing_time = np.all(
            self.time_stream[1:] >= self.time_stream[:-1],
            axis=0
        )

        if not increasing_time:
            # sort the time stream
            sorted_time_index = np.argsort(self.time_stream)
            self.time_stream = self.time_stream[sorted_time_index]
            self.data_stream = self.data_stream[:, sorted_time_index]

    def add_processed_data(
                self,
                data_new: np.array,
                time_new: np.array,
                header_new: list,
            ) -> None:
        """
        Adds processed data to the data stream. Assumes the input matches the
        raw time stream.

        Parameters:
        -----------
        data_new : np.array
            Processed data to add to the data stream.
        time_new : np.array
            Time array for the new data.
        header_new : list
            List of headers for the new data.
        """
        # # Check for duplicate timestamps in new data
        # unique_timestamps, unique_indexes = np.unique(
        #     time_new, return_index=True
        #     )
        # if len(unique_timestamps) != len(time_new):
        #     if data_new.shape[0] != len(time_new):
        #         data_new = data_new[:, unique_indexes]
        #     else:
        #         data_new = data_new[unique_indexes]
        #     time_new = unique_timestamps
        #     warnings.warn(
        #         "Removing duplicate timestamps found in input data."
        #         )

        self.header_list = np.append(self.header_list, header_new)
        # add other rows to the data array
        self.data_stream = np.concatenate(
            (
                self.data_stream,
                np.full(
                    (len(header_new), self.data_stream.shape[1]),
                    np.nan,
                ),
            ),
            axis=0,
        )

        data_fit = (len(data_new.shape) == 1) or \
            (data_new.shape[1] == self.data_stream.shape[1])

        # check the length of the data
        if data_fit:
            # add the data
            self.data_stream[-len(header_new):, :] = data_new
        else:
            # self.data_stream[-1, :] = np.interp(
            #     self.time_stream,
            #     time_new,
            #     data_new,
            # ) if len(data_new.shape) == 1 else np.apply_along_axis(
            #     lambda x: np.interp(self.time_stream, time_new, x),
            #     axis=1,
            #     arr=data_new,
            # )
            if len(data_new.shape) == 1:
                self.data_stream[-1, :] = np.interp(
                    self.time_stream,
                    time_new,
                    data_new,
                )
            else:
                # interpolate the data
                for i in range(len(header_new)):
                    self.data_stream[-len(header_new)+i, :] = np.interp(
                        self.time_stream,
                        time_new,
                        data_new[i, :],
                    )

        self.header_dict = convert.list_to_dict(self.header_list)
        self.reaverage()

    def merge_different_headers(
            self,
            data_new: np.array,
            header_new: list
            ) -> Union[np.array, list]:
        """
        Formats the data stream and header_list as needed to merge the new data
        with the existing data. New headers are added to the end of the header
        list and data is filled with NaN. Numerical headers are sorted in
        ascending order.

        Args:
            data_new (np.ndarray): The new data array to be merged.
            header_new (list): The new header list to be merged.

        Returns:
            Union[np.ndarray, list]: A tuple containing the formated data array
            and the formated header.
        """
        self.data_stream, self.header_list, data_new, header_new = \
            stats.merge_formating(
                data_current=self.data_stream,
                header_current=self.header_list,
                data_new=data_new,
                header_new=header_new
            )
        return data_new, header_new

    def check_average_data(
            self,
            ) -> None:
        """
        Checks if the average data is present. If not, it creates it.
        Also if there is not enough average time to cover the next averaging,
        this adds another hour to the average time.

        TODO: should be split into add_average_data, check_average_data, and
        extend_average_data
        """
        base_rounding_interval_sec = self.average_base_sec
        if self.average_base_time.size == 0:  # average base initialisation
            if self.average_epoch_start is None:
                average_time_start = int(
                    convert.round_arbitrary(
                        self.time_stream[0],
                        base=base_rounding_interval_sec,
                        mode='floor'
                    )
                )
                average_time_end = int(
                    convert.round_arbitrary(
                        self.time_stream[-1],
                        base=base_rounding_interval_sec,
                        mode='ceil'
                    )
                )
            else:
                average_time_start = self.average_epoch_start
                average_time_end = self.average_epoch_end

            self.average_base_time = np.arange(
                    average_time_start,
                    average_time_end,
                    self.average_base_sec
                )
            self.average_base_data = np.zeros((
                    len(self.header_list),
                    len(self.average_base_time)
                    ))*np.nan
            self.average_base_data_std = np.zeros((
                    len(self.header_list),
                    len(self.average_base_time)
                    ))*np.nan

        # check if the average time is long enough to cover the next averaging,
        # if not, add another base_rounding_interval_sec to the average time
        if (self.average_base_time[-1] <= self.time_stream[-1]
                and self.average_epoch_start is None):

            average_time_end = int(convert.round_arbitrary(
                self.time_stream[-1],
                base=base_rounding_interval_sec, mode='ceil'
                ))
            base_time_to_add = np.arange(
                self.average_base_time[-1],
                average_time_end, self.average_base_sec
                )
            self.average_base_time = np.append(
                    self.average_base_time,
                    base_time_to_add
                )
            self.average_base_data = np.append(
                    self.average_base_data,
                    np.zeros((
                        len(self.header_list),
                        len(base_time_to_add)
                    ))*np.nan,
                    axis=1
                )  # +1 for the last element
            self.average_base_data_std = np.append(
                    self.average_base_data_std,
                    np.zeros((
                        len(self.header_list),
                        len(base_time_to_add)
                    ))*np.nan,
                    axis=1
                )  # +1 for the last element

    def average_to_base_interval(self) -> np.ndarray:
        """
        Calculate the average of the data stream over specified time intervals.

        This method calculates the average of the data stream over a series of
        time intervals specified by `self.average_base_time`. The average and
        standard deviation of the data are calculated for each interval,
        and the results are stored in `self.average_base_data` and
        `self.average_base_data_std`, respectively.

        Returns:
        -------
            None: It updates the `self.average_base_data` and
            `self.average_base_data_std` attributes.
        """
        # Check that the data is sufficient to cover the averaging period
        self.check_average_data()

        # Call the stats.average_to_interval function to calculate the averages
        self.average_base_data, self.average_base_data_std = \
            stats.average_to_interval(
                time_stream=self.time_stream,
                average_base_sec=self.average_base_sec,
                average_base_time=self.average_base_time,
                data_stream=self.data_stream,
                average_base_data=self.average_base_data,
                average_base_data_std=self.average_base_data_std,
            )

    def reaverage(
                self,
                reaverage_base_sec: Optional[float] = None,
                epoch_start: Optional[float] = None,
                epoch_end: Optional[float] = None
            ) -> None:
        """
        Re-calculate the average of the data stream with a new base time.

        This method re-calculates the average of the data stream using a new
        base time interval specified by `reaverage_base_sec`. If `epoch_start`
        and `epoch_end` are specified, only data within the specified time
        range will be included in the averaging calculation.

        Parameters:
            reaverage_base_sec (float, optional): The length of the new time
                interval for calculating the average. If None, the current
                value of `self.average_base_sec` will be used.
            epoch_start (float, optional): The start time of the epoch to use
                for calculating the average. If None, the current value of
                `self.average_epoch_start` will be used.
            epoch_end (float, optional): The end time of the epoch to use for
                calculating the average. If None, the current value of
                `self.average_epoch_end` will be used.

        Returns:
        -------
            None: It updates the `self.average_base_data` and
            `self.average_base_data_std` attributes.
        """
        # Clear the current average data

        self.average_base_time = np.array([])
        self.average_base_data = np.array([])
        self.average_base_data_std = np.array([])

        # Set the new averaging parameters if they are specified
        if reaverage_base_sec is not None:
            self.average_base_sec = reaverage_base_sec
        if epoch_start is not None:
            self.average_epoch_start = epoch_start
        if epoch_end is not None:
            self.average_epoch_end = epoch_end

        # Calculate the new average data
        self.average_to_base_interval()

    def average_data(self) -> None:
        """
        Wraper to average the data to the base time.
        """
        self.average_to_base_interval()

    def return_data(
                self,
                keys: Optional[Union[str, list]] = None,
                raw: bool = False
            ) -> np.ndarray:
        """
        Returns the data stream, averaged to the base time interval.

        This method returns the data stream, averaged to the base time interval
        specified by `self.average_base_sec` and `self.average_base_time`. If
        `keys` is not None, only the data corresponding to the specified
        channels will be returned. If `raw` is True, the raw data stream will
        be returned instead of the averaged data.

        Parameters:
        ----------
            keys (str or list, optional): A channel name or list of channel
                names specifying the channels to include in the returned data.
                If None, all channels will be included. Default is None.
            raw (bool, optional): If True, the raw data stream will be returned
                instead of the averaged data. Default is False.

        Returns:
        --------
            np.ndarray: The data stream, averaged to the base time interval and
                optionally filtered by channel and/or returned as raw data.

        Example usage:
        --------------
            # Return all data averaged to the base time interval
            data = analyzer.return_data()

            # Return only the data for channels 'ch1' and 'ch2'
            data = analyzer.return_data(keys=['ch1', 'ch2'])

            # Return the raw data for all channels
            raw_data = analyzer.return_data(raw=True)
        """
        # Calculate the average data if it hasn't been done already
        if not raw and self.average_base_data.size == 0:
            self.average_to_base_interval()

        # Filter the data by channel if keys is not None
        if keys is not None:
            key_values = convert.get_values_in_dict(keys, self.header_dict)
        else:
            key_values = slice(None)  # may not work?

        # Return either the raw data or the averaged data
        if raw:
            return self.data_stream[key_values, :]
        else:
            return self.average_base_data[key_values, :]

    def return_std(
                self,
                keys: Optional[Union[str, list]] = None,
                raw: bool = False
            ) -> np.ndarray:
        """
        Returns the standard deviation of the data stream, on the base time
        interval.

        This method returns the standard deviation of the data stream,
        averaged to the base time interval specified by `self.average_base_sec`
        and `self.average_base_time`. If `keys` is not None, only the standard
        deviation of the data corresponding to the specified channels will be
        returned. If `raw` is True, the raw data stream will be returned
        instead of the averaged standard deviation.

        Parameters:
        ----------
            keys (str or list, optional): A channel name or list of channel
                names specifying the channels to include in the returned data.
                If one, all channels will be included. Default is None.
            raw (bool, optional): If True, the raw data stream will be
                returned instead of the averaged standard deviation.
                Default is False.

        Returns:
        -------
            np.ndarray: The standard deviation of the data stream, averaged
                to the base time interval and optionally filtered by channel
                and/or returned as raw data.

        Example usage:
        -------------
            # Return the standard deviation of all data averaged to the
                base time interval
            std_data = datastream.return_std()

            # Return only the standard deviation for channels 'ch1' and 'ch2'
            std_data = datastream.return_std(keys=['ch1', 'ch2'])

            # Return the raw data for all channels
            raw_data = datastream.return_std(raw=True)
        """
        # Calculate the average data if it hasn't been done already
        if not raw and self.average_base_data_std.size == 0:
            self.average_to_base_interval()

        # Filter the data by channel if keys is not None
        if keys is not None:
            key_values = convert.get_values_in_dict(keys, self.header_dict)
        else:
            key_values = slice(None)

        # Return either the raw data or the averaged standard deviation
        if raw:
            return self.data_stream[key_values, :]
        else:
            return self.average_base_data_std[key_values, :]

    def return_time(
                self,
                datetime64: bool = False,
                raw: bool = False
            ) -> np.ndarray:
        """
        Returns the time stream, averaged to the base time interval.

        This method returns the time stream, averaged to the base time
        interval specified by `self.average_base_sec` and
        `self.average_base_time`. If `datetime64` is True, the time stream is
        returned as a numpy array of datetime64 objects, otherwise it
        is returned as a numpy array of epoch times in seconds.
        If `raw` is True, the raw time stream will be returned instead of
        the averaged time stream.

        Parameters:
        ----------
            datetime64 (bool, optional): If True, the time stream is returned
                as a numpy array of datetime64 objects, otherwise it is
                returned as a numpy array of epoch times in seconds.
                Default is False.
            raw (bool, optional): If True, the raw time stream will be
                returned instead of the averaged time stream.
                Default is False.

        Returns:
        -------
            np.ndarray: The time stream, averaged to the base time interval
                and optionally converted to datetime64 objects and/or
                returned as raw data.

        Example usage:
            # Return the averaged time stream as epoch times
            time_data = datastream.return_time()

            # Return the raw time stream as datetime64 objects
            time_data = datastream.return_time(datetime64=True, raw=True)

            # Return the averaged time stream for the first 10 channels
            time_data = datastream.return_time(raw=False)[:10]
        """
        # Calculate the average data if it hasn't been done already
        if not raw and self.average_base_time.size == 0:
            self.average_to_base_interval()

        # Return either the raw data or the averaged data
        if raw:
            if datetime64:

                raise NotImplementedError  # not working implement this

                # return convert.datetime64_from_epoch_array(
                #     self.time_stream)
            return self.time_stream
        else:
            if datetime64:
                return convert.datetime64_from_epoch_array(
                    self.average_base_time)
            return self.average_base_time

    def return_header_list(self) -> list:
        """Returns the header list. (wrapper)"""
        return self.header_list

    def return_header_dict(self) -> dict:
        """Returns the header dictionary. (wrapper)"""
        return self.header_dict
