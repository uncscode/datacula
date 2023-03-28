"""creates the datastream and datalake class"""
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

# %%

import glob, time, json, os, pickle
import numpy as np
import pandas as pd
from datetime import datetime
from datacula import convert, loader


#%%
def drop_zeros(data_stream_object: object, zero_keys: list) -> object:
    """Drop rows where zero keys not zero, and return data stream

    Parameters
    ----------
    data_stream : object
        data stream object
    zero_keys : list
        list of keys to check for zeros

    Returns
    -------
    object
        data stream object
    """
    # get zero keys
    zeros = np.sum(
            data_stream_object.return_data(
                keys=zero_keys,
                raw=True
            ),
            axis=0
        ) == 0
    data_stream_object.data_stream = data_stream_object.data_stream[:, zeros]
    data_stream_object.time_stream = data_stream_object.time_stream[zeros]
    data_stream_object.re_average_data()
    return data_stream_object


class DataStream():
    """ The class to calculate time averages of data streams, as the data is
    read in.

    Parameters:
        header_list (list): list of header names.
        average_times (list): list of times to average data over in seconds.
    """

    def __init__(
            self,
            header_list: list,
            average_times: list,
            average_base: float = 2.0
            ) -> None:
        """ Constructiong the class
        """
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
            time_stream: np.array,
            data_stream: np.array,
            header_check: bool = False,
            header: list = None
            ) -> None:
        """
        Inputs the data stream and time stream.
        Parameters:
            time_stream (np.array): time stream (n,).
            data_stream (np.array): data stream (n, m).
            header_check (bool): if True, checks if the header is the same as
                the header_list.
            header (list): list of header names.
        """
        if self.data_stream.size == 0:
            self.data_stream = data_stream
            self.time_stream = time_stream
        elif header_check:
            data_stream, self.header_list = self.merge_different_headers(
                data_stream,
                header,
                )
            self.header_dict = convert.list_to_dict(self.header_list) # updates header_dict
            self.data_stream = np.append(self.data_stream, data_stream, axis=1)
            self.time_stream = np.append(self.time_stream, time_stream, axis=0)
        else:
            self.data_stream = np.append(self.data_stream, data_stream, axis=1)
            self.time_stream = np.append(self.time_stream, time_stream, axis=0)

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
        Adds processed data to the data stream. assumes the input matches the
        raw time stream. If not it will fill the data with nan.
        """
        self.header_list = np.append(
                    self.header_list,
                    header_new
                )
        # add other rows to the data array
        self.data_stream = np.concatenate(
                (
                    self.data_stream,
                    np.full((
                        len(header_new),
                        self.data_stream.shape[1]
                        ), np.nan)
                ),
                axis=0
            )
        
        if len(data_new.shape) == 1:
            if data_new.shape[0] == self.data_stream.shape[1]:
                data_fit = True
            else:
                data_fit = False
        elif data_new.shape[1] == self.data_stream.shape[1]:
            data_fit = True
        else:
            data_fit = False

        # check the length of the data
        if data_fit:
            # add the data
            self.data_stream[-len(header_new):, :] = data_new
        else:
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
        self.header_dict = list_to_dict(self.header_list)
        self.re_average_data()

    def merge_different_headers(
            self,
            data_new: np.array,
            header_new: list
            ) -> (np.array, list):
        """
        Merges the data stream and time stream into one array with the header
        as the first row.
        """
        # elements in header_new that are not in self.header_list
        header_new_not_listed = [
                x for x in header_new if x not in self.header_list
            ]
        header_list_not_new = [
                x for x in self.header_list if x not in header_new
            ]

        # expand the data array to include the new columns if there are any
        if bool(header_new_not_listed):
            self.header_list = np.append(
                    self.header_list,
                    header_new_not_listed
                )
            # add other rows to the data array
            self.data_stream = np.concatenate(
                    (
                        self.data_stream,
                        np.full((
                            len(header_new_not_listed),
                            self.data_stream.shape[1]
                            ), np.nan)
                    ),
                    axis=0
                )

        if bool(header_list_not_new):
            header_new = np.append(
                        header_new,
                        header_list_not_new
                    )
            data_new = np.concatenate(
                    (
                        data_new,
                        np.full((
                            len(header_list_not_new),
                            data_new.shape[1]
                            ), np.nan)
                    ),
                    axis=0
                )

        # check that the shapes are the same
        if self.data_stream.shape[0] != data_new.shape[0]:
            raise ValueError(
                    'self.data_stream  ',
                    self.data_stream .shape,
                    ' and data_new ',
                    data_new.shape,
                    ' are not the same shape, check the data formatting'
                )
        if self.header_list.shape != header_new.shape:
            raise ValueError(
                    'self.header_list ',
                    self.header_list.shape,
                    ' and header_new ',
                    header_new.shape,
                    ' are not the same shape, check the data formatting'
                )

        # check if all the headers are numbers by checking the first element
        list_of_numbers = np.all([x[0].isnumeric() for x in self.header_list])

        if list_of_numbers: # make it a sorted list of numbers
            # convert the headers to floats
            header_list_numeric = np.array(self.header_list).astype(float)
            header_new_numeric = np.array(header_new).astype(float)
            # find the indices of the headers
            header_stored_indices = np.argsort(header_list_numeric)
            header_new_indices = np.argsort(header_new_numeric)
            # sort the header and keep it a list
            self.header_list = self.header_list[header_stored_indices]
            # sort the data
            self.data_stream = self.data_stream[header_stored_indices, :]
            data_new = data_new[header_new_indices, :]
        else:
            # match header_new to self.header_list and sort the data
            header_new_indices = [
                    header_new.index(x) for x in self.header_list
                ]
            data_new = data_new[:, header_new_indices]
        return data_new, header_new

    def check_average_data(
            self,
            ) -> None:
        """
        Checks if the average data is present. If not, it creates it.
        Also if there is not enough average time to cover the next averaging, 
        this adds another hour to the average time.
        """
        base_rounding_interval_sec = 3600.0
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
            self.average_base_data = np.zeros(
                    (len(self.header_list),
                    len(self.average_base_time))
                )*np.nan
            self.average_base_data_std = np.zeros(
                    (len(self.header_list),
                    len(self.average_base_time))
                )*np.nan

        # check if the average time is long enough to cover the next averaging,
        # if not, add another base_rounding_interval_sec to the average time
        if self.average_base_time[-1] <= self.time_stream[-1] and self.average_epoch_start is None:
            average_time_end = int(convert.round_arbitrary(self.time_stream[-1],
                base=base_rounding_interval_sec, mode='ceil'
                ))
            base_time_to_add = np.arange(self.average_base_time[-1], 
                average_time_end, self.average_base_sec
                )
            self.average_base_time = np.append(
                    self.average_base_time,
                    base_time_to_add
                )
            self.average_base_data = np.append(
                    self.average_base_data,
                    np.zeros((len(self.header_list),
                    len(base_time_to_add)))*np.nan, # +1 for the last element
                    axis=1
                )
            self.average_base_data_std = np.append(
                    self.average_base_data_std,
                    np.zeros((len(self.header_list),
                    len(base_time_to_add)))*np.nan, # +1 for the last element
                    axis=1
                )

    def average_to_base_interval(self) -> np.array:
        """
        Calculates the average of the data stream using the start of the 
        nearest previous hour as the initial time.
        """
        # check if the average time sufficient to cover the next averaging
        self.check_average_data()

        # average the data in the time interval initialization
        data_i0 = 0
        data_i1 = 0
        interval_look_buffer_multiple = 2
        start_time = self.time_stream[0]
        
        # estimating how much of the time_stream we would need to look at 
        # for a given average interval.
        if len(self.time_stream) > 100:
            time_lookup_span = round(
                    self.average_base_sec 
                    * interval_look_buffer_multiple
                    / np.nanmean(np.diff(self.time_stream[0:100]))
                )
        else:
            time_lookup_span = 100

        # loop through the average time intervals
        for i, time_i in enumerate(self.average_base_time, start=1):
            if (data_i1 < len(self.time_stream)) and (start_time < time_i):
                
                # trying to only look at the time_stream in the average time
                # interval, assumes that the time stream is sorted
                if data_i0+time_lookup_span < len(self.time_stream):
                    compare_bool = np.nonzero(
                            self.time_stream[data_i0:data_i0+time_lookup_span]
                            >= time_i
                        )
                    if len(compare_bool[0]) > 0:
                        data_i1 = data_i0 + compare_bool[0][0]
                    else:
                        # used it all for this iteration
                        compare_bool = np.nonzero(
                                self.time_stream[data_i0:]
                                >= time_i
                            )
                        data_i1 = data_i0+compare_bool[0][0]
                        # re-calculate time look up span, 
                        # as timesteps have changed 
                        if len(self.time_stream[data_i0:]) > 100:
                            time_lookup_span = round(
                                    self.average_base_sec
                                    * interval_look_buffer_multiple
                                    / np.nanmean(
                                        np.diff(
                                            self.time_stream[
                                                data_i0:data_i0+100
                                            ]
                                        )
                                    )
                                )
                        else:
                            time_lookup_span = 100
                else:
                    compare_bool = np.nonzero(
                                self.time_stream[
                                        data_i0 : data_i0 + time_lookup_span
                                    ] >= time_i
                            )
                    if len(compare_bool[0]) > 0:
                        data_i1 = data_i0 + compare_bool[0][0]
                    else:
                        data_i1 = len(self.time_stream)

                if data_i0 < data_i1:
                    # average the data in the time interval
                    self.average_base_data[:, i-1] = np.nanmean(
                        self.data_stream[:, data_i0:data_i1], axis=1
                        ) # the actual averaging of data is here
                    self.average_base_data_std[:, i-1] = np.nanstd(
                        self.data_stream[:, data_i0:data_i1], axis=1
                        ) # the actual std data is here
                else:
                    start_time = self.time_stream[data_i1]
                    
                data_i0 = data_i1

    def re_average_data(
            self,
            reaverage_base_sec=None,
            epoch_start=None,
            epoch_end=None
            ) -> None:
        """
        Re-averages the data to a new average base time.
        """
        self.average_base_time = np.array([])
        self.average_base_data = np.array([])
        self.average_base_data_std = np.array([])
        if reaverage_base_sec is not None:
            self.average_base_sec = reaverage_base_sec
        if epoch_start is not None:
            self.average_epoch_start = epoch_start
            self.average_epoch_end = epoch_end
        self.average_to_base_interval()

    def average_data(self) -> None:
        """
        Averages the data to the base time.
        """
        self.average_to_base_interval()

    def return_data(self, keys=None, raw=False) -> np.array:
        """
        Returns the data stream, on the base time interval.
        """
        # self.average_to_base_interval()

        if keys is None:
            if raw:
                return self.data_stream
            else:
                return self.average_base_data
        else:
            key_values = convert.get_values_in_dict(keys, self.header_dict)
            if raw:
                return self.data_stream[key_values, :]
            else:
                return self.average_base_data[key_values, :]

    def return_std(self, keys=None, raw=False) -> np.array:
        """
        Returns the data stream, on the base time interval.
        """
        # self.average_to_base_interval()

        if keys is None:
            if raw:
                return self.data_stream
            else:
                return self.average_base_data_std
        else:
            key_values = convert.get_values_in_dict(keys, self.header_dict)
            if raw:
                return self.data_stream[key_values, :]
            else:
                return self.average_base_data_std[key_values, :]

    def return_time(self, datetime64=False, raw=False) -> np.array:
        """
        Returns the time stream, on the base time interval.
        """
        # self.average_to_base_interval()

        if datetime64:
            if raw:
                return convert.datetime64_from_epoch_array(self.time_stream)
            else:
                return convert.datetime64_from_epoch_array(self.average_base_time)
        else:
            if raw:
                return self.time_stream
            else:
                return self.average_base_time

    def return_header_list(self) -> list:
        """
        Returns the header list.
        """
        return self.header_list

    def return_header_dict(self) -> dict:
        """
        Returns the header dictionary.
        """
        return self.header_dict


class DataLake():
    """DataLake class to store and manage datastreams
    """

    def __init__(self, settings: dict, path: str = None, UTC_to_local: int = 0):
        """initialise the data lake"""
        self.settings = settings
        self.path_to_data = path
        self.datastreams = {}  # dictionary of datastreams to be filled in by the add_data function
        self.UTC_to_local = UTC_to_local  # UTC to local time offset in hours
        # first define the datastream

    def list_datastream(self):
        """list the datastreams in the data lake"""
        return list(self.datastreams.keys())

    def update_datastream(self):
        """update the datastreams in the data lake"""
        for key in self.settings.keys():
            if key in self.datastreams.keys():
                self.update_data(key)
            else:
                print('Initialising datastream: ', key)
                self.initialise_datastream(key)

    def update_data(self, key: str):
        """update the data in the datastream"""

    def add_processed_datastream(
            self,
            key: str,
            time_stream: np.array,
            data_stream: np.array,
            data_stream_header: list,
            average_times: list,
            average_base: int
            ):
        """add processed data to the datalake"""
        self.datastreams[key] = DataStream(
                            data_stream_header,
                            average_times,
                            average_base
                        )  # first define the datastream
        self.datastreams[key].add_data(
                            time_stream,
                            data_stream
                        )  # add the data to the datastream

    def initialise_datastream(self, key: str):
        """initialise the datastream"""
        # import data based on the settings
        files, full_paths, sizes = loader.get_files_in_folder_with_size(
            self.path_to_data,
            self.settings[key]['relative_data_folder'],
            self.settings[key]['filename_regex'],
            size=10)
        first_pass = True
        # load the data type
        for path in full_paths:
            # 1D general data
            if self.settings[key]['data_loading_function'] == 'general_load':
                epoch_time, data = self.import_general_data(path, key)
                if first_pass:
                    self.datastreams[self.settings[key]['data_stream_name']] = DataStream(
                            self.settings[key]['data_header'],
                            [600],
                            self.settings[key]['base_interval_sec']
                        ) # first define the datastream
                    first_pass = False
                
                # add the data
                self.datastreams[
                        self.settings[key]['data_stream_name']
                    ].add_data(
                        time_stream=epoch_time,
                        data_stream=data,
                    )

            # 2D sizer data
            elif self.settings[key]['data_loading_function'] == 'general_2d_sizer_load':
                epoch_time, dp_header, data_2D, data_1D = self.import_sizer_data(path, key)
                
                if first_pass:
                    # generate 1D datastream
                    self.datastreams[self.settings[key]['data_stream_name'][0]] = DataStream(
                            self.settings[key]['data_header'],
                            [600],
                            self.settings[key]['base_interval_sec']
                        ) # first define the datastream
                    # generate 2D datastream
                    self.datastreams[self.settings[key]['data_stream_name'][1]] = DataStream(
                            dp_header,
                            [600],
                            self.settings[key]['base_interval_sec']
                        ) # first define the datastream
                    first_pass = False
                
                # add the data
                self.datastreams[
                        self.settings[key]['data_stream_name'][0]
                    ].add_data(
                        time_stream=epoch_time,
                        data_stream=data_1D,
                    )
                self.datastreams[
                        self.settings[key]['data_stream_name'][1]
                    ].add_data(
                        time_stream=epoch_time,
                        data_stream=data_2D,
                        header_check=True,
                        header=dp_header
                    )
            # nothing loaded
            else:
                raise ValueError('Data loading function not recognised', self.settings[key]['data_loading_function'])

    def reaverage_datalake(
            self,
            average_base_sec: int = None,
            stream_keys: list = None,
            epoch_start: int = None,
            epoch_end: int = None,
            ) -> object:
        """Reaverage data stream in datalake

        Parameters
        ----------
        datalake : object
            datalake object
        stream_keys : list, optional
            list of stream keys to reaverage, by default None

        Returns
        -------
        object
            datalake object
        """
        if stream_keys is None:
            stream_keys = self.list_datastream()
        for key in stream_keys:
            self.datastreams[key].re_average_data(average_base_sec, epoch_start, epoch_end)

    def import_sizer_data(self, path: str, key: str):
        """add the 2D sizer data from sources like SMPS or APS """
        data = loader.data_raw_loader(path)

        if 'date_location' in self.settings[key].keys():
            date_offset = loader.non_standard_date_location(data, self.settings[key]['date_location'])
        else:
            date_offset = None


        epoch_time, dp_header, data_2D, data_1D = loader.sizer_data_formatter(
                data = data,
                data_checks=self.settings[key]['data_checks'],
                data_sizer_reader=self.settings[key]['data_sizer_reader'],
                time_column=self.settings[key]['time_column'],
                time_format=self.settings[key]['time_format'],
                delimiter=self.settings[key]['data_delimiter'],
                date_offset=date_offset,
            )
        return epoch_time, dp_header, data_2D.T, data_1D.T

    def import_general_data(self, path: str, key: str):
        """add the general data from sources that are timeseries and values, e.g., weather station  """
        data = loader.data_raw_loader(path)

        #code spell with sizer data, should consolidate
        if 'date_location' in self.settings[key].keys():
            date_offset = loader.non_standard_date_location(data, self.settings[key]['date_location'])
        else:
            date_offset = None

        epoch_time, data = loader.general_data_formatter(
                data = data,
                data_checks=self.settings[key]['data_checks'],
                data_column=self.settings[key]['data_column'],
                time_column=self.settings[key]['time_column'],
                time_format=self.settings[key]['time_format'],
                delimiter=self.settings[key]['data_delimiter'],
                date_offset=date_offset,
                seconds_shift=self.settings[key]['Time_shift_to_Linux_Epoch_sec']
            )
        return epoch_time, data.T

    def remove_zeros(self, zeros_keys: list = None):
        """remove zeros from the datastream"""
        
        if zeros_keys is None:
            zeros_keys = ['CAPS_dual', 'pass3']

        for key in zeros_keys:
            if key in self.datastreams.keys():
                if key == 'CAPS_dual':
                    self.datastreams['CAPS_dual'] = drop_zeros(self.datastreams['CAPS_dual'], ['Zero_dry_CAPS', 'Zero_wet_CAPS'])
                elif key == 'pass':
                    self.datastreams['pass3'] = drop_zeros(self.datastreams['pass3'], ['Zero'])


def DataLake_saveNload(path: str, datalake: object = None, sufix_name: str = None):
    """
    Save and load datalake object, as pickle file. Adds output folder to path
    if not already present. If datalake is None, then it will load the pickle
    file. If datalake is not None, then it will save the datalake as pickle.

    Parameters
    ----------
    datalake : DataLake
        DataLake object to be saved and loaded.
    path : str
        Path to save and load pickle file.
    sufix_name : str, optional
        Sufix to add to pickle file name. The default is None.
    
    Returns
    -------
    datalake2 : DataLake
        Loaded DataLake object. or True if save was successful.
    """

    # add output folder to path if not already present
    output_folder = os.path.join(path, 'output')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    # add sufix to file name if present
    if sufix_name is not None:
        file_name = 'datalake_' + sufix_name + '.pk'
    else:
        file_name = 'datalake.pk'

    # path to save and load pickle file
    file_path = os.path.join(output_folder, file_name)

    # save datalake
    if datalake is not None:
        # open a file, where you ant to store the data
        file = open(file_path, 'wb')
        # dump information to that file
        pickle.dump(datalake, file)
        # close the file
        file.close()
        return True

    # load datalake
    else:
        # open pickle file
        file = open(file_path, 'rb')
        # load datalake
        datalake = pickle.load(file)
        # close the file
        file.close()
        return datalake


def datastream_to_csv(
        datastream,
        path,
        filename,
        header_keys=None,
        time_shift_sec=0,
        ):
    """
    Function to save a datastream to a csv file.
    TODO: remove the pandas dependency
    
    Parameters
    ----------
    datastream : DataStream
        DataStream object to be saved
    path : str
        path to save the csv file
    time_shift_sec : int, optional
        time shift in seconds, by default 0
    """

    # save the data streams to text files
    data = datastream.return_data(keys=header_keys)
    time = loader.datetime64_from_epoch_array(
            datastream.return_time(datetime64=False),
            delta=time_shift_sec
        )

    if header_keys is None:
        header = list(datastream.return_header_list())
    else:
        header = header_keys

    combo = pd.DataFrame(data.T, index=time, columns=header)
    combo.index.name = 'DateTime'

    # add output folder to path if not already present
    output_folder = os.path.join(path, 'output')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    save_path = os.path.join(path, 'output', filename+'.csv')
    combo.to_csv(save_path, sep=',', index=True)


def datalake_to_csv(
        datalake,
        path,
        time_shift_sec=0,
        keys=None,
        sufix_name=None,
        ):
    """
    Function to save a datalake to a csv file. Iterates through the datastreams,
    or just the keys specified.

    Parameters
    ----------
    datalake : DataLake
        object of datastreams be saved
    path : str
        path to save the csv file
    time_shift_sec : int, optional
        time shift in seconds, by default 0
    keys : list, optional
        list of keys to save, by default None
    """

    if keys is None:
        keys = list(datalake.datastreams.keys())

    for key in keys:
        if sufix_name is not None:
            save_name = key + '_' + sufix_name
        else:
            save_name = key

        datastream_to_csv(
            datastream=datalake.datastreams[key],
            path=path,
            filename=save_name,
            time_shift_sec=time_shift_sec,
        )
        print('saved: ', key)