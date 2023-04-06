"""creates the datastream and datalake class"""


from typing import Tuple, List, Optional

import numpy as np
from datacula import loader, stats
from datacula.lake.datastream import DataStream


class DataLake():
    """
    DataLake class to store and manage datastreams.

    This class provides a simple way to manage multiple datastreams, where
    each datastream is stored as a separate instance of the DataStream class.
    Datastreams can be added to the DataLake using the add_data method, and
    can be accessed or modified using the various methods provided by the
    DataStream class.

    Attributes:
    ----------
        settings (dict): A dictionary of settings for each datastream to be
            added to the DataLake. The keys of the dictionary should be the
            names of the datastreams, and the values should be dictionaries
            containing the settings for each datastream.
        path (str): The path to the data to be read in. Default is None.
        utc_to_local (int): The UTC to local time offset in hours.
            Default is 0.

    Methods:
    ----------
        list_datastream: Returns a list of the datastreams in the DataLake.
        update_datastream: Updates all datastreams in the DataLake.
        add_processed_datastream: Adds a processed datastream to the DataLake.
        initialise_datastream: Initialises a datastream using the settings in
            the DataLake object.
        reaverage_datastream: Reaverages the data in the specified datastream.
        remove_zeros: Removes filter/zeros from the specified datastream.
        

    Example usage:
    ----------
        # Instantiate a DataLake object
        settings = {TODO: add this}
        datalake = DataLake(settings)

        # Add a new datastream to the DataLake
        datalake.add_data(TODO: add this)

        # Get a list of the datastreams in the DataLake
        datastream_list = datalake.list_datastream()

        # Update all datastreams in the DataLake
        datalake.update_datastream()
    """

    def __init__(
                self,
                settings: dict,
                path: str = None,
                utc_to_local: int = 0
            ):
        """
        Initializes the DataLake object.

        Parameters:
        ----------
            settings (dict): A dictionary of settings for each datastream to
                be added to the DataLake. The keys of the dictionary should be
                the names of the datastreams, and the values should be
                dictionaries containing the settings for each datastream.
            path (str, optional): The path to the data to be read in.
                Default is None.
            utc_to_local (int, optional): The UTC to local time offset
                in hours. Default is 0. TODO: add UTC time zone support
        """
        self.settings = settings
        self.path_to_data = path
        # dictionary of datastreams to be filled in by the add_data function
        self.datastreams = {}
        self.utc_to_local = utc_to_local

    def list_datastream(self) -> list:
        """
        Returns a list of the datastreams in the DataLake.

        Returns:
        ----------
            list: A list of the datastream names in the DataLake.
        """
        return list(self.datastreams.keys())

    def update_datastream(self) -> None:
        """
        Updates all datastreams in the DataLake.

        This method loops through each datastream defined in the `settings`
        attribute of the DataLake and calls the `update_data` method for
        each datastream. If a datastream does not exist in the DataLake,
        it will be initialized using the `initialise_datastream` method.

        Returns:
        ----------
            None.
        """
        for key in self.settings:
            if key in self.datastreams:
                self.update_data(key)
            else:
                print('Initialising datastream: ', key)
                self.initialise_datastream(key)

    def update_data(self, key: str) -> None:
        """
        Updates the data in the specified datastream.

        Parameters:
        ----------
            key (str): The key corresponding to the datastream to be updated.

        Raises:
        ----------
            NotImplementedError: This method is a placeholder and needs to be
                implemented in a method.
        """
        raise NotImplementedError(
            "update_data method needs to be implemented in a subclass.")

    # pylint: disable=too-many-arguments
    def add_processed_datastream(
        self,
        key: str,
        time_stream: np.array,
        data_stream: np.array,
        data_stream_header: list,
        average_times: list,
        average_base: int
    ) -> None:
        """
        Adds a processed datastream to the DataLake.

        This method adds a new processed datastream to the DataLake.
        The processed datastream is represented as a `DataStream` object,
        which is created using the provided `data_stream_header`,
        `average_times`, and `average_base` arguments. The `time_stream`
        and `data_stream` arrays are then added to the `DataStream` object
        using the `add_data` method.

        Parameters:
        ----------
            key (str): The key to use for the new datastream.
            time_stream (np.array): The time stream for the new datastream.
            data_stream (np.array): The data stream for the new datastream.
            data_stream_header (list): The header for the new datastream.
            average_times (list): The list of times to average the datastream
                over.
            average_base (int): The base time in seconds to average the
                datastream over.

        Returns:
        ----------
            None.
        """
        self.datastreams[key] = DataStream(
            data_stream_header,
            average_times,
            average_base
        )
        self.datastreams[key].add_data(
            time_stream,
            data_stream
        )

    def initialise_datastream(self, key: str) -> None:
        """
        Initialises a datastream using the settings in the DataLake object.

        Parameters:
        ----------
            key (str): The key of the datastream to initialise.

        Returns:
            None.
        """
        # import data based on the settings
        _, full_paths, _ = loader.get_files_in_folder_with_size(
            self.path_to_data,
            self.settings[key]['relative_data_folder'],
            self.settings[key]['filename_regex'],
            min_size=10)
        first_pass = True
        # load the data type
        for path in full_paths:
            if self.settings[key]['data_loading_function'] == 'general_load':
                self.initialise_1d_datastream(key, path, first_pass)
                first_pass = False
            elif (self.settings[key]['data_loading_function'] ==
                    'general_2d_sizer_load'):
                self.initialise_2d_datastream(key, path, first_pass)
                first_pass = False
            else:
                raise ValueError('Data loading function not recognised',
                                 self.settings[key]['data_loading_function'])

    def initialise_1d_datastream(
                self,
                key: str,
                path: str,
                first_pass: bool
            ) -> None:
        """
        Initialises a 1D datastream using the settings in the DataLake object.

        Parameters:
        ----------
            key (str): The key of the datastream to initialise.
            path (str): The path of the file to load data from.
            first_pass (bool): Whether this is the first time loading data.

        Returns:
        ----------
            None.

        TODO: change the way the datastream is stored so type hints can be used
        """
        epoch_time, data = self.import_general_data(path, key)
        if first_pass:
            self.datastreams[
                self.settings[key]['data_stream_name']] = DataStream(
                    header_list=self.settings[key]['data_header'],
                    average_times=[600],
                    average_base=self.settings[key]['base_interval_sec']
                )
        self.datastreams[self.settings[key]['data_stream_name']].add_data(
                    time_stream=epoch_time,
                    data_stream=data,
                )

    def initialise_2d_datastream(
                self,
                key: str,
                path: str,
                first_pass: bool
            ) -> None:
        """
        Initialises a 2D datastream using the settings in the DataLake object.

        Parameters:
        ----------
            key (str): The key of the datastream to initialise.
            path (str): The path of the file to load data from.
            first_pass (bool): Whether this is the first time loading data.

        Returns:
        ----------
            None.
        """
        epoch_time, dp_header, data_2d, data_1d = self.import_sizer_data(
                path=path,
                key=key
            )
        if first_pass:
            self.datastreams[
                self.settings[key]['data_stream_name'][0]] = DataStream(
                    header_list=self.settings[key]['data_header'],
                    average_times=[600],
                    average_base=self.settings[key]['base_interval_sec']
                )
            self.datastreams[
                self.settings[key]['data_stream_name'][1]] = DataStream(
                    header_list=dp_header,
                    average_times=[600],
                    average_base=self.settings[key]['base_interval_sec']
                )
        self.datastreams[self.settings[key]['data_stream_name'][0]].add_data(
                    time_stream=epoch_time,
                    data_stream=data_1d,
                )
        self.datastreams[self.settings[key]['data_stream_name'][1]].add_data(
                    time_stream=epoch_time,
                    data_stream=data_2d,
                    header_check=True,
                    header=dp_header
                )

    def reaverage_datastreams(
                self,
                average_base_sec: Optional[int] = None,
                stream_keys: Optional[List[str]] = None,
                epoch_start: Optional[int] = None,
                epoch_end: Optional[int] = None
            ) -> None:
        """
        Reaverage datastreams in the datalake.

        Parameters
        ----------
        average_base_sec : int, optional
            The new average base time in seconds, by default None.
        stream_keys : list of str, optional
            The keys of the datastreams to reaverage, by default None
            (i.e., reaverage all datastreams).
        epoch_start : int, optional
            The start epoch time of the reaverage interval, by default None
            (i.e., use the original epoch start time).
        epoch_end : int, optional
            The end epoch time of the reaverage interval, by default None
            (i.e., use the original epoch end time).
        """
        if stream_keys is None:
            stream_keys = self.list_datastream()
        for key in stream_keys:
            self.datastreams[key].reaverage(
                reaverage_base_sec=average_base_sec,
                epoch_start=epoch_start,
                epoch_end=epoch_end
            )

    def import_sizer_data(
                self,
                path: str,
                key: str
            ) -> Tuple[np.array, np.array, np.array, np.array]:
        """
        Imports the 2D sizer data from sources like SMPS or APS.

        Parameters
        ----------
        path : str
            The path to the data file.
        key : str
            The key of the datastream.

        Returns
        -------
        Tuple[np.array, np.array, np.array, np.array]
            A tuple containing epoch time, dp header, 2D sizer data, and
            1D sizer data.
        """

        data = loader.data_raw_loader(path)

        # Determine the date offset
        if 'date_location' in self.settings[key]:
            date_offset = loader.non_standard_date_location(
                    data=data,
                    date_location=self.settings[key]['date_location']
                )
        else:
            date_offset = None

        # Format the data
        epoch_time, dp_header, data_2d, data_1d = loader.sizer_data_formatter(
            data=data,
            data_checks=self.settings[key]['data_checks'],
            data_sizer_reader=self.settings[key]['data_sizer_reader'],
            time_column=self.settings[key]['time_column'],
            time_format=self.settings[key]['time_format'],
            delimiter=self.settings[key]['data_delimiter'],
            date_offset=date_offset,
        )

        # Transpose the data
        data_2d = data_2d.T
        data_1d = data_1d.T

        return epoch_time, dp_header, data_2d, data_1d

    def import_general_data(
                self,
                path: str,
                key: str
            ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Add the general data from sources that are timeseries and values,
        e.g., weather station or gas analyser.

        Parameters:
        -----------
        path: str
            Path to the data file.
        key: str
            Key to the data source configuration in the settings.

        Returns:
        --------
        Tuple[np.ndarray, np.ndarray]:
            Epoch time and data formatted as 2D array, transposed
            (time along the first axis).
        """
        data = loader.data_raw_loader(path)

        # Code spell with sizer data, should consolidate.
        if 'date_location' in self.settings[key].keys():
            date_offset = loader.non_standard_date_location(
                    data=data,
                    date_location=self.settings[key]['date_location']
                )
        else:
            date_offset = None

        epoch_time, data = loader.general_data_formatter(
            data=data,
            data_checks=self.settings[key]['data_checks'],
            data_column=self.settings[key]['data_column'],
            time_column=self.settings[key]['time_column'],
            time_format=self.settings[key]['time_format'],
            delimiter=self.settings[key]['data_delimiter'],
            date_offset=date_offset,
            seconds_shift=self.settings[key]['Time_shift_to_Linux_Epoch_sec']
        )

        # Transpose the data
        data = data.T
        return epoch_time, data

    def remove_zeros(self, zeros_keys: Optional[List[str]] = None) -> None:
        """
        Removes zero values from the datastreams.

        Parameters
        ----------
        zeros_keys : list of str, optional
            List of keys for the datastreams to remove zeros from.
            If None, default keys ['CAPS_dual', 'pass3'] are used.
        """
        if zeros_keys is None:
            zeros_keys = ['CAPS_dual', 'pass3']

        for key in zeros_keys:
            if key in self.datastreams:
                if key == 'CAPS_dual':
                    self.datastreams['CAPS_dual'] = stats.drop_zeros(
                            datastream_object=self.datastreams['CAPS_dual'],
                            zero_keys=['Zero_dry_CAPS', 'Zero_wet_CAPS']
                        )
                elif key == 'pass':
                    self.datastreams['pass3'] = stats.drop_zeros(
                            datastream_object=self.datastreams['pass3'],
                            zero_keys=['Zero']
                        )
