"""interface to import data to a data stream"""

import os
from datacula import loader
from datacula.stream import Stream

MIN_SIZE_BYTES = 10

def import_file_stream(
        path,
        import_settings,
        stream=Stream(),
        ) -> None:

    """
    Initialises a file stream using the settings provided.

    Parameters:
    ----------
        key (str): The key of the datastream to initialise.

    Returns:
        None.
    """
    # import data based on the settings
    file_names, full_paths, _ = loader.get_files_in_folder_with_size(
        path=path,
        subfolder=import_settings['relative_data_folder'],
        filename_regex=import_settings['filename_regex'],
        min_size=MIN_SIZE_BYTES)
    first_pass = True
    # load the data type
    for path in full_paths:
        print('Loading data from:', os.path.split(path)[-1])

        if self.settings[key]['data_loading_function'] == 'general_load':
            self.initialise_1d_datastream(key, path, first_pass)
            first_pass = False
        elif (self.settings[key]['data_loading_function'] ==
                'general_2d_sizer_load'):
            self.initialise_2d_datastream(key, path, first_pass)
            first_pass = False
        elif (self.settings[key]['data_loading_function'] ==
                'netcdf_load'):
            self.initialise_netcdf_datastream(key, path, first_pass)
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

def initialise_netcdf_datastream(
            self,
            key: str,
            path: str,
            first_pass: bool
        ) -> None:
    """
    Initialises a netcdf datastream using the settings in the DataLake
    object. This can load either 1D or 2D data, as specified in the
    settings.

    Parameters:
    ----------
        key (str): The key of the datastream to initialise.
        path (str): The path of the file to load data from.
        first_pass (bool): Whether this is the first time loading data.

    Returns:
    ----------
        None.
    """
    # ValueKey error if netcdf_reader not in settings
    if 'netcdf_reader' not in self.settings[key]:
        raise ValueError('netcdf_reader not in settings')

    # Load the data 1d data
    if 'data_1d' in self.settings[key]['netcdf_reader']:
        epoch_time, header_1d, data_1d = loader.netcdf_data_1d_load(
            file_path=path,
            settings=self.settings[key])

        if first_pass:  # create the datastream
            self.datastreams[
                self.settings[key]['data_stream_name'][0]
                ] = DataStream(
                    header_list=header_1d,
                    average_times=[600],
                    average_base=self.settings[key]['base_interval_sec']
                )

        self.datastreams[
            self.settings[key]['data_stream_name'][0]
            ].add_data(
                    time_stream=epoch_time,
                    data_stream=data_1d,
                )

    if 'data_2d' in self.settings[key]['netcdf_reader']:
        epoch_time, header_2d, data_2d = loader.netcdf_data_2d_load(
            file_path=path,
            settings=self.settings[key])

        if first_pass:  # create the datastream
            self.datastreams[
                self.settings[key]['data_stream_name'][1]
                ] = DataStream(
                    header_list=header_2d,
                    average_times=[600],
                    average_base=self.settings[key]['base_interval_sec']
                )

        self.datastreams[
            self.settings[key]['data_stream_name'][1]
            ].add_data(
                    time_stream=epoch_time,
                    data_stream=data_2d,
                    header_check=True,
                    header=header_2d
                )
