"""interface to import data to a data stream"""
from typing import Dict, Any, List
import os
from datacula import loader
from datacula.stream import Stream


def get_new_files(
        path: str,
        import_settings: Dict[str, Any],
        loaded_list: List[Any] = None,
) -> tuple:
    """
    Scan a directory for new files based on import settings and stream status.

    This function looks for files in a specified path using import settings.
    It compares the new list of files with a pre-loaded list in the stream
    object to determine which files are new. The comparison is made based on
    file names and sizes. It returns a tuple with the paths of new files, a
    boolean indicating if this was the first pass, and a list of file
    information for new files.

    Parameters:
    ----------
    path : str
        The top-level directory path to scan for files.
    import_settings : dict
        A dictionary with 'relative_data_folder', 'filename_regex',
        and 'MIN_SIZE_BYTES' as keys
        used to specify the subfolder path and the regex pattern for filtering
        file names. It should also include 'min_size' key to specify the
        minimum size of the files to be considered.
    loaded_list : list of lists
        A list of lists with file names and sizes that have already been
        loaded. The default is None. If None, it will be assumed that no
        files have been loaded.

    Returns:
    -------
    tuple of (list, bool, list)
        A tuple containing a list of full paths of new files, a boolean
        indicating if no previous files were loaded (True if it's the first
        pass), and a list of lists with new file names and sizes.

    Returns:
    Raises:
    ------
    YourErrorType
        Explanation of when and why your error is raised and what it means.
    """
    # Validate the path and settings
    if not os.path.isdir(path):
        raise ValueError(
            f"The provided path does not exist or is not a directory: {path}")
    # Settings Verification
    required_keys = [
        'relative_data_folder',
        'filename_regex',
        'MIN_SIZE_BYTES']
    if not all(key in import_settings for key in required_keys):
        raise KeyError(
            f"import_settings must contain the following keys: {required_keys}"
        )
    # Loaded files verification
    if loaded_list is not None:
        if not all(isinstance(item, list) for item in loaded_list):
            raise TypeError(
                f"loaded_list must be a list of lists. It is currently a \
                list of {type(loaded_list[0])}"
            )
        if not all(len(item) == 2 for item in loaded_list):
            raise ValueError(
                f"loaded_list must be a list of lists with 2 items. It is \
                currently a list of lists with {len(loaded_list[0])} items"
            )

    # import data based on the settings what type
    file_names, full_paths, file_sizes = loader.get_files_in_folder_with_size(
        path=path,
        subfolder=import_settings['relative_data_folder'],
        filename_regex=import_settings['filename_regex'],
        min_size=import_settings['MIN_SIZE_BYTES'])
    # combined the file names and sizes into a list of lists
    file_info = [[name, size]
                 for name, size in zip(file_names, file_sizes)]
    # check and compare the previous file list with the new list.
    if not loaded_list:
        first_pass = True
    else:
        first_pass = False
        # check if the files are the same loop through the files
        new_full_paths = []
        new_file_info = []
        for i, comparison_list in enumerate(file_info):
            if comparison_list not in loaded_list:
                new_full_paths.append(full_paths[i])
                new_file_info.append(comparison_list)
        full_paths = new_full_paths
        file_info = new_file_info
    return full_paths, first_pass, file_info


# def load_files_interface(
#         full_paths: list,
#         settings: dict,
#         stream: Stream,
# ) -> None:
#     first_pass = True
#     # load the data type
#     for file_i, path in enumerate(full_paths):
#         print('Loading data from:', os.path.split(path)[-1])

#         if self.settings[key]['data_loading_function'] == 'general_1d_load':
#             self.initialise_1d_datastream(key, path, first_pass)
#             first_pass = False
#         elif (self.settings[key]['data_loading_function'] ==
#                 'general_2d_sizer_load'):
#             self.initialise_2d_datastream(key, path, first_pass)
#             first_pass = False
#         elif (self.settings[key]['data_loading_function'] ==
#                 'netcdf_load'):
#             self.initialise_netcdf_datastream(key, path, first_pass)
#             first_pass = False
#         else:
#             raise ValueError('Data loading function not recognised',
#                              self.settings[key]['data_loading_function'])


# def initialise_1d_datastream(
#     self,
#     key: str,
#     path: str,
#     first_pass: bool
# ) -> None:
#     """
#     Initialises a 1D datastream using the settings in the DataLake object.

#     Parameters:
#     ----------
#         key (str): The key of the datastream to initialise.
#         path (str): The path of the file to load data from.
#         first_pass (bool): Whether this is the first time loading data.

#     Returns:
#     ----------
#         None.

#     : change the way the datastream is stored so type hints can be used
#     """
#     epoch_time, data = self.import_general_data(path, key)
#     if first_pass:
#         self.datastreams[
#             self.settings[key]['data_stream_name']] = DataStream(
#                 header_list=self.settings[key]['data_header'],
#                 average_times=[600],
#                 average_base=self.settings[key]['base_interval_sec']
#         )
#     self.datastreams[self.settings[key]['data_stream_name']].add_data(
#         time_stream=epoch_time,
#         data_stream=data,
#     )


# def initialise_2d_datastream(
#     self,
#     key: str,
#     path: str,
#     first_pass: bool
# ) -> None:
#     """
#     Initialises a 2D datastream using the settings in the DataLake object.

#     Parameters:
#     ----------
#         key (str): The key of the datastream to initialise.
#         path (str): The path of the file to load data from.
#         first_pass (bool): Whether this is the first time loading data.

#     Returns:
#     ----------
#         None.
#     """
#     epoch_time, dp_header, data_2d, data_1d = self.import_sizer_data(
#         path=path,
#         key=key
#     )
#     if first_pass:
#         self.datastreams[
#             self.settings[key]['data_stream_name'][0]] = DataStream(
#                 header_list=self.settings[key]['data_header'],
#                 average_times=[600],
#                 average_base=self.settings[key]['base_interval_sec']
#         )
#         self.datastreams[
#             self.settings[key]['data_stream_name'][1]] = DataStream(
#                 header_list=dp_header,
#                 average_times=[600],
#                 average_base=self.settings[key]['base_interval_sec']
#         )
#     self.datastreams[self.settings[key]['data_stream_name'][0]].add_data(
#         time_stream=epoch_time,
#         data_stream=data_1d,
#     )
#     self.datastreams[self.settings[key]['data_stream_name'][1]].add_data(
#         time_stream=epoch_time,
#         data_stream=data_2d,
#         header_check=True,
#         header=dp_header
#     )


# def initialise_netcdf_datastream(
#     self,
#     key: str,
#     path: str,
#     first_pass: bool
# ) -> None:
#     """
#     Initialises a netcdf datastream using the settings in the DataLake
#     object. This can load either 1D or 2D data, as specified in the
#     settings.

#     Parameters:
#     ----------
#         key (str): The key of the datastream to initialise.
#         path (str): The path of the file to load data from.
#         first_pass (bool): Whether this is the first time loading data.

#     Returns:
#     ----------
#         None.
#     """
#     # ValueKey error if netcdf_reader not in settings
#     if 'netcdf_reader' not in self.settings[key]:
#         raise ValueError('netcdf_reader not in settings')

#     # Load the data 1d data
#     if 'data_1d' in self.settings[key]['netcdf_reader']:
#         epoch_time, header_1d, data_1d = loader.netcdf_data_1d_load(
#             file_path=path,
#             settings=self.settings[key])

#         if first_pass:  # create the datastream
#             self.datastreams[
#                 self.settings[key]['data_stream_name'][0]
#             ] = DataStream(
#                 header_list=header_1d,
#                 average_times=[600],
#                 average_base=self.settings[key]['base_interval_sec']
#             )

#         self.datastreams[
#             self.settings[key]['data_stream_name'][0]
#         ].add_data(
#             time_stream=epoch_time,
#             data_stream=data_1d,
#         )

#     if 'data_2d' in self.settings[key]['netcdf_reader']:
#         epoch_time, header_2d, data_2d = loader.netcdf_data_2d_load(
#             file_path=path,
#             settings=self.settings[key])

#         if first_pass:  # create the datastream
#             self.datastreams[
#                 self.settings[key]['data_stream_name'][1]
#             ] = DataStream(
#                 header_list=header_2d,
#                 average_times=[600],
#                 average_base=self.settings[key]['base_interval_sec']
#             )

#         self.datastreams[
#             self.settings[key]['data_stream_name'][1]
#         ].add_data(
#             time_stream=epoch_time,
#             data_stream=data_2d,
#             header_check=True,
#             header=header_2d
#         )
