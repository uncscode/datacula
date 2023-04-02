"""File readers and loaders for datacula."""
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import glob, os, pickle
import numpy as np
from datetime import datetime
from datacula import convert


def data_raw_loader(file_path: str) -> list:
    """
    Loads the data from the file_path and returns a list of the data.
    """
    print('Loading data from: ', file_path.split("\\")[-2:])
    with open(file_path, 'r', encoding='utf8', errors='replace') as f:
        data = f.readlines()
    return data


def data_format_checks(data: list, data_checks: dict) -> list:
    """
    Checks if the data is in the correct format.
    TODO: change to case or keys check so that not all requirements need to be in the dic.
    """
    if data_checks['skip_rows'] > 0:
        data = data[data_checks['skip_rows']:]
    if data_checks['skip_end'] > 0:
        data = data[:-data_checks['skip_end']]
    if np.size(data_checks['characters']) == 1:  # min count
        data = [x for x in data if len(x) > data_checks['characters']]
    elif np.size(data_checks['characters']) == 2:  # min and max count
        data = [x for x in data if len(x) > data_checks['characters'][0] and len(x) < data_checks['characters'][1]]
    if data_checks['commas_count'] > -1:
        data = [x for x in data if x.count(',') == data_checks['commas_count']]
    if data_checks['forwardslash_count'] > -1:
        data = [x for x in data if x.count('/') == data_checks['forwardslash_count']]
    if data_checks['colon_count'] > -1:
        data = [x for x in data if x.count(':') == data_checks['colon_count']]
    data = [x.strip() for x in data]
    return data


def sample_data(
        data: list,
        time_column: int,
        time_format: str,
        data_columns: list,
        delimiter: str,
        date_offset: str = None,
        seconds_shift: int = 0,
    ) -> (np.array, np.array):
    """
    Samples the data to get the time and data streams.
    """
    epoc_time = np.zeros(len(data))
    data_array = np.zeros((len(data), len(data_columns)))

    for i, line in enumerate(data):
        line_array = line.split(delimiter) # split the line into an array
        
        if time_format == 'epoch': # fails if no time column in data
            epoc_time[i] = float(line_array[time_column])+seconds_shift
        elif bool(date_offset):
            epoc_time[i] = datetime.strptime(
                    date_offset
                    + ' '
                    + line_array[time_column],
                    time_format
                ).timestamp() + seconds_shift
        elif type(time_column) is int:
            epoc_time[i] = datetime.strptime(
                    line_array[time_column],
                    time_format
                ).timestamp() + seconds_shift
        elif type(time_column) is list:
            epoc_time[i] = datetime.strptime(
                    line_array[time_column[0]]
                    + ' '
                    + line_array[time_column[1]],
                    time_format
                ).timestamp() + seconds_shift
        else:
            print(
                    'time_column must be "epoch", int, or list ',
                    '(2 ints, to be combined with space) ',
                    'or set date_offset for a fixed date offset'
                )
        for j, col in enumerate(data_columns):
            if col < len(line_array):
                value = line_array[col].strip()
            else:
                value = ''
            
            if value == '': # no data
                data_array[i, j] = np.nan
            elif value.count('�') > 0:
                data_array[i, j] = np.nan
            elif value[0].isnumeric(): # if the first character is a number
                data_array[i, j] = float(value)
            elif value[-1].isnumeric():
                data_array[i, j] = float(value)
            elif value[0] == '-':
                data_array[i, j] = float(value)
            elif value[0] == '+':
                data_array[i, j] = float(value)
            elif value[0] == '.':
                data_array[i, j] = float(value)
            elif value.isalpha():
                true_match = [
                        'ON', 'on', 'On', 'oN', '1', 'True', 'true',
                        'TRUE', 'tRUE', 't', 'T', 'Yes', 'yes', 'YES',
                        'yES', 'y', 'Y'
                    ]
                false_match = [
                        'OFF', 'off', 'Off', 'oFF', '0',
                        'False', 'false', 'FALSE', 'fALSE', 'f',
                        'F', 'No', 'no', 'NO', 'nO', 'n', 'N'
                    ]
                nan_match = [
                        'NaN', 'nan', 'Nan', 'nAN', 'NAN', 'NaN',
                        'nAn', 'naN', 'NA', 'Na', 'nA', 'na', 
                        'N', 'n', '', 'aN', 'null', 'NULL', 'Null',
                    ]

                if value in true_match:
                    data_array[i, j] = 1
                elif value in false_match:
                    data_array[i, j] = 0
                elif value in nan_match:
                    data_array[i, j] = np.nan
                else:
                    raise ValueError('No DATA match, true or false or nan: ' + value)
            else:
                raise ValueError('DATA NOT READING IN CORRECTLY:', value)
    return epoc_time, data_array


def general_data_formatter(
        data: list,
        data_checks: dict,
        data_column: list,
        time_column: int,
        time_format: str,
        delimiter: str = ',',
        date_offset: str = None,
        seconds_shift: int = 0,
    ) -> (np.array, np.array):
    """
    Loads the data from the file_path and returns np.array of epoch time the data.
    """
    data = data_format_checks(data, data_checks)

    epoch_time, data_array = sample_data(
            data,
            time_column,
            time_format,
            data_column,
            delimiter,
            date_offset,
            seconds_shift,
        )
    return epoch_time, data_array


def sizer_data_formatter(
            data: list,
            data_checks: dict,
            data_sizer_reader: dict,
            time_column: int,
            time_format: str,
            delimiter: str = ',',
            date_offset: str = None,
        ) -> (np.array, np.array, np.array):
        """
        returns np.array of epoch time the data.
        """
        data_header = data[data_sizer_reader["header_rows"]].split(delimiter)
        dp_range = [
                data_header.index(data_sizer_reader["Dp_start_keyword"]),
                data_header.index(data_sizer_reader["Dp_end_keyword"])
            ]
        dp_columns = list(range(dp_range[0]+1, dp_range[1]))
        dp_header = np.array([data_header[i] for i in dp_columns])
        data_column = [
                data_header.index(x) for x in
                data_sizer_reader["list_of_data_headers"]
            ]

        data = data_format_checks(data, data_checks) # cleans the data
        epoch_time, data_smps_2D = sample_data(
                data,
                time_column,
                time_format,
                dp_columns,
                delimiter,
                date_offset
            ) # selects the 2D size distribution data
        epoch_time, data_smps_1D = sample_data(
                data,
                time_column,
                time_format,
                data_column,
                delimiter,
                date_offset
            ) # selects the 1D distribution data

        return epoch_time, dp_header, data_smps_2D, data_smps_1D


def non_standard_date_location(data, date_location) -> str:
    if date_location['method'] == 'file_header_block':
        date = data[date_location['row']].split(date_location['delimiter'])[date_location['index']].strip()
    else:
        raise ValueError('no other date location methods implemented')
    return date


def get_files_in_folder_with_size(
        path: str,
        subfolder: str,
        filename_regex: str,
        size: int = 10,
    ) -> (list, list, list):
    """get the files in the folder with a minimum size"""
    original_path = os.getcwd()
    search_path = os.path.join(
            path,
            subfolder
        )
    os.chdir(search_path) # this should not be needed
    file_list = glob.glob(filename_regex)
    os.chdir(original_path)

    # filter the files by size
    file_list = [
            file for file in file_list
            if os.path.getsize(os.path.join(path,subfolder,file)) > size
        ]
    full_path = [os.path.join(path,subfolder, x) for x in file_list]
    file_size = [os.path.getsize(x) for x in full_path]

    return file_list, full_path, file_size


def save_datalake(path: str, data_lake: object = None, sufix_name: str = None):
    """
    Save datalake object as a pickle file.

    Parameters
    ----------
    data_lake : DataLake
        DataLake object to be saved.
    path : str
        Path to save pickle file.
    sufix_name : str, optional
        Suffix to add to pickle file name. The default is None.
    """
    # create output folder if it does not exist
    output_folder = os.path.join(path, 'output')
    os.makedirs(output_folder, exist_ok=True)
    
    # add suffix to file name if present
    if sufix_name is not None:
        file_name = f'datalake_{sufix_name}.pk'
    else:
        file_name = 'datalake.pk'

    # path to save pickle file
    file_path = os.path.join(output_folder, file_name)

    # save datalake
    with open(file_path, 'wb') as f:
        pickle.dump(data_lake, f)


def load_datalake(path: str) -> object:
    """
    Load datalake object from a pickle file.

    Parameters
    ----------
    path : str
        Path to load pickle file.

    Returns
    -------
    data_lake : DataLake
        Loaded DataLake object.
    """
    # path to load pickle file
    file_path = os.path.join(path, 'output', 'datalake.pk')

    # load datalake
    with open(file_path, 'rb') as f:
        data_lake = pickle.load(f)

    return data_lake


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


