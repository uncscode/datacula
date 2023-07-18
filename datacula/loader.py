"""File readers and loaders for datacula."""
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
# pylint: disable=too-many-branches
# noaq: C901
from typing import List, Union, Tuple, Dict, Any

import warnings
import glob
import os
import pickle
from datetime import datetime
import numpy as np
import pandas as pd
from datacula import convert

FILTER_WARNING_FRACTION = 0.5


def data_raw_loader(file_path: str) -> list:
    """
    Load raw data from a file at the specified file path and return it as a
    list of strings.

    Parameters:
        file_path (str): The file path of the file to read.

    Returns:
        list: The raw data read from the file as a list of strings.

    Examples:
        >>> data = data_raw_loader('my_file.txt')
        Loading data from: my_file.txt
        >>> print(data)
        ['line 1', 'line 2', 'line 3']
    """
    try:
        with open(file_path, 'r', encoding='utf8', errors='replace') as file:
            data = [line.rstrip() for line in file]
        print('Loading data from:', os.path.split(file_path)[-1])
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        data = []
    return data


def filter_list(data: List[str], char_counts: dict) -> List[str]:
    """
    A pass filter of rows from a list of strings.
    Each row must contain a specified number of characters to pass the filter.
    The number of characters to count is specified in the char_counts
    dictionary. The keys are the characters to count, and the values are the
    exact count required for each character in each row.

    Parameters:
    ----------
        data (List[str]): A list of strings to filter.
            A list of strings to filter.
        char_counts (dict): A dictionary of character counts to select by.
            The keys are the characters to count, and the values are the
            count required for each character.

    Returns:
    ----------
        List[str]: A new list of strings containing only the rows that meet the
        character count requirements.

    Raises:
    ----------
        UserWarning: If more than 90% of the rows are filtered out, and it
            includes the character(s) used in the filter.

    Examples:
    ----------
        >>> data = ['apple,banana,orange', 'pear,kiwi,plum',
                    'grapefruit,lemon']
        >>> char_counts = {',': 2}
        >>> filtered_data = filter_rows_by_count(data, char_counts)
        >>> print(filtered_data)
        ['apple,banana,orange', 'pear,kiwi,plum']
    """
    filtered_data = data
    for char, count in char_counts.items():
        if count > -1:
            filtered_data = [
                row for row in filtered_data if row.count(char) == count]
        if len(filtered_data) / len(data) < FILTER_WARNING_FRACTION:
            warnings.warn(
                f"More than {FILTER_WARNING_FRACTION} of the rows have " +
                f"been filtered out based on the character: {char}.")
    return filtered_data


def data_format_checks(data: List[str], data_checks: dict) -> List[str]:
    """
    Check if the data is in the correct format.

    Args:
        data (List[str]): A list of strings containing the raw data.
        data_checks (dict): Dictionary containing the format checks.

    Returns:
        List[str]: A list of strings containing the formatted data.

    Raises:
        TypeError: If data is not a list.

    Examples:
        >>> data = ['row 1', 'row 2', 'row 3']
        >>> data_checks = {
        ...     "characters": [0, 10],
        ...     "char_counts": {",": 2, "/": 0, ":": 0},
        ...     "skip_rows": 0,
        ...     "skip_end": 0
        ... }
        >>> formatted_data = data_format_checks(data, data_checks)
        >>> print(formatted_data)
        ['row 2']
    """
    if not isinstance(data, list):
        raise TypeError("data must be a list")
    length_initial = len(data)
    if data_checks.get('skip_rows', 0) > 0:
        data = data[data_checks['skip_rows']:]
    if data_checks.get('skip_end', 0) > 0:
        data = data[:-data_checks['skip_end']]
    if len(data_checks.get('characters', [])) == 1:
        # Filter out any rows with fewer than the specified number of
        # characters.
        data = [
                x for x in data
                if (
                    len(x)
                    > data_checks['characters'][0]
                    )
                ]
    elif len(data_checks.get('characters', [])) == 2:
        # Filter out any rows with fewer than the minimum or more than the
        # maximum number of characters.
        data = [
                x for x in data
                if (
                    data_checks['characters'][0]
                    < len(x)
                    < data_checks['characters'][1]
                    )
                ]

    if len(data) / length_initial < FILTER_WARNING_FRACTION:
        warnings.warn(
            f"More than {FILTER_WARNING_FRACTION} of the rows have " +
            'been filtered out based on the characters limit ' +
            f"{data_checks['characters']} or skip rows.")

    if 'char_counts' in data_checks:
        char_counts = data_checks.get('char_counts', {})
        data = filter_list(data, char_counts)
    # Strip any leading or trailing whitespace from the rows.
    data = [x.strip() for x in data]
    return data


def parse_time_column(
            time_column: Union[int, List[int]],
            time_format: str,
            line: str,
            date_offset: str = None,
            seconds_shift: int = 0
        ) -> float:
    """
    Parses the time column of a data line and returns it as a timestamp.

    Parameters:
    ----------
    time_column : Union[int, List[int]]
        The index or indices of the column(s) containing the time information.
    time_format : str
        The format of the time information, e.g. '%Y-%m-%d %H:%M:%S'.
    line : str
        The data line to parse.
    date_offset : Optional[str], default=None
        A fixed date offset to add to the timestamp in front.
    seconds_shift : int, default=0
        A number of seconds to add to the timestamp.

    Returns:
    -------
    float
        The timestamp corresponding to the time information in the data line,
        in seconds since the epoch.

    Raises:
    ------
    ValueError
        If an invalid time column or format is specified.
    """
    if time_format == 'epoch':
        # if the time is in epoch format
        return float(line[time_column]) + seconds_shift
    if isinstance(time_column, int):
        # if the time and date are in one column
        return datetime.strptime(
                                    line[time_column],
                                    time_format
                                ).timestamp() + seconds_shift
    if isinstance(time_column, list) and len(time_column) == 2:
        # if the time and date are in two column
        time_str = f"{line[time_column[0]]} {line[time_column[1]]}"
        return datetime.strptime(
                                    time_str,
                                    time_format
                                ).timestamp() + seconds_shift
    if date_offset:
        # if the time is in one column, and the date is fixed
        time_str = f"{date_offset} {line[time_column]}"
        return datetime.strptime(
                                    time_str,
                                    time_format
                                ).timestamp() + seconds_shift
    raise ValueError(
        f"Invalid time column or format: {time_column}, {time_format}")


def sample_data(
            data: list,
            time_column: int,
            time_format: str,
            data_columns: list,
            delimiter: str,
            date_offset: str = None,
            seconds_shift: int = 0,
        ) -> Tuple[np.array, np.array]:
    """
    Samples the data to get the time and data streams.
    TODO: revise this function
    """
    epoc_time = np.zeros(len(data))
    data_array = np.zeros((len(data), len(data_columns)))

    for i, line in enumerate(data):
        line_array = line.split(delimiter)  # split the line into an array

        epoc_time[i] = parse_time_column(
                time_column=time_column,
                time_format=time_format,
                line=line_array,
                date_offset=date_offset,
                seconds_shift=seconds_shift
        )

        for j, col in enumerate(data_columns):
            if col < len(line_array):
                value = line_array[col].strip()
            else:
                value = ''

            if value == '':  # no data
                data_array[i, j] = np.nan
            elif value.count('�') > 0:
                data_array[i, j] = np.nan
            elif value[0].isnumeric():  # if the first character is a number
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
                        '-99999', '-9999'
                    ]

                if value in true_match:
                    data_array[i, j] = 1
                elif value in false_match:
                    data_array[i, j] = 0
                elif value in nan_match:
                    data_array[i, j] = np.nan
                else:
                    raise ValueError(
                        'No DATA match, true or false or nan: ' + value)
            else:
                raise ValueError('DATA NOT READING IN CORRECTLY:', value)
    return epoc_time, data_array


def general_data_formatter(
    data: list,
    data_checks: dict,
    data_column: list,
    time_column: Union[int, List[int]],
    time_format: str,
    delimiter: str = ',',
    date_offset: str = None,
    seconds_shift: int = 0,
) -> Tuple[np.array, np.array]:
    """
    Formats and samples the data to get the time and data streams.

    Parameters:
    ----------
    data : list
        The list of strings containing the data.
    data_checks : dict
        A dictionary of data format checks to apply to the data.
    data_column : list
        The list of indices of the columns containing the data.
    time_column : Union[int, List[int]]
        The index or indices of the column(s) containing the time information.
    time_format : str
        The format of the time information, e.g. '%Y-%m-%d %H:%M:%S'.
    delimiter : str, default=','
        The delimiter used to separate columns in the data.
    date_offset : str, default=None
        A fixed date offset to add to the timestamp in front.
    seconds_shift : int, default=0
        A number of seconds to add to the timestamp.

    Returns:
    -------
    Tuple[np.array, np.array]
        A tuple containing two np.array objects: the first contains the
        epoch times, and the second contains the data.
    """
    # Check the data format
    data = data_format_checks(data, data_checks)

    # Sample the data to get the epoch times and the data
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
            data: List[str],
            data_checks: Dict[str, Any],
            data_sizer_reader: Dict[str, str],
            time_column: int,
            time_format: str,
            delimiter: str = ',',
            date_offset: str = None
        ) -> Tuple[np.ndarray, List[str], np.ndarray, np.ndarray]:
    """
    Formats data from a particle sizer.

    Parameters
    ----------
    data : List[str]
        The data to be formatted.
    data_checks : Dict[str, Any]
        Dictionary specifying the formatting requirements for the data.
    data_sizer_reader : Dict[str, str]
        Dictionary containing information about the sizer data format.
    time_column : int
        The index of the time column in the data.
    time_format : str
        The format of the time information.
    delimiter : str, default=','
        The delimiter used in the data.
    date_offset : str, default=None
        The date offset to add to the timestamp.

    Returns
    -------
    Tuple[np.ndarray, List(str) np.ndarray, np.ndarray]
        A tuple containing the epoch time, the Dp header, and the data arrays.
    """

    # Get Dp range and columns
    data_header = data[data_sizer_reader["header_rows"]].split(delimiter)
    dp_range = [
                data_header.index(data_sizer_reader["Dp_start_keyword"]),
                data_header.index(data_sizer_reader["Dp_end_keyword"])
                ]
    dp_columns = list(range(dp_range[0]+1, dp_range[1]))
    dp_header = list([data_header[i] for i in dp_columns])
    # change from np.array

    # Get data columns
    data_column = [
        data_header.index(x) for x in data_sizer_reader["list_of_data_headers"]
        ]

    # Format data
    data = data_format_checks(data, data_checks)

    # Get data arrays
    epoch_time, data_smps_2d = sample_data(
        data,
        time_column,
        time_format,
        dp_columns,
        delimiter,
        date_offset
    )
    epoch_time, data_smps_1d = sample_data(
        data,
        time_column,
        time_format,
        data_column,
        delimiter,
        date_offset
    )

    if "convert_scale_from" in data_sizer_reader.keys():
        if data_sizer_reader["convert_scale_from"] == "dw":
            inverse = True
        elif data_sizer_reader["convert_scale_from"] == "dw/dlogdp":
            inverse = False
        else:
            raise ValueError(
                "Invalid value for convert_scale_from in data_sizer_reader. "+\
                "Either dw/dlogdp or dw must be specified."
            )
        for i in range(len(epoch_time)):
            data_smps_2d[i,:] = convert.convert_sizer_dn(
                diameter=np.array(dp_header).astype(float),
                dn_dlogdp=data_smps_2d[i,:],
                inverse=inverse
            )

    return epoch_time, dp_header, data_smps_2d, data_smps_1d


def non_standard_date_location(
            data: list,
            date_location: dict
        ) -> str:
    """
    Extracts the date from a non-standard location in the data.

    Parameters:
    ----------
    data : list
        A list of strings representing the data.
    date_location : dict
        A dictionary specifying the method for extracting the date from the
        data.
        Supported methods include:
            - 'file_header_block': The date is located in the file header
                block, and its position is specified by the 'row',
                'delimiter', and 'index' keys.

    Returns:
    -------
    str
        The date extracted from the specified location in the data.

    Raises:
    ------
    ValueError
        If an unsupported or invalid method is specified in date_location.
    """
    if date_location['method'] == 'file_header_block':
        row_index = date_location['row']
        delimiter = date_location['delimiter']
        index = date_location['index']
        date = data[row_index].split(delimiter)[index].strip()
    else:
        raise ValueError('Invalid date location method specified')

    return date


def get_files_in_folder_with_size(
    path: str,
    subfolder: str,
    filename_regex: str,
    min_size: int = 10,
) -> Tuple[List[str], List[str], List[int]]:
    """
    Returns a list of files in the specified folder and subfolder that
    match the given filename pattern and have a size greater than the
    specified minimum size.

    Parameters:
    ----------
    path : str
        The path to the parent folder.
    subfolder : str
        The name of the subfolder containing the files.
    filename_regex : str
        A regular expression pattern for matching the filenames.
    min_size : int, optional
        The minimum file size in bytes (default is 10).

    Returns:
    -------
    Tuple[List[str], List[str], List[int]]
        A tuple containing three lists:
        - The filenames that match the pattern and size criteria
        - The full paths to the files
        - The file sizes in bytes
    """
    search_path = os.path.join(path, subfolder)

    if not os.path.isdir(search_path):
        raise ValueError(f"{search_path} is not a directory")

    file_list = glob.glob(os.path.join(search_path, filename_regex))
    # os.chdir(search_path) # this should not be needed
    # file_list = glob.glob(filename_regex)
    # os.chdir(original_path)

    # filter the files by size
    file_list = [
        file for file in file_list
        if os.path.getsize(os.path.join(search_path, file)) > min_size
    ]

    full_path = [os.path.join(search_path, file) for file in file_list]
    file_size_in_bytes = [os.path.getsize(path) for path in full_path]

    return file_list, full_path, file_size_in_bytes


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
    with open(file_path, 'wb') as file:
        pickle.dump(data_lake, file)


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
    with open(file_path, 'rb') as file:
        data_lake = pickle.load(file)

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
    time = convert.datetime64_from_epoch_array(
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
    Function to save a datalake to a csv file. Iterates through the
    datastreams, or just the keys specified.

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
