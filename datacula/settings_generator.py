"""Callable to generate settings file from template."""

from typing import Dict, List, Union


def for_general_1d_load(
        relative_data_folder: str = 'instrument_data',
        filename_regex: str = '*.csv',
        file_MIN_SIZE_BYTES: str = 10,
        data_checks: Dict[str, Union[int, Dict[str, int]]] =
        {
            "characters": [10, 100],
            "char_counts": {",": 4, ":": 0},
            "skip_rows": 0,
            "skip_end": 0,
        },
        data_column: List[int] = [3, 5],
        data_header: List[str] = ['data 1', 'data 3'],
        time_column: List[int] = [0, 1],
        time_format: str = '%Y-%m-%d %H:%M:%S.%f',
        delimiter: str = ',',
        Time_shift_seconds: int = 0,
        timezone_identifier: str = 'UTC',
) -> Dict:
    """Generate settings file for 1d general file."""

    # combine into settings dictionary
    settings = {
        'relative_data_folder': relative_data_folder,
        'filename_regex': filename_regex,
        'MIN_SIZE_BYTES': file_MIN_SIZE_BYTES,
        'data_loading_function': 'general_1d_load',
        'data_checks': data_checks,
        'data_column': data_column,
        'data_header': data_header,
        'time_column': time_column,
        'time_format': time_format,
        'delimiter': delimiter,
        'Time_shift_seconds': Time_shift_seconds,
        'timezone_identifier': timezone_identifier,
    }
    return settings