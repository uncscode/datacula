"""Major clean up needed
"""
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import glob, time, json, os, pickle
import numpy as np
import pandas as pd
from datetime import datetime

script_path = os.path.dirname(os.path.abspath('server_data.py'))


# timestamps_shift_from_UTC_sec = {'CDT_timestamp': 60*60*5, 'CST_timestamp': 60*60*6}


# %%
def server_data_settings_dict_creator() -> dict:
    """
    Creates a blank dictionary of all server data settings.
    """
    # default_instruments = ['SP2_data', 'NO2_data', 'PASS3_data', 'picarro_data',
    # 'N2O_data', 'CHOH_aeris_data', 'PAX_data', 'CPC_3010_data', 'CPC_3022_data',
    # 'NOx_data', 'CH4_aeris_data', 'anemometer_data', 'SPAMS_data',
    # 'CAPS_dual_data','CCNc_data']

    # blank_instrument_dict = {'instrument_name': '',
    #                             'data_processing_function': '',
    #                             'relative_data_folder': '',
    #                             'last_file_processed': '',
    #                             'last_timestamp_processed': '',
    #                             'skipRowsDict': 0,
    #                         }

    # # create blank dictionary
    # server_data_settings_dict = {}
    # for values in default_instruments:
    #     server_data_settings_dict[values] = blank_instrument_dict

    # TODO: add data headers to server_data_settings_dict
    server_data_settings_dict = {
        "SP2_data": {
            "instrument_name": "SP2",
            "data_processing_function": "sp2_load",
            "relative_data_folder": "SP2_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_to_Linux_Epoch_sec":18000
        },
        "NO2_data": {
            "instrument_name": "teledyne_no2",
            "data_processing_function": "teledyne_no2_load",
            "relative_data_folder": "NO2_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":0
        },
        "PASS3_data": {
            "instrument_name": "PASS3",
            "data_processing_function": "pass3_load",
            "relative_data_folder": "PASS3_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":18000
        },
        "picarro_data": {
            "instrument_name": "Picarro_GHG",
            "data_processing_function": "picarro_load",
            "relative_data_folder": "picarro_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":18000
        },
        "N2O_data": {
            "instrument_name": "Aries_N20",
            "data_processing_function": "aeries_n2o_load",
            "relative_data_folder": "N2O_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":0
        },
        "CHOH_aeris_data": {
            "instrument_name": "Aries_CHOH",
            "data_processing_function": "aeries_choh_load",
            "relative_data_folder": "CHOH_aeris_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":0
        },
        "PAX_data": {
            "instrument_name": "PAX",
            "data_processing_function": "pax_load",
            "relative_data_folder": "PAX_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":18000
        },
        "CPC_3010_data": {
            "instrument_name": "CPC_3010_a",
            "data_processing_function": "cpc_3010_load",
            "relative_data_folder": "CPC_3010_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":0
        },
        "CPC_3022_data": {
            "instrument_name": "CPC_3022",
            "data_processing_function": "cpc_3022_load",
            "relative_data_folder": "CPC_3022_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":0
        },
        "NOx_data": {
            "instrument_name": "Tech2B_NOx",
            "data_processing_function": "tech2b_nox_load",
            "relative_data_folder": "NOx_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0
        },
        "CH4_aeris_data": {
            "instrument_name": "Aries_CH4",
            "data_processing_function": "aries_ch4_load",
            "relative_data_folder": "CH4_aeris_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":0
        },
        "anemometer_data": {
            "instrument_name": "Spherical_Anemometer",
            "data_processing_function": "spherical_anemometer_load",
            "relative_data_folder": "anemometer_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":0
        },
        "SPAMS_data": {
            "instrument_name": "LANL_SPAMS",
            "data_processing_function": "",
            "relative_data_folder": "SPAMS_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":18000
        },
        "CAPS_dual_data": {
            "instrument_name": "LANL_CAPS_dual",
            "data_processing_function": "",
            "relative_data_folder": "CAPS_dual_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":18000
        },
        "CCNc": {
            "instrument_name": "CCNc",
            "data_processing_function": "",
            "relative_data_folder": "CCNc_data",
            "last_file_processed": "",
            "last_timestamp_processed": "",
            "skipRowsDict": 0,
            "Time_shift_to_Linux_Epoch_sec":18000
        }
    }
    return server_data_settings_dict


def write_server_data_settings(server_data_settings: dict ) -> None:
    """
    Writes server data settings to json file that is located in the same
    folder as this script.
    """
    with open(os.path.join(script_path,'server_data_settings.json'), 'w',
        encoding='utf-8'
    ) as json_file:
        json.dump(server_data_settings, json_file, indent=4)


def get_server_data_settings(mode: str = 'automatic') -> dict:
    """
    Reads server data settings from json file that is located in the same 
    folder as this script.
    If automatic, the script will look for the file and if it does not exist,
    it will create it. If new, the script will write/overwrite a new file.

    Parameters:
        mode (str): automatic or new.
    Returns:
        dict: server data settings.
    """
    if mode == 'automatic':
        try:
            with open(os.path.join(script_path, 'server_data_settings.json'), 
            'r', encoding='utf-8'
            ) as json_file:
                server_data_settings = json.load(json_file)
        except FileNotFoundError:
            server_data_settings = server_data_settings_dict_creator()
            write_server_data_settings(server_data_settings)
    elif mode == 'new':
            server_data_settings = server_data_settings_dict_creator()
            write_server_data_settings(server_data_settings)
    return server_data_settings


def folder_checker() -> None:
    """
    Checks if the folders for the data are present. If not, it creates them.
    """
    server_data_settings_current = get_server_data_settings()
    for instrument in server_data_settings_current.keys():
        # up one directory from the code path 
        # and into the server_files directory
        if not os.path.exists(
            os.path.join(
                script_path, os.pardir,
                'server_files',
                server_data_settings_current[
                    instrument]['relative_data_folder']
                )
                ):
            os.makedirs(
                os.path.join(
                    script_path, os.pardir, 'server_files',
                    server_data_settings_current[
                        instrument]['relative_data_folder']
                    )
                )

