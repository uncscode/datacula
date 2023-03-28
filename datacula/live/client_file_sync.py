"""File syncing script for a client to copy local files to a network drive.

Writen to work independently of the rest of the datacula code base and run on
older versions of python.
"""
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import time
import os
import json
import file_sync
import time
import logging
import datetime
from scheduler import Scheduler

schedule = Scheduler()

logging.basicConfig(level=logging.INFO)
script_path = os.path.dirname(__file__)


def file_sync_settings_dict_creator() -> dict:
    """
    Creates a blank dictionary for client sync options.
    """
    file_sync_settings_dict = {
        "blank_data": {
            "core_folder": script_path,
            "clone_folder": script_path,
            "files_core_search_key": '*.*',
            "files_clone_search_key": '*.*',
            "max_file_age_days": 30,
        },
        "blank_data2": {
            "core_folder": script_path,
            "clone_folder": script_path,
            "files_core_search_key": '*.*',
            "files_clone_search_key": '*.*',
            "max_file_age_days": 30,
        },
    }
    return file_sync_settings_dict


def write_file_sync_settings(
            sync_settings: dict
        ) -> None:
    """
    Writes server data settings to json file that is located in the same folder 
    as this script.
    """
    with open(
                os.path.join(script_path, 'file_sync_settings.json'),
                'w',
                encoding='utf-8'
            ) as json_file:

        json.dump(sync_settings, json_file, indent=4)


def get_file_sync_settings(mode: str = 'automatic') -> dict:
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
            with open(
                        os.path.join(script_path, 'file_sync_settings.json'),
                        'r',
                        encoding='utf-8'
                    ) as json_file:
                file_sync_settings = json.load(json_file)

        except FileNotFoundError:
            file_sync_settings = file_sync_settings_dict_creator()
            write_file_sync_settings(file_sync_settings)
    elif mode == 'new':
        file_sync_settings = file_sync_settings_dict_creator()
        write_file_sync_settings(file_sync_settings)
    return file_sync_settings


# Run main function
def main():
    """
    file_sync main settings.
    """
    file_sync_settings = get_file_sync_settings()

    for file_prop in file_sync_settings:
        print('Checking '+file_prop)
        file_sync.copy_files(
            core_directory=file_sync_settings[file_prop][
                'core_folder'],
            clone_directory=file_sync_settings[file_prop][
                'clone_folder'],
            core_search_key=file_sync_settings[file_prop][
                'files_core_search_key'],
            clone_search_key=file_sync_settings[file_prop][
                'files_clone_search_key'],
            max_file_age_days=file_sync_settings[file_prop][
                'max_file_age_days'],
            )


def run_file_sync():
    try:
        logging.info('Running file sync at %s', datetime.datetime.now())
        main()
    except PermissionError as e:
        logging.error('Permission error: %s', str(e))
    except Exception as e:
        logging.error('File sync error: %s', str(e))


def main():
    file_sync_settings = get_file_sync_settings()
    for prop_name, prop_data in file_sync_settings.items():
        logging.info('Checking %s', prop_name)
        file_sync.copy_files(
            core_directory=prop_data['core_folder'],
            clone_directory=prop_data['clone_folder'],
            core_search_key=prop_data['files_core_search_key'],
            clone_search_key=prop_data['files_clone_search_key'],
            max_file_age_days=prop_data['max_file_age_days'],
        )


if __name__ == '__main__':
    file_sync_settings = get_file_sync_settings()
    if list(file_sync_settings.keys())[0] == 'blank_data':
        print('Created the parameter file, you need to edit it and re-run')
    else:
        logging.info('Startup check')
        main()
        # https://digon.io/hyd/project/scheduler/t/master/pages/examples/quick_start.html
        # Schedule file sync to run every two hours between 2am and 10pm
        schedule.every(2).hours.at('XX:05').do(run_file_sync)
        # schedule.every(2).hours.at('04:00').do(run_file_sync)
        # schedule.every(2).hours.at('06:00').do(run_file_sync)
        # schedule.every(2).hours.at('08:00').do(run_file_sync)
        # schedule.every(2).hours.at('10:00').do(run_file_sync)
        # schedule.every(2).hours.at('12:00').do(run_file_sync)
        # schedule.every(2).hours.at('14:00').do(run_file_sync)
        # schedule.every(2).hours.at('16:00').do(run_file_sync)
        # schedule.every(2).hours.at('18:00').do(run_file_sync)
        # schedule.every(2).hours.at('20:00').do(run_file_sync)




# if __name__ == '__main__':
#     file_sync_settings = get_file_sync_settings()

#     if list(file_sync_settings.keys())[0] != 'blank_data':
#         print('Startup Check')
#         main()
#         while True:
#             if any(x==time.localtime()[3] for x in [2,4,6,8,10,12,14,16,18,20,22]):
#                 time.sleep(30)# delay offset
#                 try:
#                     print('running file sync at '+time.ctime())
#                     main()
#                     time.sleep(3600) # sleep for an hour
#                 except:
#                     print('[****file sync error, trying again in 15 sec]')
#                     time.sleep(15)
#             else:
#                 time.sleep(15) # sleep between hour checks
#     else:
#         print('Created the parameter file, you need to edit it and re-run')


# %%
