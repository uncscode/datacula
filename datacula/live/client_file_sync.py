#%%

# -*- coding: utf-8 -*-
"""
Created by Kyle Gorkowski
"""
# import socket
from distutils import core
import time
# import glob
import os
import json
import file_sync

script_path = os.path.dirname(__file__)
# print(script_path)

def file_sync_settings_dict_creator() -> dict:
    """
    Creates a blank dictionary for client sync options.
    """
    file_sync_settings_dict = {
        "blank_data": {
            "core_folder": script_path,
            "clone_folder": script_path,
            "files_core_search_key":'*.*',
            "files_clone_search_key":'*.*', 
            "max_file_age_days": 30,
        },
        "blank_data2": {
            "core_folder": script_path,
            "clone_folder": script_path,
            "files_core_search_key":'*.*',
            "files_clone_search_key":'*.*', 
            "max_file_age_days": 30,
        },
    }
    return file_sync_settings_dict


def write_file_sync_settings(file_sync_settings :dict ) -> None:
    """
    Writes server data settings to json file that is located in the same folder 
    as this script.
    """
    with open(os.path.join(script_path,'file_sync_settings.json'), 'w',
        encoding='utf-8'
    ) as json_file:
        json.dump(file_sync_settings, json_file, indent=4)


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
            with open(os.path.join(script_path,'file_sync_settings.json'), 
            'r', encoding='utf-8'
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
        # print(file_sync_settings[file_prop]['core_folder'])
        print('Checking '+file_prop)
        file_sync.copy_files(
            core_directory=file_sync_settings[file_prop]['core_folder'],
            clone_directory=file_sync_settings[file_prop]['clone_folder'],
            core_search_key=file_sync_settings[file_prop]['files_core_search_key'],
            clone_search_key=file_sync_settings[file_prop]['files_clone_search_key'],
            max_file_age_days=file_sync_settings[file_prop]['max_file_age_days'],
            )

#%%
if __name__ == '__main__':
    file_sync_settings = get_file_sync_settings()
    if list(file_sync_settings.keys())[0] != 'blank_data':
        print('Startup Check')
        main()
        while True:
            if any(x==time.localtime()[3] for x in [2,4,6,8,10,12,14,16,18,20,22]):
                time.sleep(30)# delay offset
                try:
                    print('running file sync at '+time.ctime())
                    main()
                    time.sleep(3600) # sleep for an hour
                except:
                    print('[****file sync error, trying again in 15 sec]')
                    time.sleep(15)
            else:
                time.sleep(15) # sleep between hour checks
    else:
        print('Created the parameter file, you need to edit it and re-run')


# %%
