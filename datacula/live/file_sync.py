# -*- coding: utf-8 -*-
"""
Created on July 16 2022
Copies data from core folder (or path) to clone path (or folder)
Methods:
    copy_files(core_directory:str, clone_directory:str,
        core_search_key:str = '*.*', clone_search_key:str = '*.*',
        )->None
    copy_folders(core_path:str, clone_path:str,
        core_search_key:str = '*', clone_search_key:str = '*',)->None

@author: Kyle Gorkowski
"""
import glob
import os
import platform
import time

if platform.system() == 'Windows':
    system_copy = 'copy'
else:
    system_copy = 'cp'


def copy_files(core_directory:str, clone_directory:str,
    core_search_key:str = '*.*', clone_search_key:str = '*.*',
    max_file_age_days:float = -1,
    )->None:
    """
    Copies file from the core directory to the clone directory, with a check 
    for file size. If the file size is the same, the file is not copied. 
    If the file size is different, the file is copied. If the file does not 
    exist in the clone directory, it is copied.

    Parameters:
        core_dir (str): The directory to copy files from.
        clone_dir (str): The directory to copy files to.
        core_search_key (str): glob.glob search key to use
        clone_search_key (str): glob.glob search key to use
    """
    if os.path.exists(core_directory):
        core_dir_files = glob.glob(os.path.join(core_directory,core_search_key))
    else:
        print('******Core directory does not exist******')

    if os.path.exists(clone_directory):
        clone_dir_files = glob.glob(
            os.path.join(clone_directory, clone_search_key)
            )
    else:
        os.makedirs(clone_directory)
        clone_dir_files = glob.glob(
            os.path.join(clone_directory, clone_search_key)
            )

    max_file_age_sec = max_file_age_days * 86400
    #loop through all files in core_dir and check if they exist in clone_dir
    for file in core_dir_files:
        file_name = os.path.basename(file)

        copy = True
        #checks if the file is too old
        if os.path.getctime(file)+max_file_age_sec > time.time() or max_file_age_days < 0 :

            for file_clone_check in clone_dir_files:
                file_clone_name = os.path.basename(file_clone_check)
                if file_name == file_clone_name:
                    #file exists in clone_dir, check size
                    if os.path.getsize(file) == os.path.getsize(file_clone_check):
                        copy = False
                        clone_dir_files.remove(file_clone_check) #removes file from list
                    break
            if copy:
                os.system(
                    system_copy+ ' "'+file+ '"'+' '+ '"'+
                    os.path.join(clone_directory,file_name)+'"'
                    )
                print('    copied: ' + file_name)


# core_dir = 'F:\\OneDrive\\Professional Current DnD\\Aerosol microphysics\\CAFE-processing\\server\\server_files\\NOx_data'
# clone_dir = 'F:\\OneDrive\\Professional Current DnD\\Aerosol microphysics\\CAFE-processing\\server\\server_files_clone\\NOx_data'
# copy_files(core_dir, clone_dir)

#copy folders from core to clone
def copy_folders(core_path:str, clone_path:str,
    core_search_key:str = '*', clone_search_key:str = '*', 
    files_core_search_key:str = '*.*', files_clone_search_key:str = '*.*', 
    max_file_age_days:float = -1,
    )->None:
    """
    Copies folders from the core directory to the clone directory, with a check
    for files size. If the file size is the same, the file is not copied.
    
    Parameters:
        core_path (str): The directory to copy folders from.
        clone_path (str): The directory to copy folders to.
        core_search_key (str): glob.glob search key to use
        clone_search_key (str): glob.glob search key to use
    """

    if os.path.exists(core_path):
        core_path_folders = glob.glob(os.path.join(core_path,core_search_key))
    else:
        print('******Core path does not exist******')

    for folder in core_path_folders:
        folder_name = os.path.basename(folder)
        print('Compared folder: ' + folder_name)

        copy_files(folder, os.path.join(clone_path, folder_name),
        core_search_key = files_core_search_key, clone_search_key = files_clone_search_key,
        max_file_age_days = max_file_age_days)


# # %%

# core_path = 'F:\\OneDrive\\Professional Current DnD\\Aerosol microphysics\\CAFE-processing\\server\\server_files'
# clone_path = 'F:\\OneDrive\\Professional Current DnD\\Aerosol microphysics\\CAFE-processing\\server\\server_files_clone'

# copy_folders(core_path, clone_path)
