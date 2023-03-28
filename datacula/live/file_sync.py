"""
Copies data from core folder (or path) to clone path (or folder)
    Methods: copy_files, copy_folders
"""
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import glob
import os
import time


def copy_files(
            core_directory: str,
            clone_directory: str,
            core_search_key: str = '*.*',
            clone_search_key: str = '*.*',
            max_file_age_days: float = -1
        ) -> None:
    """
    Copies files from the core directory to the clone directory, with a check
    for file size. If the file size is the same, the file is not copied.
    If the file size is different, the file is copied. If the file does not
    exist in the clone directory, it is copied.

    Parameters:
        core_directory (str): The directory to copy files from.
        clone_directory (str): The directory to copy files to.
        core_search_key (str): glob.glob search key to use.
        clone_search_key (str): glob.glob search key to use.
        max_file_age_days (float): Maximum age of files to copy (in days).
    """
    if not os.path.exists(core_directory):
        print('******Core directory does not exist******')
        return
    print(f"    core path {core_directory}")
    print(f" -->clone path {clone_directory}")

    core_dir_files = glob.glob(os.path.join(core_directory, core_search_key))

    if os.path.exists(clone_directory):
        clone_dir_files = glob.glob(
            os.path.join(clone_directory, clone_search_key))
    else:
        os.makedirs(clone_directory)
        clone_dir_files = []

    max_file_age_sec = max_file_age_days * 86400
    system_copy = 'cp' if os.name == 'posix' else 'copy'

    # Loop through all files in core_dir and check if they exist in clone_dir
    for file in core_dir_files:
        file_name = os.path.basename(file)

        # Check if the file is too old or copy all files
        if max_file_age_days < 0 or (
                    os.path.getctime(file) + max_file_age_sec < time.time()
                ):
            continue  # skips to next loop iteration
        copy = True

        # Check if the file exists in clone_dir and has the same size
        for file_clone_check in clone_dir_files:
            file_clone_name = os.path.basename(file_clone_check)

            if file_name == file_clone_name:
                if os.path.getsize(file) == os.path.getsize(file_clone_check):
                    copy = False
                    clone_dir_files.remove(file_clone_check)  # Removes file
                break

        if copy:
            new_clone = os.path.join(clone_directory, file_name)
            os.system(f"{system_copy} '{file}' '{new_clone}'")

            print(f"    copied: {file_name}")


def copy_folders(
            core_path: str,
            clone_path: str,
            core_search_key: str = '*',
            files_core_search_key: str = '*.*',
            files_clone_search_key: str = '*.*',
            max_file_age_days: float = -1
        ) -> None:
    """
    Copies folders from the core directory to the clone directory, with a check
    for files size. If the file size is the same, the file is not copied.

    Parameters:
        core_path (str): The directory to copy folders from.
        clone_path (str): The directory to copy folders to.
        core_search_key (str): glob.glob search key to use for folders.
        files_core_search_key (str): glob.glob search key to use for files.
        files_clone_search_key (str): glob.glob search key to use for files.
        max_file_age_days (float): Maximum age of files to copy (in days).
    """

    if not os.path.exists(core_path):
        print('******Core path does not exist******')
        return

    core_path_folders = glob.glob(os.path.join(core_path, core_search_key))

    for folder in core_path_folders:
        folder_name = os.path.basename(folder)
        print('Compared folder: ' + folder_name)

        clone_folder = os.path.join(clone_path, folder_name)
        os.makedirs(clone_folder, exist_ok=True)

        copy_files(
            core_directory=folder,
            clone_directory=clone_folder,
            core_search_key=files_core_search_key,
            clone_search_key=files_clone_search_key,
            max_file_age_days=max_file_age_days
        )
