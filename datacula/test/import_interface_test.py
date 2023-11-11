"""test for import_interface.py"""

import datacula.import_interface as import_interface
from datacula.stream import Stream


def generate_files(
        file_count=10,
):
    """generate temp files for testing"""
    import tempfile
    import os
    import numpy as np
    import random
    import string

    # create temp folder
    temp_folder = tempfile.mkdtemp(prefix='interfacetest_')
    # create a subfolder
    subfolder_name = 'subfolder'
    subfolder_path = os.path.join(temp_folder, subfolder_name)
    os.mkdir(subfolder_path)

    size = 1000
    # create temp files
    file_names = []
    file_size = []
    for i in range(file_count):
        file_name = ''.join(random.choices(string.ascii_uppercase +
                                           string.digits, k=10))
        file_names.append(file_name)
        file_size.append(size)
        #subfolder1
        file_path = os.path.join(subfolder_path, file_name)
        with open(file_path, 'wb') as f:
            f.write(np.zeros(size, dtype=np.uint8))

    return subfolder_name, temp_folder, file_size, file_names


def delete_temp_files(temp_folder):
    import shutil
    shutil.rmtree(temp_folder)


def test_get_new_files():
    """test for get_new_files function"""

    # gerneate temp files
    subfolder_name, temp_folder, file_size, file_names = generate_files()

    settings = {
        'relative_data_folder': subfolder_name,
        'filename_regex': '*',
        'MIN_SIZE_BYTES': 10,
    }
    # load files location
    full_paths, first_pass, file_info = import_interface.get_new_files(
        path=temp_folder,
        import_settings=settings,
    )
    # check if the files are loaded
    assert first_pass == True
    assert len(full_paths) == len(file_names)
    assert len(file_info) == len(file_names)


    # remove a file from the info list and re-run, to check if the file loads
    last_file = file_info[-1]
    file_info.pop()
    full_paths, first_pass, file_info = import_interface.get_new_files(
        path=temp_folder,
        import_settings=settings,
        loaded_list=file_info,
    )

    assert first_pass == False
    assert file_info[0] == last_file

    # delete temp files
    delete_temp_files(temp_folder)

    # signle file test
    # create temp folder
    subfolder_name, temp_folder, file_size, file_names = generate_files(
        file_count=1)
    
    full_paths, first_pass, file_info = import_interface.get_new_files(
        path=temp_folder,
        import_settings=settings,
    )

    assert first_pass == True
    assert len(full_paths) == len(file_names)
    assert len(file_info) == len(file_names)

    # retest with no new files

    full_paths, first_pass, file_info = import_interface.get_new_files(
        path=temp_folder,
        import_settings=settings,
        loaded_list=file_info,
    )

    assert first_pass == False
    assert len(full_paths) == 0
    assert len(file_info) == 0
