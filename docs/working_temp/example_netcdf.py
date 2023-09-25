# %%
import warnings
import numpy as np
import netCDF4 as nc
import datetime
import datacula.convert as convert

from typing import List, Union, Tuple, Dict, Any
# %%

file_path = "E:\\Tracer\\working_folder\\raw_data\\ARM_aos_aps\\houaosapsM1.b1.20220627.145726.nc"

# %%
instrument_settings = {"ARM_aps": {
        "instrument_name": "AOS_aps_data",
        "data_stream_name": ["aos_aps_1D", "aos_aps_2D"],
        "data_processing_function": "SMPS_processing",
        "data_loading_function": "netcdf_2d_sizer_load",
        "relative_data_folder": "ARM_aos_aps",
        "last_file_processed": "",
        "last_timestamp_processed": "",
        "Time_shift_to_Linux_Epoch_sec": 0,
        "netcdf_reader": {
            "data_2d": "dN_dlogDp",
            "header_2d": 'diameter_aerodynamic',
            "data_1d": ['total_N_conc',
                        'total_SA_conc'],
            "header_1d": ['concentration_N_cm3',
                          'surface_area_concentration_nm_cm3'],
            },
        "time_column": ['base_time', 'time_offset'],
        "time_format": "epoch",
        "filename_regex": "*.nc",
        "base_interval_sec": 1,
        "data_delimiter": ","
        }
    }
# %%
settings = instrument_settings['ARM_aps']





# %% test if function works

# epoch_time = netcdf_get_epoch_time(file_path, settings)

# # data_1d

# epoch_time, header_1d, data_1d = netcdf_data_1d_load(file_path, settings)

# # data_2d

# epoch_time, header_2d, data_2d = netcdf_data_2d_load(file_path, settings)


# %%
