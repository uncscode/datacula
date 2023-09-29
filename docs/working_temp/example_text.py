# %%
import warnings
import numpy as np
import netCDF4 as nc
from datetime import datetime, timedelta

import pytz
from matplotlib import pyplot as plt, dates
import datacula.convert as convert
import datacula.loader as loader
from datacula.time_manage import time_str_to_epoch

from typing import List, Union, Tuple, Dict, Any

from datacula.lake.datalake import DataLake
from datacula.lake import processer, plot
# %%
file_fullpath = "D:\\Tracer\\working_folder\\raw_data\\ARM_laport_aod\\20220620_20220806_ARM_LaPorte.aod"


# file_path = "E:\\Tracer\\working_folder\\raw_data\\ARM_aos_aps\\houaosapsM1.b1.20220627.145726.nc"
file_path = "D:\\Tracer\\working_folder\\raw_data"
# %%
instrument_settings = {"ARM_laport_aod": {
        "instrument_name": "id_aeronet",
        "data_stream_name": "arm_aeronet",
        "data_loading_function": "general_load",
        "relative_data_folder": "ARM_laport_aod",
        "Time_shift_sec": 0,
        "timezone_identifier": "UTC",
        "data_checks": {
            "skip_rows": 8,
            "skip_end": 0,
            "char_counts": {":": 8}
        },
        "data_header": [
            "aod_extinction_440nm",
            "aod_extinction_675nm",
            "aod_extinction_870nm",
            "aod_extinction_1020nm",
            "fine_aod_extinction_440nm",
            "fine_aod_extinction_675nm",
            "fine_aod_extinction_870nm",
            "fine_aod_extinction_1020nm",
            "coarse_aod_extinction_440nm",
            "coarse_aod_extinction_675nm",
            "coarse_aod_extinction_870nm",
            "coarse_aod_extinction_1020nm"
        ],
        "data_column": [
            5, 6, 7, 8, 9, 10, 11, 12,
            13, 14, 15, 16
        ],
        "time_column": [1,2],
        "time_format": "%d:%m:%Y %H:%M:%S",
        "filename_regex": "*.aod",
        "base_interval_sec": 3600,
        "data_delimiter": ","
    }
}

# %% load into datalake


datalake = DataLake(instrument_settings, file_path)

datalake.update_datastream()
# datalake.remove_zeros()
datalake.info(header_print_count=15)


# %%
time_format = "%m/%d/%Y %H:%M:%S"
tracer_timezone = pytz.timezone('US/Central')
epoch_start = time_str_to_epoch('07/03/2022 00:00:00', time_format, 'US/Central')
epoch_end = time_str_to_epoch('07/28/2022 00:00:00', time_format, 'US/Central')
datalake.reaverage_datastreams(3600*10, epoch_start=epoch_start, epoch_end=epoch_end)




# %%

fig, ax = plt.subplots()
plot.timeseries(
    ax,
    datalake,
    "arm_aeronet",
    "aod_extinction_870nm",
    "arm",
    shade=True)

ax.minorticks_on()
plt.tick_params(rotation=-35)
# ax.set_ylabel("Particle Mass (ug/m3)")
# ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter("%m/%d", tz=tracer_timezone))
# ax.xaxis.set_minor_formatter(dates.DateFormatter("%d"))
# ax.set_ylim(bottom=0)
ax.grid()
ax.legend()
# fig.savefig(save_fig_path+"\\"+"unint_mass.pdf", dpi=300)



# %% test if function works

# epoch_time = netcdf_get_epoch_time(file_path, settings)

# # data_1d

# epoch_time, header_1d, data_1d = netcdf_data_1d_load(file_path, settings)

# # data_2d

# epoch_time, header_2d, data_2d = netcdf_data_2d_load(file_path, settings)


# %%
