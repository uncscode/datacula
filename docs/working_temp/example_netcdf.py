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
file_fullpath = "D:\\Tracer\\working_folder\\raw_data\\aeronet_PuertoRico\\20220701_20220731_La_Parguera.ONEILL_lev10"

nc_file = loader.netcdf_info_print(file_fullpath, file_return=True)

# test = nc_file.variables.get("")
# print(test)


# file_path = "E:\\Tracer\\working_folder\\raw_data\\ARM_aos_aps\\houaosapsM1.b1.20220627.145726.nc"
file_path = "D:\\Tracer\\working_folder\\raw_data"
# %%
instrument_settings = {
    "aeronet_PuertoRico": {
        "instrument_name": "id_aeronet",
        "data_stream_name": "aeronet_puerto_rico",
        "data_loading_function": "general_load",
        "relative_data_folder": "aeronet_PuertoRico",
        "Time_shift_sec": 0,
        "timezone_identifier": "UTC",
        "data_checks": {
            "skip_rows": 8,
            "skip_end": 0
        },
        "data_header": [
            "total_aod_500nm[tau]",
            "fine_aod_500nm[tau]",
            "coarse_aod_500nm[tau]",
            "fine_mode_fraction_500nm"
        ],
        "data_column": [
            4, 5, 6, 7
        ],
        "time_column": [0,1],
        "time_format": "%d:%m:%Y %H:%M:%S",
        "filename_regex": "*.ONEILL_lev10",
        "base_interval_sec": 3600,
        "data_delimiter": ","
    }
}


# "data_2d": "dN_dlogDp",
# "header_2d": "diameter_aerodynamic",
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
datalake.reaverage_datastreams(3600*3, epoch_start=epoch_start, epoch_end=epoch_end)




# %%

fig, ax = plt.subplots()
plot.timeseries(
    ax,
    datalake,
    "aeronet_puerto_rico",
    "total_aod_500nm[tau]",
    "AOD",
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
