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
file_fullpath = "F:\\Tracer\\working_folder\\raw_data\\ARM_neph_wet\\houaosnephwetM1.b1.20220626.000000.nc"

nc_file = loader.netcdf_info_print(file_fullpath, file_return=True)

# test = nc_file.variables.get("")
# print(test)


# file_path = "E:\\Tracer\\working_folder\\raw_data\\ARM_aos_aps\\houaosapsM1.b1.20220627.145726.nc"
file_path = "F:\\Tracer\\working_folder\\raw_data"
# %%
instrument_settings = {
    "ARM_neph_wet": {
        "instrument_name": "id_aosnephwet",
        "data_stream_name": ["arm_neph_wet"],
        "data_loading_function": "netcdf_load",
        "relative_data_folder": "ARM_neph_wet",
        "Time_shift_sec": 0,
        "timezone_identifier": "UTC",
        "netcdf_reader": {
            "data_1d": ["Bs_R_Wet_Neph3W",
                        "Bs_G_Wet_Neph3W",
                        "Bs_R_Wet_Neph3W",
                        "Bbs_B_Wet_Neph3W",
                        "Bbs_G_Wet_Neph3W",
                        "Bbs_R_Wet_Neph3W",
                        "RH_Neph_Wet"],
            "header_1d": ["Bsca450nm_[1/Mm]",
                          "Bsca550nm_[1/Mm]",
                          "Bsca700nm_[1/Mm]",
                          "Bbsca450nm_[1/Mm]",
                          "Bbsca550nm_[1/Mm]",
                          "Bbsca700nm_[1/Mm]",
                          "relative_humidity_[%]"]
            },
        "time_column": ["base_time", "time_offset"],
        "time_format": "epoch",
        "filename_regex": "*.nc",
        "base_interval_sec": 60
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
    "arm_neph_wet",
    "Bsca550nm_[1/Mm]",
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
