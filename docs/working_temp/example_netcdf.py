# %%
import warnings
import numpy as np
import netCDF4 as nc
from datetime import datetime, timedelta

from matplotlib import pyplot as plt, dates
import datacula.convert as convert

from typing import List, Union, Tuple, Dict, Any

from datacula.lake.datalake import DataLake
from datacula.lake import processer, plot
# %%

# file_path = "E:\\Tracer\\working_folder\\raw_data\\ARM_aos_aps\\houaosapsM1.b1.20220627.145726.nc"
file_path = "D:\\Tracer\\working_folder\\raw_data"
# %%
instrument_settings = {"ARM_aps": {
        "instrument_name": "AOS_aps_data",
        "data_stream_name": ["aos_aps_1D", "aos_aps_2D"],
        "data_processing_function": "SMPS_processing",
        "data_loading_function": "netcdf_load",
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
# %% load into datalake


datalake = DataLake(instrument_settings, file_path)

datalake.update_datastream()
# datalake.remove_zeros()
datalake.info(header_print_count=15)

# %% 
# epoch_start = datetime.fromisoformat('2022-06-30T19:00').timestamp()
# epoch_end = datetime.fromisoformat('2022-08-01T05:00').timestamp()

# %% 
# truncation processing
datalake.reaverage_datastreams(600)

# %% list datastreams in a nice print format





# %%

fig, ax = plt.subplots()
plot.timeseries(
    ax,
    datalake,
    'aos_aps_1D',
    'surface_area_concentration_nm_cm3',
    'arm',
    shade=True)

ax.minorticks_on()
plt.tick_params(rotation=-35)
# ax.set_ylabel('Particle Mass (ug/m3)')
# ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
# ax.set_ylim((0,50))
ax.grid()
ax.legend()
# fig.savefig(save_fig_path+'\\'+'unint_mass.pdf', dpi=300)



# %% test if function works

# epoch_time = netcdf_get_epoch_time(file_path, settings)

# # data_1d

# epoch_time, header_1d, data_1d = netcdf_data_1d_load(file_path, settings)

# # data_2d

# epoch_time, header_2d, data_2d = netcdf_data_2d_load(file_path, settings)


# %%
