# %%
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import numpy as np
from matplotlib import pyplot as plt, dates
import json
from datacula.lake.datalake import DataLake
from datacula.lake import plot
from datacula.lake import processer
from datacula import loader
from datetime import datetime, timedelta
import pytz
import os
from datacula.time_manage import time_str_to_epoch

# preferred settings for plotting
plt.rcParams.update({'text.color': "#333333",
                     'axes.labelcolor': "#333333",
                     "figure.figsize": (6,4),
                     "font.size": 14,
                     "axes.edgecolor": "#333333",
                     "axes.labelcolor": "#333333",
                     "xtick.color": "#333333",
                     "ytick.color": "#333333",
                     "pdf.fonttype": 42,
                     "ps.fonttype": 42})

# %%
# settings_path = "C:\\Users\\253393\\Documents\\GitHub\\CAFE-processing\\server\\dev\\server_data_settings.json"
# settings_path = "C:\\Code\\datacula\\private_dev\\lake_settings.json"
settings_path = "D:\\Tracer\\working_folder\\lake_settings.json"
# settings_path = "C:\\Users\\kkgor\\OneDrive\\Documents\\GitHub\\CAFE-processing\\server\\dev\\server_data_settings.json"

#load json file
with open(settings_path, 
            'r', encoding='utf-8'
            ) as json_file:
                settings = json.load(json_file)

# %%
# path = "C:\\Users\\253393\\Desktop\\hard_drive_backup\\working_folder\\raw_data\\"
# path = "U:\\Projects\\TRACER_analysis\\working_folder\\raw_data"
path = "D:\\Tracer\\working_folder\\raw_data"
# path = "U:\\code_repos\\CAFE-processing\\server\\server_files\\"
# path = "C:\\Users\\kkgor\\OneDrive\\Documents\\GitHub\\CAFE-processing\\server\\server_files\\"
# settings = get_server_data_settings()

#%%
keys_subset = ["aeronet_PuertoRico",
               "SP2_data",
               "SPAMS_data",
               "CAPS_data",
               "SMPS_data",
               "APS3320_data",
               "CCNc",
               "ARM_aps",
               "ARM_met",
               "ARM_psap",
               "ARM_ccnc",
               "ARM_cpc_fine",
               "ARM_cpc_ultra",
               "ARM_neph_dry",
               "ARM_neph_wet",
               "ARM_laport_aod",
               "ARM_laport_sda",
               ]
# keys_subset = ["APS3320_data", "ARM_aps"]
simple_settings = {key: settings[key] for key in keys_subset}

datalake = DataLake(simple_settings, path)

datalake.update_datastream()
loader.save_datalake(path=path, data_lake=datalake, sufix_name='full_raw')

datalake.info()
print('Done')

# %% 
time_format = "%m/%d/%Y %H:%M:%S"
tracer_timezone = pytz.timezone('US/Central')
epoch_start = time_str_to_epoch('07/13/2022 00:00:00', time_format, 'US/Central')
epoch_end = time_str_to_epoch('07/15/2022 00:00:00', time_format, 'US/Central')
datalake.reaverage_datastreams(600*1, epoch_start=epoch_start, epoch_end=epoch_end)
# epoch_start = datetime.fromisoformat('2022-06-30T19:00').timestamp()
# epoch_end = datetime.fromisoformat('2022-08-01T05:00').timestamp()

datalake = processer.sizer_mean_properties(
    datalake=datalake,
    stream_key='smps_2D',
    new_key='smps_mean_properties',
    diameter_multiplier_to_nm=1,
)

# %%

fig, ax = plt.subplots()
plot.timeseries(
    ax,
    datalake,
    'smps_mean_properties',
    'Mass_(ugPm3)',
    'SMPS',
    shade=True)
plot.timeseries(
    ax,
    datalake,
    'spams',
    'total_mass[ug/m3]',
    'LANL ams',
    shade=True)
# plot.timeseries(
#     ax,
#     datalake,
#     'sp2',
#     'BC_mass[ug/m3]',
#     'SP2',
#     shade=True)

ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('PM10 Unit Mass ($\mu g/m^3$)')
# ax.set_xlim((epoch_start, epoch_end))
# ax.set_yscale('log')
ax.xaxis.set_major_formatter(dates.DateFormatter('%d %H', tz=tracer_timezone))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
ax.set_ylim(bottom=0, top=20)
ax.grid()
# ax.legend()

