# %%
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import numpy as np
from matplotlib import pyplot as plt, dates
import json
from data_lake_tester import DataLake, DataLake_saveNload, datalake_to_csv, datastream_to_csv
import datalake_processer as dlp
from datetime import datetime, timedelta
import os

plt.rcParams["figure.figsize"] = (10,8)
plt.rcParams["font.size"] = (16)
# %%
# settings_path = "C:\\Users\\253393\\Documents\\GitHub\\CAFE-processing\\server\\dev\\server_data_settings.json"
settings_path = "U:\\Areas\\code_repos\\CAFE-processing\\server\\dev\\server_data_settings.json"
# settings_path = "C:\\Users\\kkgor\\OneDrive\\Documents\\GitHub\\CAFE-processing\\server\\dev\\server_data_settings.json"

#load json file
with open(settings_path, 
            'r', encoding='utf-8'
            ) as json_file:
                settings = json.load(json_file)

# %%
# path = "C:\\Users\\253393\\Desktop\\hard_drive_backup\\working_folder\\raw_data\\"
path = "U:\\Projects\\TRACER_analysis\\working_folder\\raw_data"
# path = "U:\\code_repos\\CAFE-processing\\server\\server_files\\"
# path = "C:\\Users\\kkgor\\OneDrive\\Documents\\GitHub\\CAFE-processing\\server\\server_files\\"
# settings = get_server_data_settings()

#%%
keys_subset = ["picarro_data", "PASS3_data"]
# ["PASS3_data", "picarro_data","SP2_data", "SPAMS_data", "CAPS_dual_data", "SMPS_data", "APS_data", "CCNc"]
simple_settings = {key: settings[key] for key in keys_subset}

datalake = DataLake(simple_settings, path)

# %%
datalake.update_datastream()
datalake.remove_zeros()


# %% 
epoch_start = datetime.fromisoformat('2022-06-30T19:00').timestamp()
epoch_end = datetime.fromisoformat('2022-08-01T05:00').timestamp()

# %% 
# truncation processing
datalake.reaverage_datalake(300, epoch_start=epoch_start, epoch_end=epoch_end)

datalake = dlp.pass3_processing(
    datalake=datalake,
    babs_405_532_781=[1, 1, 1],
    bsca_405_532_781=[1.37, 1.2, 1.4],
)

# datalake = dlp.caps_processing(
#     datalake=datalake,
#     truncation_bsca=False,
#     truncation_interval_sec=600,
#     truncation_interp=True,
#     refractive_index=1.5,
#     calibration_wet=0.97,
#     calibration_dry=1.003
# )

# DataLake_saveNload(path, datalake=datalake)


# %%
datalake.reaverage_datalake(3*300)

save_fig_path = os.path.join(path, "plots")

fig, ax = plt.subplots()
ax.plot(
    datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
    datalake.datastreams['CAPS_dual'].return_data(keys=['Bext_wet_CAPS_450nm[1/Mm]'])[0],
    label='Extinction wet'
)
ax.plot(
    datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
    datalake.datastreams['CAPS_dual'].return_data(keys=['Bext_dry_CAPS_450nm[1/Mm]'])[0],
    label='Extinction dry'
)
ax.plot(
    datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
    datalake.datastreams['CAPS_dual'].return_data(keys=['Bsca_wet_CAPS_450nm[1/Mm]'])[0],
    label='Scattering wet'
)
ax.plot(
    datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
    datalake.datastreams['CAPS_dual'].return_data(keys=['Bsca_dry_CAPS_450nm[1/Mm]'])[0],
    label='Scattering dry'
)
ax.set_ylim(0,200)
plt.tick_params(rotation=-35)
ax.set_ylabel('Extinction or Scattering [1/Mm]')
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
ax.legend()
fig.savefig(save_fig_path+'\\'+'CAPS_dual.png', dpi=300)

# %%
datalake = dlp.albedo_processing(datalake=datalake)

fig, ax = plt.subplots()
ax.plot(
    datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
    datalake.datastreams['CAPS_dual'].return_data(keys=['SSA_wet_CAPS_450nm[1/Mm]'])[0],
    label='wet'
)
ax.plot(
    datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
    datalake.datastreams['CAPS_dual'].return_data(keys=['SSA_dry_CAPS_450nm[1/Mm]'])[0],
    label='dry'
)
plt.tick_params(rotation=-35)
ax.set_ylabel('Single Scattering Albedo')
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
ax.set_ylim(0.8,1.1)
ax.legend()
fig.savefig(save_fig_path+'\\'+'CAPS_dual_SSA.png', dpi=300)

print('wet mean: ', np.nanmean(datalake.datastreams['CAPS_dual'].return_data(keys=['SSA_wet_CAPS_450nm[1/Mm]'])[0]))
print('dry mean: ', np.nanmean(datalake.datastreams['CAPS_dual'].return_data(keys=['SSA_dry_CAPS_450nm[1/Mm]'])[0]))


# %%
datalake.reaverage_datalake(60*10)

# PASS
pass_save_keys = ['Bsca405nm[1/Mm]', 'Bsca532nm[1/Mm]', 'Bsca781nm[1/Mm]']
datastream_to_csv(datalake.datastreams['pass3'], path, header_keys=pass_save_keys, time_shift_sec=0, filename='PASS3_CDT')
datastream_to_csv(datalake.datastreams['pass3'], path, header_keys=pass_save_keys, time_shift_sec=-3600, filename='PASS3_CST')

# # CDT
# datalake_to_csv(datalake=datalake, path=path, time_shift_sec=0, sufix_name='CDT')
# # CST
# datalake_to_csv(datalake=datalake, path=path, time_shift_sec=-3600, sufix_name='CST')
# %%

