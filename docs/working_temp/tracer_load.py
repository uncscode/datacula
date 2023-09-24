# %%
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import numpy as np
from matplotlib import pyplot as plt, dates
import json
from datacula.lake.datalake import DataLake
from datacula.lake import processer, plot
from datacula import loader
from datetime import datetime, timedelta
import pytz
import os

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
settings_path = "C:\\Users\\kkgor\\OneDrive\\Areas\\GitHub\\datacula\\private_dev\\lake_settings.json"
# settings_path = "C:\\Users\\kkgor\\OneDrive\\Documents\\GitHub\\CAFE-processing\\server\\dev\\server_data_settings.json"

#load json file
with open(settings_path, 
            'r', encoding='utf-8'
            ) as json_file:
                settings = json.load(json_file)

# %%
# path = "C:\\Users\\253393\\Desktop\\hard_drive_backup\\working_folder\\raw_data\\"
path = "U:\\Projects\\TRACER_analysis\\working_folder\\raw_data"
path = "D:\\Tracer\\working_folder\\raw_data"
# path = "U:\\code_repos\\CAFE-processing\\server\\server_files\\"
# path = "C:\\Users\\kkgor\\OneDrive\\Documents\\GitHub\\CAFE-processing\\server\\server_files\\"
# settings = get_server_data_settings()

#%%
keys_subset = ["SP2_data", "SPAMS_data", "CAPS_data", "SMPS_data", "APS3320_data"]
# ["PASS3_data", "picarro_data","SP2_data", "SPAMS_data", "CAPS_data_data", "SMPS_data", "APS_data", "CCNc"]
simple_settings = {key: settings[key] for key in keys_subset}

datalake = DataLake(simple_settings, path)

datalake.update_datastream()
datalake.remove_zeros()


# %% 
epoch_start = datetime.fromisoformat('2022-06-30T19:00').timestamp()
epoch_end = datetime.fromisoformat('2022-08-01T05:00').timestamp()

# %% 
# truncation processing
datalake.reaverage_datastreams(300, epoch_start=epoch_start, epoch_end=epoch_end)

# datalake = processer.pass3_processing(
#     datalake=datalake,
#     babs_405_532_781=[1, 1, 1],
#     bsca_405_532_781=[1.37, 1.2, 1.4],
# )

datalake = processer.caps_processing(
    datalake=datalake,
    truncation_bsca=True,
    truncation_interval_sec=600,
    truncation_interp=True,
    refractive_index=1.5,
    calibration_wet=0.95,
    calibration_dry=1.003
)
    # calibration_wet=0.985,
    # calibration_dry=1.002
# DataLake_saveNload(path, datalake=datalake)

datalake.reaverage_datastreams(300)
datalake = processer.albedo_processing(datalake=datalake)
loader.save_datalake(path=path, data_lake=datalake, sufix_name='new_lower')
# %%

# %%

tracer_timezone = pytz.timezone('US/Central')


save_fig_path = os.path.join(path, "plots")
colors = {
    "sulfate": '#E5372C',
    "nitrate": '#2E569E',
    "ammonium": '#F2B42F',
    "chloride": '#BD3D90',
    "organic": '#0E964C',
    "CAPS_wet": '#00A499',
    "CAPS_dry": '#F15A24',
    "gray_dark": '#333333',
    "gray_light": '#666666',
    "kappa_ccn": '#662D91',
    "kappa_amsCCN": '#D6562B',
    "kappa_amsHGF": '#F09B36',
    "dust": '#754C24'
}


babs_wet = datalake.datastreams['CAPS_data'].return_data(keys=['Babs_wet_CAPS_450nm[1/Mm]'])[0]
babs_dry = datalake.datastreams['CAPS_data'].return_data(keys=['Babs_dry_CAPS_450nm[1/Mm]'])[0]
ratio = babs_wet / babs_dry

albedo_ratio = datalake.datastreams['CAPS_data'].return_data(keys=['SSA_wet_CAPS_450nm[1/Mm]'])[0] / datalake.datastreams['CAPS_data'].return_data(keys=['SSA_dry_CAPS_450nm[1/Mm]'])[0]


fig, ax = plt.subplots()
ax.hist(ratio, bins=50, range=(0.2, 1.8), alpha=1, label='Absorption', color=colors['gray_light'])
ax.set_xlabel('Absorption Ratio (RH 84% / RH 54%) [1/Mm]')
ax.set_ylabel('Occurance')
ax.grid()
fig.savefig(save_fig_path+'\\'+'absorption_enhancement.pdf', dpi=300)


fig, ax = plt.subplots()
ax.hist(albedo_ratio, bins=50, range=(0.9, 1.1), alpha=1, label='Albedo', color=colors['gray_light'])
ax.set_xlabel('SSA Ratio (RH 84% / RH 54%)')
ax.set_ylabel('Occurance')
ax.grid()
fig.savefig(save_fig_path+'\\'+'SSA_enhancement.pdf', dpi=300)

# %%
fig, ax = plt.subplots()
ax.plot(
    datalake.datastreams['CAPS_data'].return_time(datetime64=True),
    datalake.datastreams['CAPS_data'].return_data(keys=['Bext_wet_CAPS_450nm[1/Mm]'])[0],
    label='Extinction wet'
)
ax.plot(
    datalake.datastreams['CAPS_data'].return_time(datetime64=True),
    datalake.datastreams['CAPS_data'].return_data(keys=['Bext_dry_CAPS_450nm[1/Mm]'])[0],
    label='Extinction dry'
)
# ax.plot(
#     datalake.datastreams['CAPS_data'].return_time(datetime64=True),
#     datalake.datastreams['CAPS_data'].return_data(keys=['Bsca_wet_CAPS_450nm[1/Mm]'])[0],
#     label='Scattering wet'
# )
# ax.plot(
#     datalake.datastreams['CAPS_data'].return_time(datetime64=True),
#     datalake.datastreams['CAPS_data'].return_data(keys=['Bsca_dry_CAPS_450nm[1/Mm]'])[0],
#     label='Scattering dry'
# )
ax.set_ylim(0,200)
plt.tick_params(rotation=-35)
ax.set_ylabel('Extinction or Scattering [1/Mm]')
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'), tz=tracer_timezone)
ax.legend()
fig.savefig(save_fig_path+'\\'+'CAPS_data.png', dpi=300)

# %%

fig, ax = plt.subplots()
ax.plot(
    datalake.datastreams['CAPS_data'].return_time(datetime64=True),
    datalake.datastreams['CAPS_data'].return_data(keys=['SSA_wet_CAPS_450nm[1/Mm]'])[0],
    label='wet'
)
ax.plot(
    datalake.datastreams['CAPS_data'].return_time(datetime64=True),
    datalake.datastreams['CAPS_data'].return_data(keys=['SSA_dry_CAPS_450nm[1/Mm]'])[0],
    label='dry'
)
plt.tick_params(rotation=-35)
ax.set_ylabel('Single Scattering Albedo')
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
ax.set_ylim(0.8,1.1)
ax.legend()
fig.savefig(save_fig_path+'\\'+'CAPS_data_SSA.png', dpi=300)

print('wet mean: ', np.nanmean(datalake.datastreams['CAPS_data'].return_data(keys=['SSA_wet_CAPS_450nm[1/Mm]'])[0]))
print('dry mean: ', np.nanmean(datalake.datastreams['CAPS_data'].return_data(keys=['SSA_dry_CAPS_450nm[1/Mm]'])[0]))


# %%
datalake.reaverage_datalake(60*10)

# PASS
pass_save_keys = ['Bsca405nm[1/Mm]', 'Bsca532nm[1/Mm]', 'Bsca781nm[1/Mm]']
# datastream_to_csv(datalake.datastreams['pass3'], path, header_keys=pass_save_keys, time_shift_sec=0, filename='PASS3_CDT')
# datastream_to_csv(datalake.datastreams['pass3'], path, header_keys=pass_save_keys, time_shift_sec=-3600, filename='PASS3_CST')

# # CDT
# datalake_to_csv(datalake=datalake, path=path, time_shift_sec=0, sufix_name='CDT')
# # CST
# datalake_to_csv(datalake=datalake, path=path, time_shift_sec=-3600, sufix_name='CST')
# %%

