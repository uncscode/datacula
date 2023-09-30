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

path = "F:\\Tracer\\working_folder\\raw_data"


#%%

datalake = loader.load_datalake(path=path, sufix_name='processed')

datalake.info()
# datalake.remove_zeros()


# %% 
time_format = "%m/%d/%Y %H:%M:%S"
tracer_timezone = pytz.timezone('US/Central')
epoch_start = time_str_to_epoch('07/01/2022 00:00:00', time_format, 'US/Central')
epoch_end = time_str_to_epoch('07/28/2022 00:00:00', time_format, 'US/Central')
# datalake.reaverage_datastreams(600, epoch_start=epoch_start, epoch_end=epoch_end)
# epoch_start = datetime.fromisoformat('2022-06-30T19:00').timestamp()
# epoch_end = datetime.fromisoformat('2022-08-01T05:00').timestamp()


datalake.info()



# %% average

datalake.remove_outliers(
    datastreams_keys=['merged_mean_properties', 'smps_mean_properties'],
    outlier_headers=['Unit_Mass_(ugPm3)_PM10', 'Unit_Mass_(ugPm3)_PM2.5'],
    mask_top=200,
    mask_bottom=0
)


epoch_start = time_str_to_epoch('07/12/2022 00:00:00', time_format, 'US/Central')
epoch_end = time_str_to_epoch('07/25/2022 00:00:00', time_format, 'US/Central')
datalake.reaverage_datastreams(3600, epoch_start=epoch_start, epoch_end=epoch_end)


# %%
fig, ax = plt.subplots()
plot.timeseries(
    ax,
    datalake,
    'merged_mean_properties',
    'Unit_Mass_(ugPm3)_PM10',
    'PM10',
    shade=True)
plot.timeseries(
    ax,
    datalake,
    'merged_mean_properties',
    'Unit_Mass_(ugm3)_PM1',
    'PM2.5',
    shade=True)
plot.timeseries(
    ax,
    datalake,
    'smps_mean_properties',
    'Unit_Mass_(ug/m3)',
    'SMPS 800 nm',
    shade=True)
plt.tick_params(rotation=-35)

ax2 = ax.twinx()
plot.timeseries(
    ax2,
    datalake,
    'arm_aeronet_sda',
    'total_aod_500nm[tau]',
    'AOD',
    shade=False,
    color='black')

ax.minorticks_on()

ax.set_ylabel('PM10 Unit Mass ($\mu g/m^3$)')
# ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d', tz=tracer_timezone))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
ax.set_ylim(bottom=0, top=80)
ax.grid()
ax.legend()
# ax2.legend()

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

