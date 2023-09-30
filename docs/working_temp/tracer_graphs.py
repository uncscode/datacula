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
from datacula import loader, convert
from datacula.colors import TAILWIND
from datetime import datetime, timedelta
import pytz
import os
from datacula.time_manage import time_str_to_epoch
import copy

# preferred settings for plotting
base_color = TAILWIND['gray']['700']
plt.rcParams.update({'text.color': base_color,
                     'axes.labelcolor': base_color,
                     "figure.figsize": (6,4),
                     "font.size": 14,
                     "axes.edgecolor": base_color,
                     "axes.labelcolor": base_color,
                     "xtick.color": base_color,
                     "ytick.color": base_color,
                     "pdf.fonttype": 42,
                     "ps.fonttype": 42})


# %%
path = "D:\\Tracer\\working_folder\\raw_data"

datalake = loader.load_datalake(path=path, sufix_name='processed')

datalake.info()
# datalake.remove_zeros()

time_format = "%m/%d/%Y %H:%M"
tracer_timezone = pytz.timezone('US/Central')
utc_timezone = pytz.timezone('UTC')

epoch_start = time_str_to_epoch('07/10/2022 00:00', time_format, 'US/Central')
epoch_end = time_str_to_epoch('07/25/2022 00:00', time_format, 'US/Central')
datalake.reaverage_datastreams(3600*2, epoch_start=epoch_start, epoch_end=epoch_end)

# %% 



datalake.remove_outliers(
    datastreams_keys=['merged_mean_properties', 'smps_mean_properties'],
    outlier_headers=['Unit_Mass_(ug/m3)_PM10', 'Unit_Mass_(ug/m3)_PM2.5'],
    mask_top=200,
    mask_bottom=0
)

# %% ratios

datalake.reaverage_datastreams(300)

babs_wet = datalake.datastreams['CAPS_data'].return_data(keys=['Babs_wet_CAPS_450nm[1/Mm]'])[0]
babs_dry = datalake.datastreams['CAPS_data'].return_data(keys=['Babs_dry_CAPS_450nm[1/Mm]'])[0]
ratio_babs_wet_dry = babs_wet / babs_dry

bsca_wet = datalake.datastreams['CAPS_data'].return_data(keys=['Bsca_wet_CAPS_450nm[1/Mm]'])[0]
bsca_dry = datalake.datastreams['CAPS_data'].return_data(keys=['Bsca_dry_CAPS_450nm[1/Mm]'])[0]
ratio_bsca_wet_dry = bsca_wet / bsca_dry

bext_wet = datalake.datastreams['CAPS_data'].return_data(keys=['Bext_wet_CAPS_450nm[1/Mm]'])[0]
bext_dry = datalake.datastreams['CAPS_data'].return_data(keys=['Bext_dry_CAPS_450nm[1/Mm]'])[0]
ratio_bext_wet_dry= bext_wet / bext_dry

albedo_ratio = datalake.datastreams['CAPS_data'].return_data(keys=['SSA_wet_CAPS_450nm[1/Mm]'])[0] / datalake.datastreams['CAPS_data'].return_data(keys=['SSA_dry_CAPS_450nm[1/Mm]'])[0]

neph_bsca_wet = datalake.datastreams['arm_neph_wet'].return_data(keys=['Bsca450nm_[1/Mm]'])[0]
neph_bsca_dry = datalake.datastreams['arm_neph_dry'].return_data(keys=['Bsca450nm_[1/Mm]'])[0]
ratio_neph_bsca_wet_dry = neph_bsca_wet / neph_bsca_dry

# mass ratio

inorganic_mass = datalake.datastreams['spams'].return_data(keys=['mass_Chl[ug/m3]', 'mass_NH4[ug/m3]', 'mass_SO4[ug/m3]', 'mass_NO3[ug/m3]'])
inorganic_mass = np.nansum(inorganic_mass, axis=0)

organic_mass = datalake.datastreams['spams'].return_data(keys=['mass_OC[ug/m3]'])[0]
bc_mass = datalake.datastreams['sp2'].return_data(keys=['BC_mass[ug/m3]'])[0]

pm1_mass = datalake.datastreams['smps_mean_properties'].return_data(keys=['Mass_(ug/m3)_PM1'])[0]

remainder_mass = pm1_mass - inorganic_mass - organic_mass - bc_mass

inorganic_mass_fraction = inorganic_mass / pm1_mass
organic_mass_fraction = organic_mass / pm1_mass
bc_mass_fraction = bc_mass / pm1_mass
remainder_mass_fraction = remainder_mass / pm1_mass

inorganic_bc_mass_ratio = inorganic_mass / bc_mass
bc_remainder_mass_ratio = bc_mass / remainder_mass

# save as new datastream
time = datalake.datastreams['CAPS_data'].return_time(datetime64=False)
# combine the data for datalake
combinded = np.vstack((
    ratio_babs_wet_dry,
    ratio_bsca_wet_dry,
    ratio_bext_wet_dry,
    albedo_ratio,
    ratio_neph_bsca_wet_dry,
    inorganic_mass_fraction,
    organic_mass_fraction,
    bc_mass_fraction,
    remainder_mass_fraction,
    inorganic_bc_mass_ratio,
    bc_remainder_mass_ratio,
))
header = [
    "ratio_babs_wet_dry",
    "ratio_bsca_wet_dry",
    "ratio_bext_wet_dry",
    "albedo_ratio",
    "ratio_neph_bsca_wet_dry",
    "inorganic_mass_fraction",
    "organic_mass_fraction",
    "bc_mass_fraction",
    "remainder_mass_fraction",
    "inorganic_bc_mass_ratio",
    "bc_remainder_mass_ratio",
]

datalake.add_processed_datastream(
    key='ratios',
    time_stream=time,
    data_stream=combinded,
    data_stream_header=header,
    average_times=datalake.datastreams['CAPS_data'].average_int_sec,
    average_base=datalake.datastreams['CAPS_data'].average_base_sec,
)

datalake_dust1 = copy.deepcopy(datalake)
datalake_dust2 = copy.deepcopy(datalake)
datalake_fire = copy.deepcopy(datalake)
# %%
datalake.reaverage_datastreams(3600*2)

epoch_start_dust1 = time_str_to_epoch('07/16/2022 15:00', time_format, 'US/Central')
epoch_end_dust1 = time_str_to_epoch('07/18/2022 22:00', time_format, 'US/Central')
datalake_dust1.reaverage_datastreams(300, epoch_start=epoch_start_dust1, epoch_end=epoch_end_dust1)

epoch_start_dust2 = time_str_to_epoch('07/20/2022 12:00', time_format, 'US/Central')
epoch_end_dust2 = time_str_to_epoch('07/22/2022 18:00', time_format, 'US/Central')
datalake_dust2.reaverage_datastreams(300, epoch_start=epoch_start_dust2, epoch_end=epoch_end_dust2)

epoch_start_fire = time_str_to_epoch('07/12/2022 12:00', time_format, 'US/Central')
epoch_end_fire = time_str_to_epoch('07/13/2022 6:00', time_format, 'US/Central')
datalake_fire.reaverage_datastreams(300, epoch_start=epoch_start_fire, epoch_end=epoch_end_fire)

# %%
save_fig_path = os.path.join(path, "plots")
colors = {
    "sulfate": '#E5372C',
    "nitrate": '#2E569E',
    "ammonium": '#F2B42F',
    "chloride": '#BD3D90',
    "organic": '#0E964C',
    "CAPS_wet": TAILWIND['sky']['400'],
    "CAPS_dry": TAILWIND['blue']['800'],
    "gray_dark": '#333333',
    "gray_light": '#666666',
    "kappa_ccn": '#662D91',
    "kappa_amsCCN": '#D6562B',
    "kappa_amsHGF": '#F09B36',
    "dust1": TAILWIND['yellow']['800'],
    "dust2": TAILWIND['yellow']['500'],
    "fire": TAILWIND['red']['500'],
    "PM1": TAILWIND['stone']['400'],
    "PM10": TAILWIND['stone']['700'],
    "AOD_laport": TAILWIND['violet']['700'],
    "AOD_puerto_rico": TAILWIND['violet']['400'],
}

# timeseries_left = np.datetime64('07/11/2022 00:00', time_format)
# timeseries_right = np.datetime64('07/25/2022 00:00', time_format)
# datetime64_from_epoch_array

timeseries_left = convert.datetime64_from_epoch_array(
    [time_str_to_epoch('07/10/2022 00:00', time_format, 'US/Central')])
timeseries_right = convert.datetime64_from_epoch_array(
    [time_str_to_epoch('07/25/2022 00:00', time_format, 'US/Central')]
)
# %% plot mass time series
fig, ax = plt.subplots()
# plot.timeseries(
#     ax,
#     datalake,
#     'merged_mean_properties',
#     'Mass_(ug/m3)_PM10',
#     'PM10',
#     color=colors['PM10'],
#     shade=False)
plot.timeseries(
    ax,
    datalake,
    "aos_aps_mean_properties",
    "Mass_(ug/m3)_PM10",
    "aos PM10",
    color=colors['PM10'],
    shade=False,
)
plot.timeseries(
    ax,
    datalake,
    'merged_mean_properties',
    'Mass_(ug/m3)_PM1',
    'PM1',
    color=colors['PM1'],
    shade=False)
# plt.tick_params(rotation=-35)
ax.set_xlim((timeseries_left, timeseries_right))
ax2 = ax.twinx()
plot.timeseries(
    ax2,
    datalake,
    'CAPS_data',
    'Bext_wet_CAPS_450nm[1/Mm]',
    'AOD',
    shade=False,
    color=colors['AOD_laport'],
    line_kwargs={'linestyle': '',
                 'marker': 'o',
                 'markersize': 4,
                 'markerfacecolor': colors['AOD_laport']})
ax2.set_ylabel('AOD (500 nm)')
ax2.set_ylim(bottom=0, top=1)
ax2.set_ycolor(colors['AOD_laport'])


ax.minorticks_on()

ax.set_ylabel('PM Mass ($\mu g/m^3$)')
ax.xaxis.set_major_locator(dates.DayLocator(interval=2, tz=tracer_timezone))
ax.xaxis.set_major_formatter(dates.DateFormatter('%d', tz=tracer_timezone))
ax.set_xlabel('July Day 2022 (CDT)')
ax.set_ylim(bottom=0, top=150)
ax.grid()
# ax.legend()
# ax2.legend()
fig.tight_layout()
fig.figure.savefig(save_fig_path+'\\'+'PM_mass.pdf', dpi=300)

# %% time series optical properties

fig, ax = plt.subplots()
plot.timeseries(
    ax,
    datalake,
    'CAPS_data',
    'Babs_wet_CAPS_450nm[1/Mm]',
    'Babs wet',
    color=colors['CAPS_wet'],
    shade=False)
plot.timeseries(
    ax,
    datalake,
    'CAPS_data',
    'Babs_dry_CAPS_450nm[1/Mm]',
    'Babs dry',
    color=colors['CAPS_dry'],
    shade=True)

# ax2.set_ylabel('remainder mass fraction')
# ax2.set_ylim(bottom=0, top=1)

ax.set_ylabel('Absorption (450 nm) [1/Mm]')
ax.set_xlim((timeseries_left, timeseries_right))
ax.xaxis.set_major_locator(dates.DayLocator(interval=2, tz=tracer_timezone))
ax.xaxis.set_major_formatter(dates.DateFormatter('%d', tz=tracer_timezone))
ax.set_xlabel('July Day 2022 (CDT)')
ax.set_ylim(bottom=0, top=30)
ax.grid()
ax.legend()
fig.tight_layout()
fig.figure.savefig(save_fig_path+'\\'+'Bsca.pdf', dpi=300)



# %% 
datalake.reaverage_datastreams(300, stream_keys=['ratios'])
datalake_dust1.reaverage_datastreams(300, stream_keys=['ratios'])
datalake_dust2.reaverage_datastreams(300, stream_keys=['ratios'])
datalake_fire.reaverage_datastreams(300, stream_keys=['ratios'])

range = (0.2, 2.5)
bins = 50
fig, ax = plt.subplots()
plot.histogram(
    ax,
    datalake_dust1,
    'ratios',
    'ratio_babs_wet_dry',
    'Dust 17th',
    bins=bins,
    range=range,
    kwargs={'alpha': 0.5}
)
plot.histogram(
    ax,
    datalake_dust2,
    'ratios',
    'ratio_babs_wet_dry',
    'Dust 21st',
    bins=bins,
    range=range,
    kwargs={'alpha': 0.5}
)
plot.histogram(
    ax,
    datalake_fire,
    'ratios',
    'ratio_babs_wet_dry',
    'Combustion 12th',
    bins=bins,
    range=range,
    kwargs={'alpha': 0.5}
)
ax.set_xlabel('Absorption Ratio (RH 84% / RH 54%) [1/Mm]')
ax.set_ylabel('Occurance')
ax.grid()
fig.savefig(save_fig_path+'\\'+'absorption_ratio.pdf', dpi=300)

# %%
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

