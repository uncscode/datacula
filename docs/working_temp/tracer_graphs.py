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
grid_color = TAILWIND['gray']['300']
plt.rcParams.update({'text.color': base_color,
                     'axes.labelcolor': base_color,
                     "figure.figsize": (6,4),
                     "font.size": 14,
                     "axes.edgecolor": base_color,
                     "axes.labelcolor": base_color,
                     "xtick.color": base_color,
                     "ytick.color": base_color,
                     "pdf.fonttype": 42,
                     "ps.fonttype": 42,
                     "grid.color": grid_color})


# %%
path = "D:\\Tracer\\working_folder\\raw_data"

datalake = loader.load_datalake(path=path, sufix_name='processed_final')
datalake.remove_outliers(
    datastreams_keys=['merged_mean_properties',
                      'smps_mean_properties',
                      'aos_merged_mean_properties',],
    outlier_headers=['Unit_Mass_(ug/m3)_PM10',
                     'Unit_Mass_(ug/m3)_PM1',
                     'Unit_Mass_(ug/m3)_PM10',],
    mask_top=150,
    mask_bottom=0
)
datalake.remove_outliers(
    datastreams_keys=['CAPS_data',
                      'CAPS_data'],
    outlier_headers=['SSA_dry_CAPS_450nm[1/Mm]',
                     'SSA_wet_CAPS_450nm[1/Mm]'],
    mask_top=1.1,
    mask_bottom=0.75
)

datalake.info(header_print_count=10, limit_header_print=True)
# datalake.remove_zeros()

time_format = "%m/%d/%Y %H:%M"
tracer_timezone = pytz.timezone('US/Central')
utc_timezone = pytz.timezone('UTC')

epoch_start = time_str_to_epoch('07/10/2022 00:00', time_format, 'US/Central')
epoch_end = time_str_to_epoch('07/25/2022 00:00', time_format, 'US/Central')
datalake.reaverage_datastreams(3600*2, epoch_start=epoch_start, epoch_end=epoch_end)

# %% 

datalake_dust1 = copy.deepcopy(datalake)
datalake_dust2 = copy.deepcopy(datalake)
datalake_fire = copy.deepcopy(datalake)
# %% time slices
datalake.reaverage_datastreams(3600*1)

epoch_start_dust1 = time_str_to_epoch('07/16/2022 15:00', time_format, 'US/Central')
epoch_end_dust1 = time_str_to_epoch('07/18/2022 22:00', time_format, 'US/Central')
datalake_dust1.reaverage_datastreams(300, epoch_start=epoch_start_dust1, epoch_end=epoch_end_dust1)

epoch_start_dust2 = time_str_to_epoch('07/20/2022 12:00', time_format, 'US/Central')
epoch_end_dust2 = time_str_to_epoch('07/22/2022 18:00', time_format, 'US/Central')
datalake_dust2.reaverage_datastreams(300, epoch_start=epoch_start_dust2, epoch_end=epoch_end_dust2)

epoch_start_fire = time_str_to_epoch('07/11/2022 12:00', time_format, 'US/Central')
epoch_end_fire = time_str_to_epoch('07/12/2022 0:00', time_format, 'US/Central')
datalake_fire.reaverage_datastreams(300, epoch_start=epoch_start_fire, epoch_end=epoch_end_fire)

# %%
save_fig_path = os.path.join(path, "plots")
colors = {
    "sulfate": TAILWIND['red']['600'],
    "nitrate": '#2E569E',
    "ammonium": '#F2B42F',
    "chloride": '#BD3D90',
    "organic": '#0E964C',
    "CAPS_wet": TAILWIND['blue']['800'],
    "CAPS_dry": TAILWIND['sky']['400'],
    "gray_dark": '#333333',
    "gray_light": TAILWIND['gray']['300'],
    "kappa_ccn": '#662D91',
    "kappa_amsCCN": '#D6562B',
    "kappa_amsHGF": '#F09B36',
    "dust1": TAILWIND['yellow']['800'],
    "dust2": TAILWIND['yellow']['400'],
    "fire": TAILWIND['red']['500'],
    "PM1": TAILWIND['stone']['400'],
    "PM10": TAILWIND['stone']['700'],
    "AOD_laport": TAILWIND['violet']['400'],
    "AOD_puerto_rico": TAILWIND['violet']['700'],
    "BC": TAILWIND['gray']['900'],
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
    "aos_merged_mean_properties",
    "Mass_(ug/m3)_PM10",
    "aos PM10",
    color=colors['PM10'],
    shade=False,
)
plot.timeseries(
    ax,
    datalake,
    'aos_merged_mean_properties',
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
    'arm_aeronet_sda',
    'total_aod_500nm[tau]',
    'AOD',
    shade=False,
    color=colors['AOD_laport'],
    line_kwargs={'linestyle': '',
                 'marker': 'o',
                 'markersize': 4,
                 'markerfacecolor': colors['AOD_laport']})
plot.timeseries(
    ax2,
    datalake,
    'aeronet_puerto_rico_shift4day',
    'total_aod_500nm[tau]',
    'AOD',
    shade=False,
    color=colors['AOD_puerto_rico'],
    line_kwargs={'linestyle': '',
                 'marker': '^',
                 'markersize': 6,
                 'markerfacecolor': colors['AOD_puerto_rico']})
plot.timeseries(
    ax2,
    datalake,
    'aeronet_puerto_rico',
    'total_aod_500nm[tau]',
    'AOD',
    shade=False,
    color=colors['AOD_puerto_rico'],
    line_kwargs={'linestyle': '',
                 'marker': '^',
                 'markersize': 6,
                 'markerfacecolor': colors['AOD_puerto_rico']})
ax2.set_ylabel('Aerosol Optical Depth (500 nm)', color=colors['AOD_puerto_rico'])
ax2.set_ylim(bottom=0, top=1)
ax2.tick_params(axis='y', labelcolor=colors['AOD_puerto_rico'], color=colors['AOD_puerto_rico'])
ax2.spines['right'].set_color(colors['AOD_puerto_rico'])

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

# %% plot ratio time series

fig, ax = plt.subplots()
plot.timeseries(
    ax,
    datalake,
    "aos_merged_mean_properties",
    "Mass_(ug/m3)_PM10",
    "aos PM10",
    color=colors['PM10'],
    shade=False,
)
plot.timeseries(
    ax,
    datalake,
    'aos_merged_mean_properties',
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
    "sp2",
    "BC_mass[ug/m3]",
    "bc mass",
    color=colors['BC'],
    shade=False,
    line_kwargs={'linestyle': '--'}
)
# plot.timeseries(
#     ax2,
#     datalake,
#     'ratios',
#     'inorganic_mass_fraction',
#     'inorganic mass fraction',
#     color=colors['sulfate'],
#     shade=False,
#     line_kwargs={'linestyle': '--'})
# ax2.set_xlim((timeseries_left, timeseries_right))
ax2.set_ylabel('BC Mass ($\mu g/m^3$)', color=colors['BC'])
ax2.set_ylim(bottom=-.1, top=0.5)
# ax2.tick_params(axis='y', labelcolor=colors['sulfate'], color=colors['sulfate'])
# ax2.spines['right'].set_color(colors['sulfate'])


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
fig.figure.savefig(save_fig_path+'\\'+'PM1_BCmass.pdf', dpi=300)

# %% time series optical properties
datalake.reaverage_datastreams(3600*2)
fig, ax = plt.subplots(nrows=2, ncols=1, sharex=True, figsize=(6,8))
ax[0].grid()
ax[1].grid()

plot.timeseries(
    ax[0],
    datalake,
    'CAPS_data',
    'Babs_wet_CAPS_450nm[1/Mm]',
    'Babs wet',
    color=colors['CAPS_wet'],
    shade=True)
plot.timeseries(
    ax[0],
    datalake,
    'CAPS_data',
    'Babs_dry_CAPS_450nm[1/Mm]',
    'Babs dry',
    color=colors['CAPS_dry'],
    shade=False)
ax[0].set_ylabel('Absorption (450 nm) [1/Mm]')
ax[0].set_ylim(bottom=0, top=15)
ax[0].set_xlim((timeseries_left, timeseries_right))
ax[0].xaxis.set_major_locator(dates.DayLocator(interval=2, tz=tracer_timezone))
# ax[0].xaxis.set_major_formatter(dates.DateFormatter('%d', tz=tracer_timezone))

plot.timeseries(
    ax[1],
    datalake,
    'CAPS_data',
    'SSA_wet_CAPS_450nm[1/Mm]',
    'Bsca wet',
    color=colors['CAPS_wet'],
    shade=True)
plot.timeseries(
    ax[1],
    datalake,
    'CAPS_data',
    'SSA_dry_CAPS_450nm[1/Mm]',
    'Bsca dry',
    color=colors['CAPS_dry'],
    shade=False)
ax[1].set_ylabel('Single Scattering Albedo (450 nm)')
ax[1].set_ylim(bottom=0.8, top=1.05)

# ax[1].set_xlim((timeseries_left, timeseries_right))
ax[1].minorticks_on()
ax[1].xaxis.set_major_locator(dates.DayLocator(interval=2, tz=tracer_timezone))
ax[1].xaxis.set_major_formatter(dates.DateFormatter('%d', tz=tracer_timezone))
ax[1].set_xlabel('July Day 2022 (CDT)')


# ax.legend()
fig.tight_layout()
fig.figure.savefig(save_fig_path+'\\'+'Babs_ssa.pdf', dpi=300)



# %% absorption ratio hitogram
interval_hist_average = 600
datalake.reaverage_datastreams(interval_hist_average, stream_keys=['ratios'])
datalake_dust1.reaverage_datastreams(interval_hist_average, stream_keys=['ratios'])
datalake_dust2.reaverage_datastreams(interval_hist_average, stream_keys=['ratios'])
datalake_fire.reaverage_datastreams(interval_hist_average, stream_keys=['ratios'])

range = (0.6, 2.5)
bins = 30
fig, ax = plt.subplots()

plot.histogram(
    ax,
    datalake_dust1,
    'ratios',
    'ratio_babs_wet_dry',
    'Dust 17th',
    bins=bins,
    range=range,
    color=colors['dust1'],
    kwargs={'alpha': 0.6,
            'density': True}
)
plot.histogram(
    ax,
    datalake_dust2,
    'ratios',
    'ratio_babs_wet_dry',
    'Dust 21st',
    bins=bins,
    range=range,
    color=colors['dust2'],
    kwargs={'alpha': 0.75,
            'density': True}
)
plot.histogram(
    ax,
    datalake_fire,
    'ratios',
    'ratio_babs_wet_dry',
    'Combustion 12th',
    bins=bins,
    range=range,
    color=colors['fire'],
    kwargs={'alpha': 0.6,
            'density': True}
)
plot.histogram(
    ax,
    datalake,
    'ratios',
    'ratio_babs_wet_dry',
    'All',
    bins=bins,
    range=range,
    color=colors['gray_light'],
    kwargs={'alpha': 1,
            'density': True,
            'fill': False,
            'edgecolor': colors['gray_light'],
            'linewidth': 2}
)
ax.set_xlabel('Absorption Ratio (RH 84% / RH 54%) [1/Mm]')
ax.set_ylabel('Probability Density')
# ax.grid()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'absorption_ratio.pdf', dpi=300)

#%% albdeo ratio hitogram
range = (0.94, 1.08)
bins = 30
fig, ax = plt.subplots()

plot.histogram(
    ax,
    datalake_dust1,
    'ratios',
    'albedo_ratio',
    'Dust 17th',
    bins=bins,
    range=range,
    color=colors['dust1'],
    kwargs={'alpha': 0.6,
            'density': True}
)
plot.histogram(
    ax,
    datalake_dust2,
    'ratios',
    'albedo_ratio',
    'Dust 21st',
    bins=bins,
    range=range,
    color=colors['dust2'],
    kwargs={'alpha': 0.75,
            'density': True}
)
plot.histogram(
    ax,
    datalake_fire,
    'ratios',
    'albedo_ratio',
    'Combustion 12th',
    bins=bins,
    range=range,
    color=colors['fire'],
    kwargs={'alpha': 0.6,
            'density': True}
)
plot.histogram(
    ax,
    datalake,
    'ratios',
    'albedo_ratio',
    'All',
    bins=bins,
    range=range,
    color=colors['gray_light'],
    kwargs={'alpha': 1,
            'density': True,
            'fill': False,
            'edgecolor': colors['gray_light'],
            'linewidth': 2}
)
ax.set_xlabel('Albedo Ratio (RH 84% / RH 54%) [1/Mm]')
ax.set_ylabel('Probability Density')
# ax.grid()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'albedo_ratio.pdf', dpi=300)

# %% hgf kappa histogram 

range = (0.0, 0.6)
bins = 30
fig, ax = plt.subplots()

plot.histogram(
    ax,
    datalake_dust1,
    'CAPS_data',
    'kappa_fit',
    'Dust 17th',
    bins=bins,
    range=range,
    color=colors['dust1'],
    kwargs={'alpha': 0.6,
            'density': True}
)
plot.histogram(
    ax,
    datalake_dust2,
    'CAPS_data',
    'kappa_fit',
    'Dust 21st',
    bins=bins,
    range=range,
    color=colors['dust2'],
    kwargs={'alpha': 0.75,
            'density': True}
)
plot.histogram(
    ax,
    datalake_fire,
    'CAPS_data',
    'kappa_fit',
    'Combustion 12th',
    bins=bins,
    range=range,
    color=colors['fire'],
    kwargs={'alpha': 0.6,
            'density': True}
)
plot.histogram(
    ax,
    datalake,
    'CAPS_data',
    'kappa_fit',
    'All',
    bins=bins,
    range=range,
    color=colors['gray_light'],
    kwargs={'alpha': 1,
            'density': True,
            'fill': False,
            'edgecolor': colors['gray_light'],
            'linewidth': 2}
)
ax.set_xlabel('Hygroscopic Parameter $\kappa_{HGF}$')
ax.set_ylabel('Probability Density')
# ax.grid()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'kappa_hist.pdf', dpi=300)


# %% inorganic mass fraction histogram

range = (0, .03)
bins = 30
key_name = 'bc_mass_fraction'
fig, ax = plt.subplots()

plot.histogram(
    ax,
    datalake_dust1,
    'ratios',
    key_name,
    'Dust 17th',
    bins=bins,
    range=range,
    color=colors['dust1'],
    kwargs={'alpha': 0.6,
            'density': True}
)
plot.histogram(
    ax,
    datalake_dust2,
    'ratios',
    key_name,
    'Dust 21st',
    bins=bins,
    range=range,
    color=colors['dust2'],
    kwargs={'alpha': 0.75,
            'density': True}
)
plot.histogram(
    ax,
    datalake_fire,
    'ratios',
    key_name,
    'Combustion 12th',
    bins=bins,
    range=range,
    color=colors['fire'],
    kwargs={'alpha': 0.6,
            'density': True}
)
plot.histogram(
    ax,
    datalake,
    'ratios',
    key_name,
    'All',
    bins=bins,
    range=range,
    color=colors['gray_light'],
    kwargs={'alpha': 1,
            'density': True,
            'fill': False,
            'edgecolor': colors['gray_light'],
            'linewidth': 2}
)
ax.set_xlabel('$BC/PM_{1}$ Mass Fraction')
ax.set_ylabel('Probability Density')
# ax.grid()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'bc_fraction_hist.pdf', dpi=300)


# %% inorganic mass fraction histogram


range = (0.1, 0.5)
bins = 30
key_name = 'inorganic_mass_fraction'
fig, ax = plt.subplots()

plot.histogram(
    ax,
    datalake_dust1,
    'ratios',
    key_name,
    'Dust 17th',
    bins=bins,
    range=range,
    color=colors['dust1'],
    kwargs={'alpha': 0.6,
            'density': True}
)
plot.histogram(
    ax,
    datalake_dust2,
    'ratios',
    key_name,
    'Dust 21st',
    bins=bins,
    range=range,
    color=colors['dust2'],
    kwargs={'alpha': 0.65,
            'density': True}
)
plot.histogram(
    ax,
    datalake_fire,
    'ratios',
    key_name,
    'Combustion 12th',
    bins=bins,
    range=range,
    color=colors['fire'],
    kwargs={'alpha': 0.6,
            'density': True}
)
# plot.histogram(
#     ax,
#     datalake,
#     'ratios',
#     key_name,
#     'All',
#     bins=bins,
#     range=range,
#     color=colors['gray_light'],
#     kwargs={'alpha': 0.2,
#             'density': True}
# )
ax.set_xlabel('Inorganic/$PM{1}$ Mass Fraction')
ax.set_ylabel('Probability Density')
# ax.grid()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'inorganic_hist.pdf', dpi=300)


# %% mass remaining fraction histogram

range = (0.5, 0.9)
bins = 30
key_name = 'remainder_mass_fraction'
fig, ax = plt.subplots()

plot.histogram(
    ax,
    datalake_dust1,
    'ratios',
    key_name,
    'Dust 17th',
    bins=bins,
    range=range,
    color=colors['dust1'],
    kwargs={'alpha': 0.7,
            'density': True}
)
plot.histogram(
    ax,
    datalake_dust2,
    'ratios',
    key_name,
    'Dust 21st',
    bins=bins,
    range=range,
    color=colors['dust2'],
    kwargs={'alpha': 0.65,
            'density': True}
)
plot.histogram(
    ax,
    datalake_fire,
    'ratios',
    key_name,
    'Combustion 12th',
    bins=bins,
    range=range,
    color=colors['fire'],
    kwargs={'alpha': 0.6,
            'density': True}
)
plot.histogram(
    ax,
    datalake,
    'ratios',
    key_name,
    'All',
    bins=bins,
    range=range,
    color=colors['gray_light'],
    kwargs={'alpha': 1,
            'density': True,
            'fill': False,
            'edgecolor': colors['gray_light'],
            'linewidth': 2}
)
ax.set_xlabel('Unaccounted for Mass / $PM{1}$ Mass Fraction')
ax.set_ylabel('Probability Density')
# ax.grid()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'remainder_hist.pdf', dpi=300)


# %% plot bc mass 

fig, ax = plt.subplots()

plot.timeseries(
    ax,
    datalake,
    'sp2',
    'BC_mass[ug/m3]',
    'BC mass',
    color=colors['BC'],
    shade=False,
    line_kwargs={'linestyle': '--'})
ax.set_xlim((timeseries_left, timeseries_right))

# ax2 = ax.twinx()
# plot.timeseries(
#     ax2,
#     datalake,
#     'spams',
#     'mass_SO4[ug/m3]',
#     'sulfate mass',
#     color=colors['sulfate'],
#     shade=False,
# )
# ax2.set_ylabel('Aerosol Optical Depth (500 nm)', color=colors['AOD_puerto_rico'])
# # ax2.set_ylim(bottom=0, top=1)
# ax2.tick_params(axis='y', labelcolor=colors['AOD_puerto_rico'], color=colors['AOD_puerto_rico'])
# ax2.spines['right'].set_color(colors['AOD_puerto_rico'])

ax.set_ylabel('BC Mass ($\mu g/m^3$)')
# ax.set_ylim(bottom=0, top=0.5)
# ax.set_xlim((timeseries_left, timeseries_right))
ax.xaxis.set_major_locator(dates.DayLocator(interval=2, tz=tracer_timezone))
ax.xaxis.set_major_formatter(dates.DateFormatter('%d', tz=tracer_timezone))
ax.set_xlabel('July Day 2022 (CDT)')
ax.grid()
# ax.legend()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'bc_mass.pdf', dpi=300)

# %%
