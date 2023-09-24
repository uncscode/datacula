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
import datalake_plot as dlplot
import os

plt.rcParams.update({'text.color': "#333333",
                     'axes.labelcolor': "#333333",
                     "figure.figsize": (5,4),
                     "font.size": 14,
                     "axes.edgecolor": "#333333",
                     "axes.labelcolor": "#333333",
                     "xtick.color": "#333333",
                     "ytick.color": "#333333",
                     "pdf.fonttype": 42,
                     "ps.fonttype": 42})

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
    "dust": '#754C24',
    "passRed": '#BE1E2D',
    "passGreen": '#009444',
    "passBlue": '#00AEEF',
}
# %%
# settings_path = "C:\\Users\\253393\\Documents\\GitHub\\CAFE-processing\\server\\dev\\server_data_settings.json"
settings_path = "U:\\Areas\\code_repos\\CAFE-processing\\server\\dev\\server_data_settings.json"
# settings_path = "C:\\Users\\kkgor\\OneDrive\\Documents\\GitHub\\CAFE-processing\\server\\dev\\server_data_settings.json"

#load json file
with open(settings_path, 
            'r', encoding='utf-8'
            ) as json_file:
                settings = json.load(json_file)

# path = "C:\\Users\\253393\\Desktop\\hard_drive_backup\\working_folder\\raw_data\\"
path = "U:\\Projects\\TRACER_analysis\\working_folder\\raw_data"
# path = "U:\\code_repos\\CAFE-processing\\server\\server_files\\"
# path = "C:\\Users\\kkgor\\OneDrive\\Documents\\GitHub\\CAFE-processing\\server\\server_files\\"
# settings = get_server_data_settings()

#%%
keys_subset = ["picarro_data", "PASS3_data", "CAPS_dual_data", "SMPS_data", "SPAMS_data", "APS_data", "CCNc", "SP2_data"]
# ["PASS3_data", "picarro_data","SP2_data", "SPAMS_data", "CAPS_dual_data", "SMPS_data", "APS_data", "CCNc"]
simple_settings = {key: settings[key] for key in keys_subset}

datalake = DataLake(simple_settings, path)

# %%
datalake.update_datastream()
datalake.remove_zeros()

epoch_start = datetime.fromisoformat('2022-06-30T19:00').timestamp()
epoch_end = datetime.fromisoformat('2022-07-31T00:00').timestamp()


# %% 
# truncation processing
datalake.reaverage_datalake(300, epoch_start=epoch_start, epoch_end=epoch_end)

datalake = dlp.pass3_processing(
    datalake=datalake,
    babs_405_532_781=[1, 1, 1],
    bsca_405_532_781=[1.37, 1.2, 1.4],
)

datalake = dlp.caps_processing(
    datalake=datalake,
    truncation_bsca=False,
    truncation_interval_sec=600,
    truncation_interp=True,
    refractive_index=1.5,
    calibration_wet=0.97,
    calibration_dry=1.003
)

datalake = dlp.sizer_mean_properties(datalake)

DataLake_saveNload(path, datalake=datalake, sufix_name='ARM')

# %% Save and graph
datalake = DataLake_saveNload(path, sufix_name='ARM')

save_fig_path = "U:\\Projects\\TRACER_analysis\\working_folder\\raw_data\\ARM_output"

#%%


datalake.reaverage_datalake(60*10)


# PASS
pass_save_keys = ['Bsca405nm[1/Mm]', 'Bsca532nm[1/Mm]', 'Bsca781nm[1/Mm]']
datastream_to_csv(datalake.datastreams['pass3'], save_fig_path, header_keys=pass_save_keys, time_shift_sec=0, filename='PASS3_CDT')
datastream_to_csv(datalake.datastreams['pass3'], save_fig_path, header_keys=pass_save_keys, time_shift_sec=-3600, filename='PASS3_CST')

# GHG
picarro_save_keys = ['CO2[ppm]calibrated', 'CO[ppm]', 'CH4[ppm]','H2O[ppm]']
datastream_to_csv(datalake.datastreams['picarro'], save_fig_path, header_keys=picarro_save_keys, time_shift_sec=0, filename='GHG_CDT')
datastream_to_csv(datalake.datastreams['picarro'], save_fig_path, header_keys=picarro_save_keys, time_shift_sec=-3600, filename='GHG_CST')

# CAPS
caps_save_keys = ['Bext_wet_CAPS_450nm[1/Mm]', 'Bsca_wet_CAPS_450nm[1/Mm]', 'Bext_dry_CAPS_450nm[1/Mm]', 'Bsca_dry_CAPS_450nm[1/Mm]', 'Wet_RH_postCAPS[%]', 'Wet_Temp_postCAPS[C]', 'dualCAPS_inlet_RH[%]', 'dualCAPS_inlet_Temp[C]']
datastream_to_csv(datalake.datastreams['CAPS_dual'], save_fig_path, header_keys=caps_save_keys, time_shift_sec=0, filename='HCAPS_CDT')
datastream_to_csv(datalake.datastreams['CAPS_dual'], save_fig_path, header_keys=caps_save_keys, time_shift_sec=-3600, filename='HCAPS_CST')

# smps
smps_1d = ['Relative_Humidity_(%)', 'Mean_(nm)', 'Geo_Mean_(nm)', 'Geo_Std_Dev.', 'Total_Conc_(#/cc)']
datastream_to_csv(datalake.datastreams['smps_1D'], save_fig_path, header_keys=smps_1d, time_shift_sec=0, filename='SMPS_1D_CDT')
datastream_to_csv(datalake.datastreams['smps_1D'], save_fig_path, header_keys=smps_1d, time_shift_sec=-3600, filename='SMPS_1D_CST')

datastream_to_csv(datalake.datastreams['smps_2D'], save_fig_path, header_keys=None, time_shift_sec=0, filename='SMPS_2D_CDT')
datastream_to_csv(datalake.datastreams['smps_2D'], save_fig_path, header_keys=None, time_shift_sec=-3600, filename='SMPS_2D_CST')

# aps
aps_1d = ['Mean_(um)', 'Geo_Mean_(um)', 'Geo_Std_Dev']
datastream_to_csv(datalake.datastreams['aps_1D'], save_fig_path, header_keys=aps_1d, time_shift_sec=0, filename='APS_1D_CDT')
datastream_to_csv(datalake.datastreams['aps_1D'], save_fig_path, header_keys=aps_1d, time_shift_sec=-3600, filename='APS_1D_CST')

datastream_to_csv(datalake.datastreams['aps_2D'], save_fig_path, header_keys=None, time_shift_sec=0, filename='APS_2D_CDT')
datastream_to_csv(datalake.datastreams['aps_2D'], save_fig_path, header_keys=None, time_shift_sec=-3600, filename='APS_2D_CST')

# SPAMS
spams_save_keys = [
    'total_mass[ug/m3]',
    'mass_Chl[ug/m3]',
    'mass_NH4[ug/m3]',
    'mass_NO3[ug/m3]',
    'mass_SO4[ug/m3]',
    'mass_OC[ug/m3]']
datastream_to_csv(datalake.datastreams['spams'], save_fig_path, header_keys=spams_save_keys, time_shift_sec=0, filename='SPAMS_CDT')
datastream_to_csv(datalake.datastreams['spams'], save_fig_path, header_keys=spams_save_keys, time_shift_sec=-3600, filename='SPAMS_CST')

# CCNc
ccnc_save_keys = ['CCN_Concentration_[#/cc]', 'CurrentSuperSaturationSet[%]', 'SampleFlow_[cc/min]', 'SheathFlow_[cc/min]']
datastream_to_csv(datalake.datastreams['CCNc'], save_fig_path, header_keys=ccnc_save_keys, time_shift_sec=0, filename='CCNc_CDT')
datastream_to_csv(datalake.datastreams['CCNc'], save_fig_path, header_keys=ccnc_save_keys, time_shift_sec=-3600, filename='CCNc_CST')

# sp2
sp2_save_keys = ['BC_mass[ug/m3']
datastream_to_csv(datalake.datastreams['sp2'], save_fig_path, header_keys=sp2_save_keys, time_shift_sec=0, filename='SP2_CDT')
datastream_to_csv(datalake.datastreams['sp2'], save_fig_path, header_keys=sp2_save_keys, time_shift_sec=-3600, filename='SP2_CST')

#  'size_properties'

# %%
plot_start = datetime.fromisoformat('2022-06-30T00:00')
plot_end = datetime.fromisoformat('2022-08-02T00:00')


# picarro
fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'picarro',
    'CO2[ppm]calibrated',
    '$CO_2$',
    shade=False,
    color=colors['gray_dark'],
)

ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('$CO_2$ (ppm)')
ax.set_xlim((plot_start, plot_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
ax.set_ylim((400,800))
ax.grid()
ax.legend()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'GHG_CO2.png', dpi=300)

# picarro
fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'picarro',
    'CO[ppm]',
    '$CO$',
    shade=False,
    color=colors['gray_dark'],
)
dlplot.timeseries(
    ax,
    datalake,
    'picarro',
    'CH4[ppm]',
    '$CH_4$',
    shade=False,
    color=colors['gray_light'],
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('$CO$ and $CH_4$ (ppm)')
ax.set_xlim((plot_start, plot_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
# ax.set_ylim((0,150))
ax.grid()
ax.legend()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'GHG_CO.png', dpi=300)

# %%
datalake.reaverage_datalake(3600*4)

# PASS
fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'pass3',
    'Bsca781nm[1/Mm]',
    '$B_{sca.}$ 781 nm',
    shade=False,
    color=colors['passRed'],
)
dlplot.timeseries(
    ax,
    datalake,
    'pass3',
    'Bsca532nm[1/Mm]',
    '$B_{sca.}$ 532 nm',
    shade=True,
    color=colors['passGreen'],
)
dlplot.timeseries(
    ax,
    datalake,
    'pass3',
    'Bsca405nm[1/Mm]',
    '$B_{sca.}$ 405 nm',
    shade=True,
    color=colors['passBlue'],
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Scattering coefficient (1/Mm)')
ax.set_xlim((plot_start, plot_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
ax.set_ylim((0,150))
ax.grid()
ax.legend()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'pass3.png', dpi=300)



# %%

# CAPS
fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'Bext_wet_CAPS_450nm[1/Mm]',
    '$B_{ext.}$ 450 nm (wetter)',
    shade=False,
    color=colors['CAPS_wet'],
)

dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'Bext_dry_CAPS_450nm[1/Mm]',
    '$B_{ext.}$ 450 nm (dryer)',
    shade=False,
    color=colors['CAPS_dry'],
)

ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Extinction coefficient (1/Mm)')
ax.set_xlim((plot_start, plot_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
ax.set_ylim((0,200))
ax.grid()
ax.legend()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'capsExt.png', dpi=300)


fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'Bsca_wet_CAPS_450nm[1/Mm]',
    'raw $B_{sca.}$ 450 nm (wetter)',
    shade=True,
    color=colors['CAPS_wet'],
)

dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'Bsca_dry_CAPS_450nm[1/Mm]',
    'raw $B_{sca.}$ 450 nm (dryer)',
    shade=True,
    color=colors['CAPS_dry'],
)

ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Scattering coefficient (1/Mm)')
ax.set_xlim((plot_start, plot_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
ax.set_ylim((0,200))
ax.grid()
ax.legend()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'capsSca.png', dpi=300)
# %%
# SMPS
fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'smps_1D',
    'Mean_(nm)',
    'Mean (nm)',
    shade=False,
    color=colors['gray_dark'],
)
dlplot.timeseries(
    ax,
    datalake,
    'smps_1D',
    'Geo_Mean_(nm)',
    'Geometric mean (nm)',
    shade=False,
    color=colors['gray_light'],
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Diameter (nm)')
ax.set_xlim((plot_start, plot_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.set_ylim((0,200))
ax.grid()
ax.legend()
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'smps_1D.png', dpi=300)

# %%
# APS
fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'aps_1D',
    'Mean_(um)',
    'Mean (um)',
    shade=False,
    color=colors['gray_dark'],
)
dlplot.timeseries(
    ax,
    datalake,
    'aps_1D',
    'Geo_Mean_(um)',
    'Geometric mean (um)',
    shade=False,
    color=colors['gray_light'],
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Diameter (microns)')
ax.set_xlim((plot_start, plot_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.set_ylim((0,200))
ax.grid()
ax.legend()
fig.tight_layout()

fig.savefig(save_fig_path+'\\'+'aps_1D.png', dpi=300)

# %%
# SPAMS and SP2
fig, ax = plt.subplots(figsize=(8, 4))
dlplot.timeseries(
    ax,
    datalake,
    'spams',
    'mass_SO4[ug/m3]',
    'Sulfate',
    shade=False,
    color=colors['sulfate'],
)
dlplot.timeseries(
    ax,
    datalake,
    'spams',
    'mass_Chl[ug/m3]',
    'Chloride',
    shade=False,
    color=colors['chloride'],
)
dlplot.timeseries(
    ax,
    datalake,
    'spams',
    'mass_NH4[ug/m3]',
    'Ammonium',
    shade=False,
    color=colors['ammonium'],
)
dlplot.timeseries(
    ax,
    datalake,
    'spams',
    'mass_NO3[ug/m3]',
    'Nitrate',
    shade=False,
    color=colors['nitrate'],
)
dlplot.timeseries(
    ax,
    datalake,
    'spams',
    'mass_OC[ug/m3]',
    'Organic carbon',
    shade=False,
    color=colors['organic'],
)
dlplot.timeseries(
    ax,
    datalake,
    'sp2',
    'BC_mass[ug/m3',
    'Black Carbon',
    shade=False,
    color=colors['gray_dark'],
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Species mass (ug/m3)')
ax.set_xlim((plot_start, plot_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.set_yscale('log')
ax.grid()
ax.legend(bbox_to_anchor=(1.05,1))
fig.tight_layout()
fig.savefig(save_fig_path+'\\'+'masses.png', dpi=300)

# %%
