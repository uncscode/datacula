# %%
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import numpy as np
from matplotlib import pyplot as plt, dates
from data_lake_tester import DataLake_saveNload
import datalake_processer as dlp
import datalake_plot as dlplot
from datetime import datetime, timedelta
import os


plt.rcParams.update({'text.color' : "#333333",
                     'axes.labelcolor' : "#333333",
                     "figure.figsize": (6,4),
                     "font.size": 14,
                     "axes.edgecolor": "#333333",
                     "axes.labelcolor": "#333333",
                     "xtick.color": "#333333",
                     "ytick.color": "#333333",
                     "xtick.labelcolor": "#333333",
                     "ytick.labelcolor": "#333333",})

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

# %%
path = "C:\\Users\\253393\\Desktop\\hard_drive_backup\\working_folder\\raw_data\\"
# path = "U:\\DATA\\2022\\TRACER\\working_folder\\raw_data\\"
# path = "U:\\code_repos\\CAFE-processing\\server\\server_files\\"
# path = "C:\\Users\\kkgor\\OneDrive\\Documents\\GitHub\\CAFE-processing\\server\\server_files\\"
# settings = get_server_data_settings()

datalake = DataLake_saveNload(path, sufix_name='agu')
datalake.reaverage_datalake(average_base_sec=3600*5)

save_fig_path = os.path.join(path, "plots")

epoch_start = datetime.fromisoformat('2022-06-30T00:00')
epoch_end = datetime.fromisoformat('2022-08-02T00:00')


# %%
# datalake.datastreams['size_properties'].return_header_dict()
fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'size_properties',
    'Unit_Mass_(ugPm3)_smps',
    'SMPS denisty=1',
    shade=True,
    color=colors['gray_dark'],
)
dlplot.timeseries(
    ax, datalake,
    'size_properties',
    'Mass_(ugPm3)_smps',
    'SMPS denisty=1.5',
    shade=False,
    color=colors['gray_light']
)
dlplot.timeseries(
    ax,
    datalake,
    'spams',
    'total_mass[ug/m3]',
    'AMS total mass',
    shade=True,
    color=colors['sulfate'],
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Particle Mass (ug/m3)')
ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
ax.set_ylim((0,50))
ax.grid()
ax.legend()
fig.savefig(save_fig_path+'\\'+'unint_mass.pdf', dpi=300)

fig, ax = plt.subplots()
# dlplot.timeseries(
#     ax,
#     datalake,
#     'size_properties',
#     'Mass_(ugPm3)_PM2.5',
#     '$PM_{2.5}$',
#     shade=False,
#     color=colors['gray_light']
# )
dlplot.timeseries(
    ax,
    datalake,
    'size_properties',
    'Mass_(ugPm3)_PM10',
    '$PM_{10}$',
    shade=True,
    color=colors['dust']
)
dlplot.timeseries(
    ax,
    datalake,
    'size_properties',
    'Mass_(ugPm3)_smps',
    '$PM_{800 nm}$',
    shade=True,
    color=colors['gray_dark'],
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Particle Mass (ug/m3)')
ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
ax.set_ylim((0,50))
ax.grid()
ax.legend()
fig.savefig(save_fig_path+'\\'+'mass.pdf', dpi=300)

# N100
fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'size_properties',
    'Total_Conc_(#/cc)_N100',
    'N100',
    shade=True,
    color=colors['gray_dark']
)
ax.minorticks_on()
ax.set_yscale('log')
plt.tick_params(rotation=-35)
ax.set_ylabel('N100 (#/cc Dp<100nm)')
ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d'))
ax.set_ylim((4e2,1e5))
ax.grid()
ax.legend()
fig.savefig(save_fig_path+'\\'+'N100.pdf', dpi=300)

# %%
# plot caps
# datalake.datastreams['CAPS_dual'].return_header_dict()
# RH = datalake.datastreams['CAPS_dual'].return_data(keys=['dualCAPS_inlet_RH[%]','Wet_RH_preCAPS[%]','Wet_RH_postCAPS[%]'])

fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'Bext_wet_CAPS_450nm[1/Mm]',
    label='Bext RH=84%', 
    color=colors['CAPS_wet']
)
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'Bext_dry_CAPS_450nm[1/Mm]',
    label='Bext RH=54%',
    color=colors['CAPS_dry']
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Extinction (1/Mm)')
ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
ax.set_ylim((0,150))
ax.grid()
ax.legend()
fig.savefig(save_fig_path+'\\'+'CAPSext.pdf', dpi=300)

fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'Bsca_wet_CAPS_450nm[1/Mm]',
    label='Bsca RH=84%',
    color=colors['CAPS_wet']
)
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'Bsca_dry_CAPS_450nm[1/Mm]',
    label='Bsca RH=54%',
    color=colors['CAPS_dry']
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Scattering (1/Mm)')
ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
ax.set_ylim((0,150))
ax.grid()
ax.legend()
fig.savefig(save_fig_path+'\\'+'CAPSsca.pdf', dpi=300)


fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'Babs_wet_CAPS_450nm[1/Mm]',
    label='Babs RH=84%',
    color=colors['CAPS_wet']
)
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'Babs_dry_CAPS_450nm[1/Mm]',
    label='Babs RH=54%',
    color=colors['CAPS_dry']
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Absorption (1/Mm)')
ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
ax.set_ylim((0,15))
ax.grid()
ax.legend()
fig.savefig(save_fig_path+'\\'+'CAPSabs.pdf', dpi=300)


fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'SSA_wet_CAPS_450nm[1/Mm]',
    label='SSA RH=84%',
    color=colors['CAPS_wet']
)
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'SSA_dry_CAPS_450nm[1/Mm]',
    label='SSA RH=54%',
    color=colors['CAPS_dry']
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Single Scattering Albedo')
ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
ax.set_ylim((0.7,1.05))
ax.grid()
ax.legend()
fig.savefig(save_fig_path+'\\'+'CAPSssa.pdf', dpi=300)


# %%
# datalake.datastreams['CAPS_dual'].return_header_dict()
# datalake.reaverage_datalake(average_base_sec=3600*6)

fig, ax = plt.subplots()
dlplot.timeseries(
    ax,
    datalake,
    'kappa_ccn',
    'kappa_CCNc',
    label='CCNc $\kappa_{CCN}$',
    shade_kwargs={'alpha':0.1},
    color=colors['kappa_ccn']
)
dlplot.timeseries(
    ax,
    datalake,
    'CAPS_dual',
    'kappa_fit',
    label='CAPS $\kappa_{HGF}$',
    color=colors['CAPS_wet']
)
dlplot.timeseries(
    ax,
    datalake,
    'kappa_ams',
    'kappa_ccn',
    label='AMS est. $\kappa_{CCN}$',
    color=colors['kappa_amsCCN']
)
dlplot.timeseries(
    ax,
    datalake,
    'kappa_ams',
    'kappa_hgf',
    label='AMS est. $\kappa_{HGF}$',
    color=colors['kappa_amsHGF']
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_xlim((epoch_start, epoch_end))
ax.set_ylabel('Retrived $\kappa$')
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
ax.set_ylim((0, 1.2))
ax.grid()
ax.legend()
fig.savefig(save_fig_path+'\\'+'kappa.pdf', dpi=300)
# %% ams data
# datalake.datastreams['spams'].return_header_dict()
total_mass = datalake.datastreams['spams'].return_data(keys=['total_mass[ug/m3]'])[0]
mass_frac_chl = datalake.datastreams['spams'].return_data(keys=['mass_Chl[ug/m3]'])[0]/total_mass
mass_frac_NH4 = datalake.datastreams['spams'].return_data(keys=['mass_NH4[ug/m3]'])[0]/total_mass
mass_frac_NO3 = datalake.datastreams['spams'].return_data(keys=['mass_NO3[ug/m3]'])[0]/total_mass
mass_frac_SO4 = datalake.datastreams['spams'].return_data(keys=['mass_SO4[ug/m3]'])[0]/total_mass
mass_frac_org = datalake.datastreams['spams'].return_data(keys=['mass_OC[ug/m3]'])[0]/total_mass

combo = [mass_frac_chl, mass_frac_NH4, mass_frac_NO3, mass_frac_SO4, mass_frac_org]
#%%
time = datalake.datastreams['spams'].return_time(datetime64=True)
fig, ax = plt.subplots()
ax.stackplot(
    time,
    combo,
    labels=['Chl', 'NH4', 'NO3', 'SO4', 'OC'],
    colors=[colors['chloride'], colors['ammonium'], colors['nitrate'], colors['sulfate'], colors['organic']]
)
ax.minorticks_on()
plt.tick_params(rotation=-35)
ax.set_ylabel('Mass Fraction (AMS)')
ax.set_xlim((epoch_start, epoch_end))
ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
ax.set_ylim((0, 1))
ax.grid()
ax.legend()
fig.savefig(save_fig_path+'\\'+'mass_frac.pdf', dpi=300)

# %%
# historgam of absoprtion enhancement
datalake.reaverage_datalake(stream_keys=['CAPS_dual'],average_base_sec=60)

babs_wet = datalake.datastreams['CAPS_dual'].return_data(keys=['Babs_wet_CAPS_450nm[1/Mm]'])[0]
babs_dry = datalake.datastreams['CAPS_dual'].return_data(keys=['Babs_dry_CAPS_450nm[1/Mm]'])[0]
ratio = babs_wet - babs_dry

albedo_ratio = datalake.datastreams['CAPS_dual'].return_data(keys=['SSA_wet_CAPS_450nm[1/Mm]'])[0] - datalake.datastreams['CAPS_dual'].return_data(keys=['SSA_dry_CAPS_450nm[1/Mm]'])[0]

# %%
fig, ax = plt.subplots()
ax.hist(ratio, bins=150, range=(-2,2), alpha=1, label='Absorption', color=colors['gray_light'])
ax.set_xlabel('Absorption Delta (RH 84% - RH 54%) [1/Mm]')
ax.set_ylabel('Occurance')
ax.grid()
fig.savefig(save_fig_path+'\\'+'absorption_enhancement.pdf', dpi=300)


fig, ax = plt.subplots()
ax.hist(albedo_ratio, bins=150, range=(-0.08,0.08), alpha=1, label='Albedo', color=colors['gray_light'])
ax.set_xlabel('SSA Delta (RH 84% - RH 54%)')
ax.set_ylabel('Occurance')
ax.grid()
fig.savefig(save_fig_path+'\\'+'SSA_enhancement.pdf', dpi=300)
# %%


