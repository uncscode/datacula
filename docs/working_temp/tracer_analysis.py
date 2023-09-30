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

path = "D:\\Tracer\\working_folder\\raw_data"


#%%

datalake = loader.load_datalake(path=path, sufix_name='full_raw')

datalake.info()
datalake.remove_zeros()


# %% 
time_format = "%m/%d/%Y %H:%M:%S"
tracer_timezone = pytz.timezone('US/Central')
epoch_start = time_str_to_epoch('07/10/2022 00:00:00', time_format, 'US/Central')
epoch_end = time_str_to_epoch('07/25/2022 00:00:00', time_format, 'US/Central')
datalake.reaverage_datastreams(300, epoch_start=epoch_start, epoch_end=epoch_end)


datalake.remove_outliers(
    datastreams_keys=['arm_aeronet_sda'],
    outlier_headers=['total_aod_500nm[tau]'],
    mask_value=-999.0000
)
datalake.remove_outliers(
    datastreams_keys=['CAPS_data'],
    outlier_headers=['Bext_dry_CAPS_450nm[1/Mm]'],
    mask_top=200,
)

datalake = processer.caps_processing(
    datalake=datalake,
    truncation_bsca=True,
    truncation_interval_sec=600,
    truncation_interp=True,
    refractive_index=1.5,
    calibration_wet=0.955,
    calibration_dry=1.003
)
datalake.reaverage_datastreams(60)
datalake = processer.albedo_processing(datalake=datalake)

loader.save_datalake(path=path, data_lake=datalake, sufix_name='processed_caps')

#%%
datalake.reaverage_datastreams(300, epoch_start=epoch_start, epoch_end=epoch_end)

datalake = processer.merge_smps_ops_datastreams(
    datalake=datalake,
    lower_key='smps_2D',
    upper_key='aps_2D',
    new_key='merged_size_dist',
)
datalake = processer.merge_smps_ops_datastreams(
    datalake=datalake,
    lower_key='smps_2D',
    upper_key='aos_aps_2D',
    new_key='aos_merged_size_dist',
)

datalake = processer.sizer_mean_properties(
    datalake=datalake,
    stream_key='merged_size_dist',
    new_key='merged_mean_properties',
    diameter_multiplier_to_nm=1,
)
datalake = processer.sizer_mean_properties(
    datalake=datalake,
    stream_key='aos_merged_size_dist',
    new_key='aos_merged_mean_properties',
    diameter_multiplier_to_nm=1,
)
datalake = processer.sizer_mean_properties(
    datalake=datalake,
    stream_key='smps_2D',
    new_key='smps_mean_properties',
    diameter_multiplier_to_nm=1,
)

datalake = processer.sizer_mean_properties(
    datalake=datalake,
    stream_key='aps_2D',
    new_key='lanl_aps_mean_properties',
    diameter_multiplier_to_nm=1000,
)
datalake = processer.sizer_mean_properties(
    datalake=datalake,
    stream_key='aos_aps_2D',
    new_key='aos_aps_mean_properties',
    diameter_multiplier_to_nm=1000,
)


# %% average

datalake.remove_outliers(
    datastreams_keys=['merged_mean_properties', 'smps_mean_properties'],
    outlier_headers=['Unit_Mass_(ug/m3)_PM10', 'Unit_Mass_(ug/m3)_PM2.5'],
    mask_top=200,
    mask_bottom=0
)

datalake.remove_outliers(
    datastreams_keys=['merged_mean_properties', 'smps_mean_properties'],
    outlier_headers=['Unit_Mass_(ug/m3)_PM10', 'Unit_Mass_(ug/m3)_PM2.5'],
    mask_top=200,
    mask_bottom=0
)

# %% ratios

datalake.reaverage_datastreams(300, epoch_start=epoch_start, epoch_end=epoch_end)

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


# %% save

loader.save_datalake(path=path, data_lake=datalake, sufix_name='processed')

print('done')
# %%