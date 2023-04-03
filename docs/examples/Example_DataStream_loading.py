# %%
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import numpy as np
from matplotlib import pyplot as plt, dates
from datacula.lake.datastream import DataStream
from datacula import loader
import os


plt.rcParams["figure.figsize"] = (10,8)
plt.rcParams["font.size"] = (16)

# %%

settings_cpc = {
        "instrument_name": "CPC_3010",
        "data_stream_name": "CPC_3010",
        "data_loading_function": "general_load",
        "relative_data_folder": "CPC_3010_data",
        "skipRowsDict": 0,
        "Time_shift_to_Linux_Epoch_sec": 0,
        "data_checks": {
            "characters": [20,35],
            "char_counts": {",": 4, "/": 0, ":": 0},
            "skip_rows": 1,
            "skip_end": 0
        },
        "data_header": ["CPC_count[#/sec]", "Temp_[C]"],
        "data_column": [1,2],
        "time_column": 0,
        "time_format": "epoch",
        "filename_regex": "CPC_3010*.csv",
        "base_interval_sec": 2,
        "data_delimiter": ","
    }

# %%
# set the parent directory of the data folder, for now this is the same as the current working directory
# but this can be a completely different path
current_path = os.getcwd()

# or like this
# path = "U:\\data\\processing\\Campgain2023_of_aswsome\\"

#%% Now we need to load a single data file
data_file = os.path.join(current_path, 'data', settings_cpc['relative_data_folder'], 'CPC_3010_data_20220701_Jul.csv')

print(data_file)

# load the data
raw_data = loader.data_raw_loader(data_file)

# print the first 5 rows
print(raw_data[:5])

# %% Now we format the data and clean it up.
# This is done by the general_data_formatter function for timeseries data
# 2d data is a separate function

data = loader.data_format_checks(raw_data, settings_cpc['data_checks'])

# Sample the data to get the epoch times and the data
epoch_time, data_array = loader.sample_data(
    data=data,
    time_column=settings_cpc['time_column'],
    time_format=settings_cpc['time_format'],
    data_columns=settings_cpc['data_column'],
    delimiter=settings_cpc['data_delimiter'],
    date_offset=None,
    seconds_shift=settings_cpc['Time_shift_to_Linux_Epoch_sec'],
)

print(f"epoch_time: {epoch_time.shape}")
print(epoch_time[:5])
print(f"data_array: {data_array.shape}")
print(data_array[:5])

# %% These data checks and formatting are combined in 'general_data_formatter'

epoch_time, data = loader.general_data_formatter(
    data=raw_data,
    data_checks=settings_cpc['data_checks'],
    data_column=settings_cpc['data_column'],
    time_column=settings_cpc['time_column'],
    time_format=settings_cpc['time_format'],
    delimiter=settings_cpc['data_delimiter'],
    date_offset=None,
    seconds_shift=settings_cpc['Time_shift_to_Linux_Epoch_sec']
)

# Transpose the data
# data = data.T

# %% Now we can create a DataStream object

# Initialize the datastream object
cpc_datastream = DataStream(
                    header_list=settings_cpc['data_header'],
                    average_times=[600],
                    average_base=settings_cpc['base_interval_sec']
                )

# %% add the data to the datastream
# first transpose the data
data = data.T

cpc_datastream.add_data(
            time_stream=epoch_time,
            data_stream=data,
        )
# %%
# what do we have in the datastream?
print(cpc_datastream.return_header_list())

# %%

fig, ax = plt.subplots()
ax.plot(
    cpc_datastream.return_time(datetime64=True),
    cpc_datastream.return_data(keys=['CPC_count[#/sec]'])[0],
    label='CPC data'
)
plt.tick_params(rotation=-35)
ax.set_ylabel('CPC_count[#/sec]')
ax.xaxis.set_major_formatter(dates.DateFormatter('%d %Hhr'))
ax.legend()

#%% we can get the raw data, but not with formatted time currently

fig, ax = plt.subplots()
ax.plot(
    cpc_datastream.return_time(datetime64=False, raw=True),
    cpc_datastream.return_data(keys=['CPC_count[#/sec]'], raw=True)[0],
    label='CPC data'
)
plt.tick_params(rotation=-35)
ax.set_ylabel('CPC_count[#/sec]')
ax.legend()



# %% 

# epoch_start = datetime.fromisoformat('2022-06-30T19:00').timestamp()
# epoch_end = datetime.fromisoformat('2022-08-01T05:00').timestamp()

# # %% 
# # truncation processing
# datalake.reaverage_datalake(300, epoch_start=epoch_start, epoch_end=epoch_end)

# datalake = dlp.pass3_processing(
#     datalake=datalake,
#     babs_405_532_781=[1, 1, 1],
#     bsca_405_532_781=[1.37, 1.2, 1.4],
# )

# # datalake = dlp.caps_processing(
# #     datalake=datalake,
# #     truncation_bsca=False,
# #     truncation_interval_sec=600,
# #     truncation_interp=True,
# #     refractive_index=1.5,
# #     calibration_wet=0.97,
# #     calibration_dry=1.003
# # )

# # DataLake_saveNload(path, datalake=datalake)


# # %%
# datalake.reaverage_datalake(3*300)

# save_fig_path = os.path.join(path, "plots")

# fig, ax = plt.subplots()
# ax.plot(
#     datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
#     datalake.datastreams['CAPS_dual'].return_data(keys=['Bext_wet_CAPS_450nm[1/Mm]'])[0],
#     label='Extinction wet'
# )
# ax.plot(
#     datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
#     datalake.datastreams['CAPS_dual'].return_data(keys=['Bext_dry_CAPS_450nm[1/Mm]'])[0],
#     label='Extinction dry'
# )
# ax.plot(
#     datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
#     datalake.datastreams['CAPS_dual'].return_data(keys=['Bsca_wet_CAPS_450nm[1/Mm]'])[0],
#     label='Scattering wet'
# )
# ax.plot(
#     datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
#     datalake.datastreams['CAPS_dual'].return_data(keys=['Bsca_dry_CAPS_450nm[1/Mm]'])[0],
#     label='Scattering dry'
# )
# ax.set_ylim(0,200)
# plt.tick_params(rotation=-35)
# ax.set_ylabel('Extinction or Scattering [1/Mm]')
# ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.legend()
# fig.savefig(save_fig_path+'\\'+'CAPS_dual.png', dpi=300)

# # %%
# datalake = dlp.albedo_processing(datalake=datalake)

# fig, ax = plt.subplots()
# ax.plot(
#     datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
#     datalake.datastreams['CAPS_dual'].return_data(keys=['SSA_wet_CAPS_450nm[1/Mm]'])[0],
#     label='wet'
# )
# ax.plot(
#     datalake.datastreams['CAPS_dual'].return_time(datetime64=True),
#     datalake.datastreams['CAPS_dual'].return_data(keys=['SSA_dry_CAPS_450nm[1/Mm]'])[0],
#     label='dry'
# )
# plt.tick_params(rotation=-35)
# ax.set_ylabel('Single Scattering Albedo')
# ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.set_ylim(0.8,1.1)
# ax.legend()
# fig.savefig(save_fig_path+'\\'+'CAPS_dual_SSA.png', dpi=300)

# print('wet mean: ', np.nanmean(datalake.datastreams['CAPS_dual'].return_data(keys=['SSA_wet_CAPS_450nm[1/Mm]'])[0]))
# print('dry mean: ', np.nanmean(datalake.datastreams['CAPS_dual'].return_data(keys=['SSA_dry_CAPS_450nm[1/Mm]'])[0]))


# # %%
# datalake.reaverage_datalake(60*10)

# # PASS
# pass_save_keys = ['Bsca405nm[1/Mm]', 'Bsca532nm[1/Mm]', 'Bsca781nm[1/Mm]']
# datastream_to_csv(datalake.datastreams['pass3'], path, header_keys=pass_save_keys, time_shift_sec=0, filename='PASS3_CDT')
# datastream_to_csv(datalake.datastreams['pass3'], path, header_keys=pass_save_keys, time_shift_sec=-3600, filename='PASS3_CST')

# # # CDT
# # datalake_to_csv(datalake=datalake, path=path, time_shift_sec=0, sufix_name='CDT')
# # # CST
# # datalake_to_csv(datalake=datalake, path=path, time_shift_sec=-3600, sufix_name='CST')
# # %%


# %%
