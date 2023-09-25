# %%

import numpy as np
import netCDF4 as nc
import datetime
import datacula.convert as convert
# %%

file_path = "D:\\Tracer\\working_folder\\raw_data\\ARM_aos_aps\\houaosapsM1.b1.20220627.145726.nc"

# %% 

def netcdf_info_print(file_path):
    nc_file = nc.Dataset(file_path)
    print("Dimensions:")
    for dim in nc_file.dimensions:
        print(dim, len(nc_file.dimensions[dim]))
    print("\nVariables:")
    for var in nc_file.variables:
        print(var,
              nc_file.variables[var].shape,
              nc_file.variables[var].dtype)
    print("\nHeaders:")
    for attr in nc_file.ncattrs():
        print(attr, "=", getattr(nc_file, attr))
    nc_file.close()

netcdf_info_print(file_path)
# %%
instrument_settings = {"ARM_aps": {
        "instrument_name": "AOS_aps_data",
        "data_stream_name": ["aos_aps_1D", "aos_aps_2D"],
        "data_processing_function": "SMPS_processing",
        "data_loading_function": "netcdf_2d_sizer_load",
        "relative_data_folder": "ARM_aos_aps",
        "last_file_processed": "",
        "last_timestamp_processed": "",
        "Time_shift_to_Linux_Epoch_sec": 0,
        "netcdf_reader": {
            "data_2d": "dN_dlogDp",
            "header_2d": 'diameter_aerodynamic',
            "data_1d": ['total_N_conc',
                        'total_SA_conc'],
            "header_1d": ['concentration_N_cm3',
                          'surface_area_concentration_nm_cm3'],
            },
        "time_column": ['base_time', 'time_offset'],
        "time_format": "epoch",
        "filename_regex": "*.nc",
        "base_interval_sec": 1,
        "data_delimiter": ","
        }
    }
# %%
settings = instrument_settings['ARM_aps']

nc_file = nc.Dataset(file_path)

epoch_time = np.zeros(nc_file.dimensions['time'].size)

for time_col in settings['time_column']:
    epoch_time += nc_file.variables.get(time_col)[:]

epoch_time = np.array(epoch_time.astype(float))

# load 1D data
data_1d = np.zeros(
    (len(settings['netcdf_reader']['data_1d']),
     nc_file.dimensions['time'].size)
     )
for i, data_col in enumerate(settings['netcdf_reader']['data_1d']):
    data = nc_file.variables.get(data_col)[:]
    data_1d[i, :] = np.ma.filled(data.astype(float), np.nan)

header_1d = settings['netcdf_reader']['header_1d']

data_1d = convert.data_shape_check(
    time=epoch_time,
    data=data_1d,
    header=header_1d)


# load 2D data
data_2d = nc_file.variables.get(settings['netcdf_reader']['data_2d'])[:]
# convert masked array to numpy array
data_2d = np.ma.filled(data_2d.astype(float), np.nan)
# get header
header_2d = nc_file.variables.get(settings['netcdf_reader']['header_2d'])[:]
header_2d = [str(item) for item in header_2d.tolist()]

data_2d = convert.data_shape_check(
    time=epoch_time,
    data=data_2d,
    header=header_2d)


# %%
