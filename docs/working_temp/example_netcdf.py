# %%

import numpy as np
import netCDF4 as nc
import datetime
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
        print(var, nc_file.variables[var].shape, nc_file.variables[var].dtype)
    print("\nHeaders:")
    for attr in nc_file.ncattrs():
        print(attr, "=", getattr(nc_file, attr))
    nc_file.close()

netcdf_info_print(file_path)
# %%


# %%



time_epoch = nc_file.variables.get('base_time')[0]+nc_file.variables.get('time_offset')[:]
