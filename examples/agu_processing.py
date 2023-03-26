# %%
import numpy as np
from matplotlib import pyplot as plt, dates
from data_lake_tester import DataLake_saveNload
import datalake_processer as dlp


plt.rcParams["figure.figsize"] = (10,8)
plt.rcParams["font.size"] = (16)

# %%
path = "C:\\Users\\253393\\Desktop\\hard_drive_backup\\working_folder\\raw_data\\"
# path = "U:\\DATA\\2022\\TRACER\\working_folder\\raw_data\\"
# path = "U:\\code_repos\\CAFE-processing\\server\\server_files\\"
# path = "C:\\Users\\kkgor\\OneDrive\\Documents\\GitHub\\CAFE-processing\\server\\server_files\\"
# settings = get_server_data_settings()

datalake = DataLake_saveNload(path)

# %%
datalake.reaverage_datalake(average_base_sec=90)

datalake = dlp.sizer_mean_properties(datalake)
datalake = dlp.ccnc_hygroscopicity(datalake)
datalake = dlp.albedo_processing(datalake)



# %%
datalake.datastreams['spams'].return_header_dict()

# %%
datalake.reaverage_datalake(stream_keys=['spams'], average_base_sec=300)

# load ams data and estimate kappa

kappa_ref_ccn = {"mass_Chl[ug/m3]": 1.28,
            "mass_NH4[ug/m3]": 0.67,
            "mass_NO3[ug/m3]": 0.88,
            "mass_SO4[ug/m3]": 0.65,
            "mass_OC[ug/m3]": 0.1}

kappa_ref_hgf = {"mass_Chl[ug/m3]": 1.12,
            "mass_NH4[ug/m3]": 0.51,
            "mass_NO3[ug/m3]": 0.80,
            "mass_SO4[ug/m3]": 0.51,
            "mass_OC[ug/m3]": 0.1}

time = datalake.datastreams['spams'].return_time(datetime64=False)
total_mass = datalake.datastreams['spams'].return_data(keys=['total_mass[ug/m3]'])[0]

kappa_ccn = np.zeros_like(total_mass)
kappa_hgf = np.zeros_like(total_mass)

for key in kappa_ref.keys():
    kappa_ccn += datalake.datastreams['spams'].return_data(keys=[key])[0]*kappa_ref_ccn[key]/total_mass
    kappa_hgf += datalake.datastreams['spams'].return_data(keys=[key])[0]*kappa_ref_hgf[key]/total_mass

datalake.add_processed_datastream(
        key='kappa_ams',
        time_stream=time,
        data_stream= np.vstack((kappa_ccn,kappa_hgf)),
        data_stream_header=['kappa_ccn', 'kappa_hgf'],
        average_times=[300],
        average_base=[300]
        )

fig, ax = plt.subplots()
ax.plot(kappa_ccn)
ax.plot(kappa_hgf)
ax.set_ylabel('kappa_ams')
ax.set_xlabel('time')
# ax.minorticks_on()
plt.tick_params(rotation=-35)
# ax.xaxis.set_major_formatter(dates.DateFormatter('%m/%d'))
# ax.set_ylim((0, 1.2))
ax.grid()

# %%
datalake.reaverage_datalake(average_base_sec=3600*5)

DataLake_saveNload(path, datalake=datalake, sufix_name='agu')
# %%
