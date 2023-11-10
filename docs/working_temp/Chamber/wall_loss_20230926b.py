# %%
# todo acount for coagulation and dillution

import numpy as np
from matplotlib import pyplot as plt, dates
import pytz
from scipy.optimize import curve_fit
import os
from datetime import datetime
import json
from datacula.lake.datalake import DataLake
from datacula.lake import processer, plot
from datacula import loader
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

#%%
data_path = 'F:\\CloudChamber\\exp20230926b'
# make plots folder
plot_path = os.path.join(data_path, 'plots')
if not os.path.exists(plot_path):
    os.makedirs(plot_path)


# epoch_start = datetime.fromisoformat('2023-04-16T00:00').timestamp()
# epoch_end = datetime.fromisoformat('2023-04-28T00:00').timestamp()


# # load json file with lake settings
# lake_settings_path = os.path.join(data_path, 'lake_settings.json')

settings = {
    "SMPS_data": {
        "instrument_name": "LANL_SMPS_data",
        "data_stream_name": ["smps_1D", "smps_2D"],
        "data_processing_function": "SMPS_processing",
        "data_loading_function": "general_2d_sizer_load",
        "relative_data_folder": "SMPS_data",
        "last_file_processed": "",
        "last_timestamp_processed": "",
        "skipRowsDict": 0,
        "Time_shift_sec": 0,
        "timezone_identifier": "US/Mountain",
        "data_checks": {
            "characters": [200],
            "skip_rows": 24,
            "skip_end": 0,
            "char_counts": {"/": 2, ":": 2}
        },
        "data_sizer_reader": {
            "header_rows": 24,
            "Dp_start_keyword": "Diameter Midpoint (nm)", 
            "Dp_end_keyword": "Scan Time (s)",
            "list_of_data_headers": [
                "Lower Size (nm)", 
                "Upper Size (nm)",
                "Sample Temp (C)",
                "Sample Pressure (kPa)",
                "Relative Humidity (%)",
                "Median (nm)",
                "Mean (nm)",
                "Geo. Mean (nm)",
                "Mode (nm)",
                "Geo. Std. Dev.",
                "Total Conc. (#/cm³)"]
            },
        "data_header": [
            "Lower_Size_(nm)",
            "Upper_Size_(nm)",
            "Sample_Temp_(C)",
            "Sample_Pressure_(kPa)",
            "Relative_Humidity_(%)",
            "Median_(nm)",
            "Mean_(nm)",
            "Geo_Mean_(nm)",
            "Mode_(nm)",
            "Geo_Std_Dev.",
            "Total_Conc_(#/cc)"
        ],
        "time_column": [1,2], 
        "time_format": "%m/%d/%Y %H:%M:%S",
        "filename_regex": "*.csv",
        "base_interval_sec": 180,
        "data_delimiter": ","
        }
}


# Initialize the data lake
my_lake = DataLake(settings=settings, path=data_path)

my_lake.update_datastream()
my_lake.info()
# %% calculate mean properties of smps 2d data 
my_lake = processer.sizer_mean_properties(my_lake, "smps_2D")


# %% Plot and find time range

# chamber flow rates
chamber_push = 1.2 # L/min
chamber_dillution = 1.2 # L/min

CHAMBER_VOLUME = 900 # L

k_rate_chamber_min = chamber_push/CHAMBER_VOLUME
k_rate_chamber_hr = k_rate_chamber_min * 60

# time range
time_format = "%m/%d/%Y %H:%M:%S"
epoch_start = time_str_to_epoch('09/26/2023 14:0:00', time_format, 'US/Mountain')
epoch_end = time_str_to_epoch('09/27/2023 06:00:00', time_format, 'US/Mountain')
# epoch_start = datetime.fromisoformat('2023-09-26T013:00').timestamp()
# epoch_end = datetime.fromisoformat('2023-09-26T15:00').timestamp()
timezone = pytz.timezone('US/Mountain')


my_lake.reaverage_datastreams(60*5, epoch_start=epoch_start, epoch_end=epoch_end)

fig, ax = plt.subplots()
plot.timeseries(
    ax,
    my_lake,
    'smps_1D',
    'Total_Conc_(#/cc)',
    label='number total',
    shade=True)

ax.minorticks_on()
plt.tick_params(rotation=-25)
ax.set_ylabel('Particle Number(#/cm³)')
# ax.set_xlim((epoch_start, epoch_end))

ax.xaxis.set_major_formatter(dates.DateFormatter('%d %H:%M', tz=timezone))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d', tz=mdt_timezone))
# ax.set_ylim((0,2000))
ax.grid()
ax.legend()
fig.tight_layout()
fig.savefig(plot_path + '\\smps_total_number.png', dpi=300)
# %% plot smps 2d data

size_bins = np.array(my_lake.datastreams['smps_2D'].header_list, dtype=float)
conc = my_lake.datastreams['smps_2D'].return_data()
time_sec = my_lake.datastreams['smps_2D'].return_time()
rebase_time_hour = (time_sec - time_sec[0])/3600  # rebase and convert to hours


fig, ax = plt.subplots()

ax.plot(size_bins, conc[:,3], label='start')
ax.plot(size_bins, conc[:,-5], label='end')

# ax.set_xscale('log')
ax.set_yscale('log')
ax.set_xlabel('Diameter (nm)')
ax.set_ylabel('Particle Number(#/cm³)') 
# ax.set_ylim((1e-1,1e4))
ax.set_xlim((10,700))
ax.grid()
ax.legend()
fig.tight_layout()

# plot 2d smps data
concentration = conc
concentration = np.where(concentration < 1e-5, 1e-5, concentration)
concentration = np.where(concentration > 10**5, 10**5, concentration)
# concentration = np.log10(concentration)

fig, ax = plt.subplots(1,1)
plt.contourf(
    rebase_time_hour,
    size_bins,
    concentration,
    cmap=plt.cm.PuBu_r, levels=50)
ax.set_yscale('log')
ax.set_xlabel('Experiment Time (hr)')
ax.set_ylabel('Diameter (nm)')
plt.colorbar(label='Concentration [#/cm3]', ax=ax)
plt.show()
fig.tight_layout()
fig.savefig(plot_path + '\\smps_2d.png', dpi=300)



# %% plot decay rate for two selected bins


# Find the index of the bin closest to 50
bin50_index = np.argmin(np.abs(size_bins - 100))
bin125_index = np.argmin(np.abs(size_bins - 400))

fig, ax = plt.subplots()
ax.plot(rebase_time_hour, conc[bin50_index,:], label='100 nm')
ax.plot(rebase_time_hour, conc[bin125_index,:], label='400 nm')
ax.set_yscale('log')
ax.set_xlabel('Time (hr)')
ax.set_ylabel('Particle Number(#/cm³)')
ax.grid()
ax.legend()
fig.tight_layout()
# %% fit exponential decay to 125 nm bin


def exp_decay(t, Q, N0, k_apparent):
    return N0 * np.exp(-t*k_apparent+Q)

# clean up Nans and fit exponential decay
def fit_exp_decay(func, x, y, p0):
    # remove nans
    mask = ~np.isnan(y)
    x = x[mask]
    y = y[mask]
    # fit
    popt, pcov = curve_fit(func, x, y, p0=p0)
    return popt, pcov

# fit exponential decay, test that it is working
popt, pcov = fit_exp_decay(
    exp_decay,
    rebase_time_hour,
    conc[bin125_index,:],
    p0=[1,1,1])

# stats of fit
N0 = popt[1]
k_apparent = popt[2]
N0_err = np.sqrt(pcov[1,1])
k_apparent_err = np.sqrt(pcov[2,2])
print(f'N0 = {N0:.2f} +/- {N0_err:.2f}')
print(f'k_apparent = {k_apparent:.2f} +/- {k_apparent_err:.2f}')
# r squared 
residuals = conc[bin125_index,:] - exp_decay(rebase_time_hour, *popt)
ss_res = np.sum(residuals**2)
ss_tot = np.sum((conc[bin125_index,:] - np.mean(conc[bin125_index,:]))**2)
r_squared = 1 - (ss_res / ss_tot)
print(f'r squared = {r_squared:.2f}')


# %% repeat fit for each bin in the smps 2d data 

# initialize arrays
N0_array = np.zeros_like(size_bins)
k_apparent_array = np.zeros_like(size_bins)
N0_err_array = np.zeros_like(size_bins)
k_apparent_err_array = np.zeros_like(size_bins)
r_squared_array = np.zeros_like(size_bins)

# loop through each bin
for bin, bin_value in enumerate(size_bins):
    # fit exponential decay
    popt, pcov = fit_exp_decay(
        exp_decay,
        rebase_time_hour,
        conc[bin,:],
        p0=[1,0, 1])
    # stats of fit
    N0_array[bin] = popt[1]
    k_apparent_array[bin] = popt[2]
    N0_err_array[bin] = np.sqrt(pcov[1,1])
    k_apparent_err_array[bin] = np.sqrt(pcov[2,2])
    # r squared 
    residuals = conc[bin,:] - exp_decay(rebase_time_hour, *popt)
    ss_res = np.sum(residuals**2)
    ss_tot = np.sum((conc[bin,:] - np.mean(conc[bin,:]))**2)
    r_squared_array[bin] = 1 - (ss_res / ss_tot)

# save fit parameters to csv
np.savetxt(
    os.path.join(plot_path, 'decay_rate.csv'),
    np.vstack((size_bins, N0_array, k_apparent_array, N0_err_array, k_apparent_err_array, r_squared_array)).T,
    delimiter=',',
    header='size_bins, N0, k_apparent, N0_err, k_apparent_err, r_squared')



# %% plot decay rate stacked plot with size distribution

fig, ax = plt.subplots(ncols=1, nrows=2, sharex=True, figsize=(8,8))

ax[0].plot(size_bins, conc[:,5], label='Start')
ax[0].plot(size_bins, conc[:,-1], label='End')
ax[0].set_yscale('log')
ax[0].set_ylabel('Particle Number(#/cm³)')
ax[0].grid()
ax[0].legend()


ax[1].plot(size_bins, k_apparent_array, label='$k_{apparent}$')
ax[1].fill_between(
    size_bins,
    k_apparent_array - k_apparent_err_array,  k_apparent_array + k_apparent_err_array,
    alpha=0.25,
    label=None)
ax[1].plot(size_bins, np.ones_like(size_bins)*k_rate_chamber_hr,
           label='Chamber ($k_{airflow}$)',
           color='k',
           linestyle='--')
# ax.set_xscale('log')
# ax.set_yscale('log')
ax[1].set_xlabel('Diameter (nm)')
ax[1].set_ylabel('k apparent decay rate (1/hr)')
ax[1].set_xlim((10, 800))
ax[1].set_ylim(bottom=-2, top=10)
ax[1].grid()
ax[1].legend()
fig.tight_layout()
fig.savefig(plot_path + '\\decay_rate.png', dpi=300)


# %%
