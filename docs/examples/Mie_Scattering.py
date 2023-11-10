# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

# %% imports
import numpy as np
from matplotlib import pyplot as plt, dates
import os
from datetime import datetime
import json
from datacula.lake import processer, plot
from datacula import loader
from datacula import mie, convert

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

diameters = np.array([20.72, 21.1, 21.48, 21.87, 22.27, 22.67, 23.08, 23.5, 23.93, 24.36, 24.8, 25.25, 25.71, 26.18, 26.66, 27.14, 27.63, 28.13, 28.64, 29.16, 29.69, 30.23, 30.78, 31.34, 31.91, 32.49, 33.08, 33.68, 34.29, 34.91, 35.55, 36.19, 36.85, 37.52, 38.2, 38.89, 39.6, 40.32, 41.05, 41.79, 42.55, 43.32, 44.11, 44.91, 45.73, 46.56, 47.4, 48.26, 49.14, 50.03, 50.94, 51.86, 52.8, 53.76, 54.74, 55.73, 56.74, 57.77, 58.82, 59.89, 60.98, 62.08, 63.21, 64.36, 65.52, 66.71, 67.93, 69.16, 70.41, 71.69, 72.99, 74.32, 75.67, 77.04, 78.44, 79.86, 81.31, 82.79, 84.29, 85.82, 87.38, 88.96, 90.58, 92.22, 93.9, 95.6, 97.34, 99.1, 100.9, 102.74, 104.6, 106.5, 108.43, 110.4, 112.4, 114.44, 116.52, 118.64, 120.79, 122.98, 125.21, 127.49, 129.8, 132.16, 134.56, 137, 139.49, 142.02, 144.6, 147.22, 149.89, 152.61, 155.38, 158.2, 161.08, 164, 166.98, 170.01, 173.09, 176.24, 179.43, 182.69, 186.01, 189.38, 192.82, 196.32, 199.89, 203.51, 207.21, 210.97, 214.8, 218.7, 222.67, 226.71, 230.82, 235.01, 239.28, 243.62, 248.05, 252.55, 257.13, 261.8, 266.55, 271.39, 276.32, 281.33, 286.44, 291.64, 296.93, 302.32, 307.81, 313.4, 319.08, 324.88, 330.77, 336.78, 342.89, 349.12, 355.45, 361.9, 368.47, 375.16, 381.97, 388.91, 395.96, 403.15, 410.47, 417.92, 425.51, 433.23, 441.09, 449.1, 457.25, 465.55, 474, 482.61, 491.37, 500.29, 509.37, 518.61, 528.03, 537.61, 547.37, 557.31, 567.42, 577.72, 588.21, 598.89, 609.76, 620.82, 632.09, 643.57, 655.25, 667.14, 679.25, 691.58, 704.14, 716.92, 729.93, 743.18, 756.67, 770.4, 784.39
])
conc_dndlog = np.array([1628.661333, 1237.850333, 1312.560667, 1396.598667, 1562.333333, 2221.477667, 1887.682, 1620.7, 1171.867667, 1452.842333, 1084.224, 1631.826667, 1355.552, 1466.396333, 1279.601333, 1415.206333, 1434.472, 688.8536667, 1105.573667, 1123.884667, 1830.002333, 1141.062, 749.317, 1520.124333, 1317.009333, 1377.588333, 981.6533333, 1057.794, 899.8853333, 801.8286667, 1185.694333, 677.925, 1306.826333, 1081.935, 1252.723, 1089.714667, 1183.771, 824.2226667, 860.3903333, 782.2536667, 726.239, 949.1466667, 918.7173333, 706.9903333, 1097.509, 1075.060333, 939.9303333, 910.1483333, 1310.311333, 2474.571, 1614.624, 1409.965667, 1287.382, 1436.442333, 946.969, 1029.993, 922.0113333, 919.6213333, 1047.388667, 1280.208333, 835.7116667, 1116.051667, 1036.240667, 1125.934333, 886.1316667, 1201.813, 1079.959, 1051.972333, 1319.007, 804.7473333, 646.5173333, 822.0936667, 740.2216667, 998.0976667, 823.7053333, 726.8793333, 648.2256667, 997.785, 982.954, 590.1273333, 938.1996667, 715.878, 652.2743333, 851.375, 504.3193333, 738.537, 596.685, 1161.830333, 598.4103333, 622.7626667, 722.138, 549.288, 568.502, 505.297, 542.535, 356.1263333, 403.0913333, 682.3673333, 674.8073333, 206.2043333, 589.9626667, 550.597, 504.794, 525.3013333, 653.9386667, 624.9763333, 437.0223333, 529.0596667, 661.79, 584.823, 651.8466667, 889.1946667, 845.2976667, 607.376, 824.872, 593.3293333, 490.041, 756.359, 659.8826667, 453.524, 850.96, 666.3823333, 786.5156667, 840.4793333, 475.325, 670.4016667, 636.4576667, 671.109, 640.608, 607.0906667, 946.8646667, 632.0093333, 489.311, 571.646, 593.1726667, 867.4076667, 1046.078, 414.0566667, 648.9716667, 701.8066667, 500.1283333, 457.9413333, 758.4313333, 899.0716667, 638.8583333, 580.2453333, 613.63, 573.9886667, 292.3913333, 488.2983333, 327.8596667, 441.1033333, 466.464, 488.787, 270.9563333, 328.2393333, 246.864, 144.3253333, 352.18, 589.7026667, 359.431, 186.396, 105.2196667, 147.8496667, 175.701, 365.877, 321.11, 216.476, 161.8316667, 170.7116667, 155.874, 58.11633333, 145.681, 36.739, 111.1586667, 149.5086667, 161.2806667, 179.6003333, 115.0893333, 193.4996667, 130.5713333, 104.5536667, 119.1563333, 40.06633333, 0, 220.2246667, 147.9093333, 214.8063333, 201.5626667, 42.17966667, 68.212, 145.6563333, 43.24433333, 119.6856667, 231.0246667, 88.63233333, 44.67366667, 45.03066667, 45.387, 45.735, 46.08633333, 92.85633333, 46.77133333
])




conc_dn = convert.convert_sizer_dn(diameters, conc_dndlog)

fig, ax = plt.subplots()
ax.plot(diameters, conc_dndlog)
# ax.set_xscale('log')
ax.set_yscale('log')
ax.set_xlabel('Diameter (nm)')
ax.set_ylabel('dN/dlogD (cm$^{-3}$)')
ax.set_title('Aerosol size distribution')
ax.grid(True)
plt.show()

# %% mass concentration
density_BC = 1.85 # g/cm3
density_sulfate = 1.77 # g/cm3

total_concentration, unit_mass_ugPm3, mean_diameter_nm, mean_vol_diameter_nm, geometric_mean_diameter_nm, mode_diameter, mode_diameter_mass = processer.distribution_mean_properties(
    sizer_dndlogdp=conc_dndlog,
    sizer_diameter=diameters,
    total_concentration=None,
    sizer_limits=[0,100]
)
print(f'BC mass ' + str(unit_mass_ugPm3 * density_BC) + ' ug/m3')

total_concentration, unit_mass_ugPm3_sulfate, mean_diameter_nm, mean_vol_diameter_nm, geometric_mean_diameter_nm, mode_diameter, mode_diameter_mass = processer.distribution_mean_properties(
    sizer_dndlogdp=conc_dndlog,
    sizer_diameter=diameters,
    total_concentration=None,
    sizer_limits=[100,320]
)
print(f'Sulfate mass ' + str(unit_mass_ugPm3_sulfate * density_sulfate) + ' ug/m3')

# %%
# lambda m] nhom +ikhom     niox +ikiox             nhos +ikhos             nacc +ikacc                 p1
# 0.450 1.572+i2.400 · 10−3,  2.786+i2.995 · 10−1,  1.505+i8.607 · 10−4,  1.539+i5.037 · 10−3,  3.802 · 10+1
# BC 1.95 + 0.96i       density 1.85, 0.110449205 ug/m3 0-90

# sulfate 1.55 + 0.0i   density 1.77, 2.7222 ug/m3 91-152
# dust 153to end

water_activity = np.linspace(0.5, 0.90, 20)

def optical_coefficients(
        kappa,
        particle_counts,
        diameters,
        water_activity_sizer,
        water_activity_dry,
        water_activity_wet_vector,
        refractive_index_dry=1.45,
        water_refractive_index=1.33,
        wavelength=450,
    ):
    """
    Calculate the ratio of optical properties for wet and dry aerosols.
    """
    absorption_dry = np.zeros_like(water_activity_wet_vector)
    extinction_dry = np.zeros_like(water_activity_wet_vector)
    scattering_dry = np.zeros_like(water_activity_wet_vector)
    albedo_dry = np.zeros_like(water_activity_wet_vector)
    absorption_wet = np.zeros_like(water_activity_wet_vector)
    extinction_wet = np.zeros_like(water_activity_wet_vector)
    scattering_wet = np.zeros_like(water_activity_wet_vector)
    albedo_wet = np.zeros_like(water_activity_wet_vector)

    for index, activity in enumerate(water_activity_wet_vector):

        optics_wet, optics_dry = mie.extinction_ratio_wet_dry(
            kappa=kappa,
            particle_counts=particle_counts,
            diameters=diameters,
            water_activity_sizer=water_activity_sizer,
            water_activity_dry=water_activity_dry,
            water_activity_wet=activity,
            refractive_index_dry=refractive_index_dry,
            water_refractive_index=water_refractive_index,
            wavelength=wavelength,
            discretize_Mie=False,
            return_coefficients=True,
            return_all_optics=True,
        )

        absorption_dry[index] =  (optics_dry[2])
        extinction_dry[index] =  (optics_dry[0])
        scattering_dry[index] = (optics_dry[1])
        albedo_dry[index] = (optics_dry[1]/optics_dry[0])
        absorption_wet[index] = (optics_wet[2])
        extinction_wet[index] =  (optics_wet[0])
        scattering_wet[index] =  (optics_wet[1])
        albedo_wet[index] = (optics_wet[1]/optics_wet[0]) 

    return absorption_dry, extinction_dry, scattering_dry, albedo_dry, absorption_wet, extinction_wet, scattering_wet, albedo_wet
#%%
# soot
absorption_dry_soot, extinction_dry_soot, scattering_dry_soot, albedo_dry_soot, absorption_wet_soot, extinction_wet_soot, scattering_wet_soot, albedo_wet_soot = optical_coefficients(
    kappa=0.1,
    particle_counts=conc_dn[0:90],
    diameters=diameters[0:90],
    water_activity_sizer=0.1,
    water_activity_dry=0.5,
    water_activity_wet_vector=water_activity,
    refractive_index_dry=1.95+0.96*1j,
    water_refractive_index=1.33,
    wavelength=450,
    )

# sulfate
absorption_dry_sulfate, extinction_dry_sulfate, scattering_dry_sulfate, albedo_dry_sulfate, absorption_wet_sulfate, extinction_wet_sulfate, scattering_wet_sulfate, albedo_wet_sulfate = optical_coefficients(
    kappa=0.60,
    particle_counts=conc_dn[91:152],
    diameters=diameters[91:152],
    water_activity_sizer=0.1,
    water_activity_dry=0.5,
    water_activity_wet_vector=water_activity,
    refractive_index_dry=1.55+0.000001*1j,
    water_refractive_index=1.33,
    wavelength=450,
    )

# dust 01
absorption_dry_dust01, extinction_dry_dust01, scattering_dry_dust01, albedo_dry_dust01, absorption_wet_dust01, extinction_wet_dust01, scattering_wet_dust01, albedo_wet_dust01 = optical_coefficients(
    kappa=0.05,
    particle_counts=conc_dn[153:],
    diameters=diameters[153:],
    water_activity_sizer=0.1,
    water_activity_dry=0.5,
    water_activity_wet_vector=water_activity,
    refractive_index_dry=1.6+0.01*1j,
    water_refractive_index=1.33,
    wavelength=450,
    )

# dust 1
absorption_dry_dust1, extinction_dry_dust1, scattering_dry_dust1, albedo_dry_dust1, absorption_wet_dust1, extinction_wet_dust1, scattering_wet_dust1, albedo_wet_dust1 = optical_coefficients(
    kappa=0.05,
    particle_counts=conc_dn[153:],
    diameters=diameters[153:],
    water_activity_sizer=0.1,
    water_activity_dry=0.5,
    water_activity_wet_vector=water_activity,
    refractive_index_dry=1.6+0.1*1j,
    water_refractive_index=1.33,
    wavelength=450,
    )

# dust_kappa1
absorption_dry_dust_kappa1, extinction_dry_dust_kappa1, scattering_dry_dust_kappa1, albedo_dry_dust_kappa1, absorption_wet_dust_kappa1, extinction_wet_dust_kappa1, scattering_wet_dust_kappa1, albedo_wet_dust_kappa1 = optical_coefficients(
    kappa=0.05,
    particle_counts=conc_dn[153:],
    diameters=diameters[153:],
    water_activity_sizer=0.1,
    water_activity_dry=0.5,
    water_activity_wet_vector=water_activity,
    refractive_index_dry=1.6+0.07*1j,
    water_refractive_index=1.33,
    wavelength=450,
    )



#%%

dust_01_absorption_dry = absorption_dry_dust01+absorption_dry_soot+absorption_dry_sulfate
dust_01_extinction_dry = extinction_dry_dust01+extinction_dry_soot+extinction_dry_sulfate
dust_01_scattering_dry = scattering_dry_dust01+scattering_dry_soot+scattering_dry_sulfate
dust_01_albedo_dry = dust_01_scattering_dry/dust_01_extinction_dry
dust_01_absorption_wet = absorption_wet_dust01+absorption_wet_soot+absorption_wet_sulfate
dust_01_extinction_wet = extinction_wet_dust01+extinction_wet_soot+extinction_wet_sulfate
dust_01_scattering_wet = scattering_wet_dust01+scattering_wet_soot+scattering_wet_sulfate
dust_01_albedo_wet = dust_01_scattering_wet/dust_01_extinction_wet

dust1_absorption_dry = absorption_dry_dust1+absorption_dry_soot+absorption_dry_sulfate
dust1_extinction_dry = extinction_dry_dust1+extinction_dry_soot+extinction_dry_sulfate
dust1_scattering_dry = scattering_dry_dust1+scattering_dry_soot+scattering_dry_sulfate
dust1_albedo_dry = dust1_scattering_dry/dust1_extinction_dry
dust1_absorption_wet = absorption_wet_dust1+absorption_wet_soot+absorption_wet_sulfate
dust1_extinction_wet = extinction_wet_dust1+extinction_wet_soot+extinction_wet_sulfate
dust1_scattering_wet = scattering_wet_dust1+scattering_wet_soot+scattering_wet_sulfate
dust1_albedo_wet = dust1_scattering_wet/dust1_extinction_wet

dustkappa1_absorption_dry = absorption_dry_dust_kappa1+absorption_dry_soot+absorption_dry_sulfate
dustkappa1_extinction_dry = extinction_dry_dust_kappa1+extinction_dry_soot+extinction_dry_sulfate
dustkappa1_scattering_dry = scattering_dry_dust_kappa1+scattering_dry_soot+scattering_dry_sulfate
dustkappa1_albedo_dry = dustkappa1_scattering_dry/dustkappa1_extinction_dry
dustkappa1_absorption_wet = absorption_wet_dust_kappa1+absorption_wet_soot+absorption_wet_sulfate
dustkappa1_extinction_wet = extinction_wet_dust_kappa1+extinction_wet_soot+extinction_wet_sulfate
dustkappa1_scattering_wet = scattering_wet_dust_kappa1+scattering_wet_soot+scattering_wet_sulfate
dustkappa1_albedo_wet = dustkappa1_scattering_wet/dustkappa1_extinction_wet



# %%

fig, ax = plt.subplots()
ax.plot(water_activity*100, dust_01_albedo_wet/dust_01_albedo_dry, label='albedo 01', color='#FCBA74')
ax.plot(water_activity*100, dust1_albedo_wet/dust1_albedo_dry, label='albedo 1', color='#71401A')
ax.plot(water_activity*100, dustkappa1_albedo_wet/dustkappa1_albedo_dry , label='07', color='#F49E1E')
ax.set_ylim(0.99, 1.1)
ax.set_xlabel('Relative Humidity (%)')
ax.set_ylabel('Albedo Humidity Enhancement (450 nm)')
ax.grid(True)
ax.legend()
plt.show()
fig.savefig('albedo_simulation.pdf', dpi=300)

fig, ax = plt.subplots()
ax.plot(water_activity*100, dust_01_absorption_wet/dust_01_absorption_dry , label='01',color='#FCBA74')
ax.plot(water_activity*100, dust1_absorption_wet/dust1_absorption_dry , label='1', color='#71401A')
ax.plot(water_activity*100, dustkappa1_absorption_wet/dustkappa1_absorption_dry , label='07', color='#F49E1E')
ax.set_ylim(0.90, 1.1)
ax.set_xlabel('Relative Humidity (%)')
ax.set_ylabel('Absorption Humidity Enhancement (450 nm)')
ax.grid(True)
ax.legend()
plt.show()
fig.savefig('absorption_simulation.pdf', dpi=300)



# print(f'extention dry: {optics_dry[0]:.1f} Mm-1, wet: {optics_wet[0]:.1f} Mm-1')

# albedo_ratio = (optics_wet[1]/optics_wet[0]) / (optics_dry[1]/optics_dry[0])
# print(f'albedo ratio: {albedo_ratio:.3f}')

# absorption_ratio = (optics_wet[2]) / (optics_dry[2])
# print(f'absorption dry: {optics_dry[2]:.1f} Mm-1, wet: {optics_wet[2]:.1f} Mm-1')
# print(f'absorption ratio: {absorption_ratio:.2f}')


# %%

# optics_wet, optics_dry = mie.extinction_ratio_wet_dry(
#     kappa=0.01,
#     particle_counts=conc_dn[0:120],
#     diameters=diameters[0:120],
#     water_activity_sizer=0.1,
#     water_activity_dry=0.54,
#     water_activity_wet=0.85,
#     refractive_index_dry=1.539+0.255*1j,
#     water_refractive_index=1.33,
#     wavelength=450,
#     discretize_Mie=False,
#     return_coefficients=True,
#     return_all_optics=True,
# )



# print(f'extention dry: {optics_dry[0]:.1f} Mm-1, wet: {optics_wet[0]:.1f} Mm-1')

# albedo_ratio = (optics_wet[1]/optics_wet[0]) / (optics_dry[1]/optics_dry[0])
# print(f'albedo ratio: {albedo_ratio:.3f}')

# absorption_ratio = (optics_wet[2]) / (optics_dry[2])
# print(f'absorption dry: {optics_dry[2]:.1f} Mm-1, wet: {optics_wet[2]:.1f} Mm-1')
# print(f'absorption ratio: {absorption_ratio:.2f}')


# %%
