# %% Imports

import numpy as np
import pandas as pd
import datacula.convert as convert
from particula import u
from particula.util.input_handling import in_temperature, in_molecular_weight, in_density, in_radius, in_surface_tension
from particula.constants import GAS_CONSTANT
import surface_tension as surface_tension

import matplotlib.pyplot as plt
# preferred settings for plotting
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

# %% Functions


def kelvin_radius(
    surface_tension=0.072 * u.N/u.m,
    molecular_weight=0.01815 * u.kg/u.mol,
    density=1000 * u.kg/u.m**3,
    temperature=298.15 * u.K
):
    """ Kelvin radius (Neil's definition)
        https://en.wikipedia.org/wiki/Kelvin_equation
    """

    temperature = in_temperature(temperature).to_base_units()
    molecular_weight = in_molecular_weight(molecular_weight).to_base_units()
    density = in_density(density).to_base_units()
    surface_tension = in_surface_tension(surface_tension).to_base_units()  # type: ignore

    return 2 * surface_tension * molecular_weight / (
        GAS_CONSTANT * temperature * density
    )


def kelvin_term(radius=None, **kwargs):
    """ Kelvin term (Neil's definition)
        https://en.wikipedia.org/wiki/Kelvin_equation
    """

    rad = in_radius(radius)

    kelvin_rad = kelvin_radius(**kwargs)

    return np.exp(kelvin_rad / rad)

# %%
mass_fraction = np.logspace(-4,0,50)
temperature = np.ones_like(mass_fraction)*(273.15 + 25)

# combine the two arrays into a single matrix
# first is temperature, second is mass fraction
for i, t in enumerate(temperature):
    print(t, ', ', mass_fraction[i])

# %% load AIOMFAC


def load_activities(file_name, a_scale=None):
    data_water = pd.read_csv(file_path, sep='\s+', header=None, names=['no.','T_[K]','RH_[%]','w(01)','x_i(01)','m_i(01)','a_coeff_x(01)','a_x(01)','flag'], skiprows=101, nrows=50)
    # data = np.loadtxt(file_path, skiprows=103, delimiter='\s+', max_rows=20)
    data_water.head()

    a_water = data_water['a_x(01)'].values
    arg_sort = np.argsort(a_water)
    a_water = a_water[arg_sort]
    mole_fraction_water = data_water['x_i(01)'].values[arg_sort]
    mass_fraction_water = data_water['w(01)'].values[arg_sort]

    if a_scale is not None:
        mole_fraction_water = np.interp(a_scale, a_water, mole_fraction_water)
        mass_fraction_water = np.interp(a_scale, a_water, mass_fraction_water)

    activity_dict = {
        'activity_water': a_water,
        'mole_fraction_water': mole_fraction_water,
        'mass_fraction_water': mass_fraction_water
    }
    return activity_dict

activity_scale = np.sort([9.99997e-01, 9.99996e-01, 9.99994e-01, 9.99993e-01, 9.99991e-01,
       9.99989e-01, 9.99986e-01, 9.99983e-01, 9.99978e-01, 9.99973e-01,
       9.99966e-01, 9.99957e-01, 9.99946e-01, 9.99933e-01, 9.99916e-01,
       9.99896e-01, 9.99870e-01, 9.99838e-01, 9.99799e-01, 9.99751e-01,
       9.99692e-01, 9.99619e-01, 9.99528e-01, 9.99417e-01, 9.99279e-01,
       9.99107e-01, 9.98893e-01, 9.98625e-01, 9.98286e-01, 9.97854e-01,
       9.97297e-01, 9.96572e-01, 9.95615e-01, 9.94330e-01, 9.92574e-01,
       9.90130e-01, 9.86655e-01, 9.81608e-01, 9.74122e-01, 9.62796e-01,
       9.45350e-01])

file_path = 'CaCO3.txt'
aiomfac_CaCO3 = load_activities(file_path, a_scale=activity_scale)

file_path = 'NH4SO4.txt'
aiomfac_NH4SO4 = load_activities(file_path, a_scale=activity_scale)

file_path = '2hexanone.txt'
aiomfac_2hexanone = load_activities(file_path, a_scale=activity_scale)
#%%


def kohler_calculation(
            dry_soluble_volume_fractions,
            insoluble_to_soluble_diameters,
            solute_densities,
            soluble_aiomfac_dict,
            activity_scale,
        ):
    """ Calculates the kohler theory for a mixture of solutes"""
    # function start

    volume_insoluble = convert.length_to_volume(insoluble_to_soluble_diameters[0], length_type='diameter')
    volume_soluble = convert.length_to_volume(insoluble_to_soluble_diameters[1], length_type='diameter')


    # calc water for solubles
    density_water_g_cm3 = 1.0

    for i, solute_faction in enumerate(dry_soluble_volume_fractions):

        volume_fraction_species, volume_fraction_water = convert.mass_fraction_to_volume_fraction(
            mass_fraction=1-soluble_aiomfac_dict[i]['mass_fraction_water'],
            density_solute=solute_densities[i],
            density_solvent=density_water_g_cm3
        )

        soluble_aiomfac_dict[i]['water_volume'] = convert.volume_water_from_volume_fraction(
            volume_solute_dry=volume_soluble * solute_faction,
            volume_fraction_water=volume_fraction_water
        )
        soluble_aiomfac_dict[i]['dry_volume'] = volume_soluble * solute_faction

    total_water_volume = sum([soluble_aiomfac_dict[i]['water_volume'] for i in range(len(soluble_aiomfac_dict))])
    total_solute_volume = sum([soluble_aiomfac_dict[i]['dry_volume'] for i in range(len(soluble_aiomfac_dict))])

    total_volume = volume_insoluble + total_solute_volume + total_water_volume

    radius_total = convert.volume_to_length(total_volume)
    diameter_total = convert.volume_to_length(total_volume, length_type='diameter')


    mixed_surface_tension = surface_tension.wet_mixing(
        volume_solute=soluble_aiomfac_dict[-1]['dry_volume']*u.nm**3,
        volume_water=total_water_volume*u.nm**3,
        wet_radius=radius_total*u.nm,
        surface_tension_solute=0.03,
        temperature=298.15,
        method='film'
    )

    activity_water_effective = activity_scale * kelvin_term(radius=radius_total*u.nm)

    activity_water_effectiveOrg = activity_scale * kelvin_term(radius=radius_total*u.nm, surface_tension=mixed_surface_tension)

    supersat = (activity_water_effective-1)*100
    supersatOrg = (activity_water_effectiveOrg-1)*100

    max_activity = np.argmax(activity_water_effective)
    max_activityOrg = np.argmax(activity_water_effectiveOrg)

    # max sat and effective kappa CCN
    kappa = convert.kappa_from_volume(
        volume_solute=volume_soluble+volume_insoluble, 
        volume_water=total_water_volume,
        water_activity=activity_scale
    )
    kappa_ccn = kappa[max_activity]
    kappa_ccnOrg = kappa[max_activityOrg]
    supersat_crit = supersat[max_activity]
    supersat_critOrg = supersatOrg[max_activityOrg]


    kohler_dict = {
        'radius_total': radius_total,
        'diameter_total': diameter_total,
        'activity_water_effective': activity_water_effective,
        'activity_water_effectiveOrg': activity_water_effectiveOrg,
        'activity_scale': activity_scale,
        'supersat': supersat,
        'supersatOrg': supersatOrg,
        'kappa': kappa,
        'kappa_ccn': kappa_ccn,
        'kappa_ccnOrg': kappa_ccnOrg,
        'supersat_crit': supersat_crit,
        'supersat_critOrg': supersat_critOrg,
    }
    return kohler_dict

# %%

sulfate_150nm_pure = kohler_calculation(
    dry_soluble_volume_fractions=[1],
    insoluble_to_soluble_diameters=[0,150],
    solute_densities=[2.0],
    soluble_aiomfac_dict=[aiomfac_NH4SO4],
    activity_scale=activity_scale
)

volume150 = convert.length_to_volume(150, length_type='diameter')
diameter_half = convert.volume_to_length(volume150/2, length_type='diameter')
bc_organic_150nm_pure = kohler_calculation(
    dry_soluble_volume_fractions=[1],
    insoluble_to_soluble_diameters=[diameter_half,diameter_half],
    solute_densities=[0.8],
    soluble_aiomfac_dict=[aiomfac_2hexanone],
    activity_scale=activity_scale
)

sulfate_org_500nm = kohler_calculation(
    dry_soluble_volume_fractions=[0.5,0.5],
    insoluble_to_soluble_diameters=[0,500],
    solute_densities=[2.0,0.8],
    soluble_aiomfac_dict=[aiomfac_NH4SO4, aiomfac_2hexanone],
    activity_scale=activity_scale
)
insolube_dust_2500_sulfate = kohler_calculation(
    dry_soluble_volume_fractions=[1],
    insoluble_to_soluble_diameters=[2500,150],
    solute_densities=[2.0],
    soluble_aiomfac_dict=[aiomfac_NH4SO4],
    activity_scale=activity_scale
)

volume2500 = convert.length_to_volume(2500, length_type='diameter')
diameter_Ca_frac = convert.volume_to_length(volume2500*.02, length_type='diameter')
diameter_insoluble = convert.volume_to_length(volume2500*.98, length_type='diameter')

insolube_dust_2500_CaCO3 = kohler_calculation(
    dry_soluble_volume_fractions=[1],
    insoluble_to_soluble_diameters=[diameter_insoluble,diameter_Ca_frac],
    solute_densities=[2.71],
    soluble_aiomfac_dict=[aiomfac_CaCO3],
    activity_scale=activity_scale
)

# film of sulfate
mono = 0.3
# Volume of the monolayer
film_volume = (4/3) * np.pi * ((2500/2)**3 - ((2500/2) - mono)**3)
diameter_film = convert.volume_to_length(film_volume, length_type='diameter')
insolube_dust_2500_coatedAmSulfate = kohler_calculation(
    dry_soluble_volume_fractions=[1],
    insoluble_to_soluble_diameters=[2500, diameter_film],
    solute_densities=[2.71],
    soluble_aiomfac_dict=[aiomfac_NH4SO4],
    activity_scale=activity_scale
)



# %% Kholer plot
color_bc = '#78716c'
color_sulfate = '#be1f3e'
color_dust = '#f49e1e'
color_processed = '#0784c7'
color_dust_coat = '#f27171'
color_dust_sulfate = '#fed7aa'

fig, ax = plt.subplots()
ax.plot(sulfate_150nm_pure['diameter_total'][:-15],
        sulfate_150nm_pure['supersat'][:-15],
        label='Ammonium Sulfate',
        color=color_sulfate)
# ax.plot(bc_organic_150nm_pure['diameter_total'][:-10],
#         bc_organic_150nm_pure['supersat'][:-10],
#         label='BC coated (water surface tension)')
ax.plot(bc_organic_150nm_pure['diameter_total'][:-10],
        bc_organic_150nm_pure['supersatOrg'][:-10],
        label='BC coated (organic film surface tension)',
        linestyle='--',
        color=color_bc)
ax.plot(sulfate_org_500nm['diameter_total'],
        sulfate_org_500nm['supersatOrg'],
        label='Ammonium Sulfate + organic (film)',
        color=color_processed,
        linewidth=3)
ax.plot(insolube_dust_2500_sulfate['diameter_total'][:-5],
        insolube_dust_2500_sulfate['supersat'][:-5],
        label='Insoluble dust + Ammonium Sulfate',
        linestyle='--',
        color=color_dust_sulfate)
ax.plot(insolube_dust_2500_CaCO3['diameter_total'],
        insolube_dust_2500_CaCO3['supersat'],
        label='Insoluble dust + Calcum Carbonate',
        color=color_dust)
ax.plot(insolube_dust_2500_coatedAmSulfate['diameter_total'],
        insolube_dust_2500_coatedAmSulfate['supersat'],
        label='Insoluble dust + Ammonium Sulfate (film)',
        color=color_dust_coat)

# ax.plot(radius_total, a_water, label='flat')
ax.set_xlabel('Wet Diameter [nm]')
ax.set_ylabel('Supersaturation of water')
# ax.legend()

ax.set_xscale('log')
ax.set_ylim(bottom=-0.01, top=.12)
ax.set_xlim(right=100000, left=100)
# ax.set_title('Kohler curve')
plt.show()
fig.tight_layout()
fig.savefig('kohler_curve.pdf', dpi=300, format='pdf')

#%% effective kappa vs supersaturation

fig, ax = plt.subplots()
ax.scatter(sulfate_150nm_pure['kappa_ccn'],
        sulfate_150nm_pure['supersat_crit'],
        label='Ammonium Sulfate',
        color=color_sulfate,
        marker='s')
ax.scatter(bc_organic_150nm_pure['kappa_ccnOrg'],
        bc_organic_150nm_pure['supersat_critOrg'],
        label='BC coated (organic film surface tension)',
        color=color_bc,
        marker='^')
ax.scatter(sulfate_org_500nm['kappa_ccnOrg'],
        sulfate_org_500nm['supersat_critOrg'],
        label='Ammonium Sulfate + organic (film)',
        color=color_processed,)
ax.scatter(insolube_dust_2500_sulfate['kappa_ccn'],
        insolube_dust_2500_sulfate['supersat_crit'],
        label='Insoluble dust + Ammonium Sulfate',
        color=color_dust_sulfate)
ax.scatter(insolube_dust_2500_CaCO3['kappa_ccn'],
        insolube_dust_2500_CaCO3['supersat_crit'],
        label='Insoluble dust + Calcum Carbonate',
        color=color_dust)
ax.scatter(insolube_dust_2500_coatedAmSulfate['kappa_ccn'],
        insolube_dust_2500_coatedAmSulfate['supersat_crit'],
        label='Insoluble dust + Ammonium Sulfate (film)',
        color=color_dust_coat)

# ax.plot(radius_total, a_water, label='flat')
ax.set_xlabel('Particles effective KappaCCN observed')
ax.set_ylabel('Supersaturation')
# ax.legend()

ax.set_xscale('log')
ax.set_ylim(bottom=-0.01, top=.12)
ax.set_xlim(right=1, left=-.05)
# ax.set_title('Kohler curve')
plt.show()
fig.tight_layout()
fig.savefig('criticalSS.pdf', dpi=300, format='pdf')

# %%
