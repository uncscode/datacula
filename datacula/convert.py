"""conversion functions common for aerosol processing
"""

from typing import Union, Tuple
import numpy as np


def coerce_type(data, dtype):
    """
    Coerces data to dtype if it is not already of that type.
    """
    if not isinstance(data, dtype):
        try:
            if dtype == np.ndarray:
                return np.array(data)
            return dtype(data)
        except (ValueError, TypeError) as exc:
            raise ValueError(f'Could not coerce {data} to {dtype}') from exc
    return data


def round_arbitrary(
        values: Union[float, list[float], np.ndarray],
        base: float = 1.0,
        mode: str = 'round',
        nonzero_edge: bool = False
        ) -> Union[float, list[float]]:
    """
    Rounds the input values to the nearest multiple of the base.

    For values exactly halfway between rounded decimal values, "Bankers
    rounding applies" rounds to the nearest even value. Thus 1.5 and 2.5
    round to 2.0, -0.5 and 0.5 round to 0.0, etc.

    Parameters:
        values: The values to be rounded.
        base: The base to which the values should be rounded.
        mode: The rounding mode: 'round', 'floor', 'ceil'
        nonzero_edge: If true the zero values are replaced
        by the original values.

    Returns:
        rounded: The rounded values.
    """
    # Check if values is a NumPy array
    values = coerce_type(values, np.ndarray)
    base = coerce_type(base, float)

    # Validate base parameter
    if not isinstance(base, float) or base <= 0:
        raise ValueError('base must be a positive float')
    # Validate mode parameter
    if mode not in ['round', 'floor', 'ceil']:
        raise ValueError("mode must be one of ['round', 'floor', 'ceil']")

    # Calculate rounding factors
    factor = np.array([-0.5, 0, 0.5])

    # Compute rounded values
    rounded = base * np.round(
            values / base
            + factor[
                np.array(
                    ['floor', 'round', 'ceil']
                    ).tolist().index(mode)
                ]
            )

    # Apply round_nonzero mode
    if nonzero_edge:
        rounded = np.where(rounded != 0, rounded, values)

    return rounded.tolist() if isinstance(values, list) else rounded


def radius_diameter(value: float, to_diameter: bool = True) -> float:
    """
    Convert a radius to a diameter, or vice versa.

    Parameters:
        value: The value to be converted.
        to_diameter: If True, convert from radius to diameter.
        If False, convert from diameter to radius.

    Returns:
        The converted value.
    """
    if to_diameter:
        return value * 2
    return value / 2


def volume_to_length(volume: float, length_type: str = 'radius') -> float:
    """
    Convert a volume to a radius or diameter.

    Parameters:
        volume: The volume to be converted.
        length_type: The type of length to convert to ('radius' or 'diameter')
        Default is 'radius'.

    Returns:
        The converted length.
    """

    if length_type not in ['radius', 'diameter']:
        raise ValueError('length_type must be radius or diameter')

    radius = (volume * 3 / (4 * np.pi)) ** (1 / 3)

    if length_type == 'radius':
        return radius
    return radius * 2


def length_to_volume(length: float, length_type: str = 'radius') -> float:
    """
    Convert radius or diameter to volume.

    Parameters:
        length: The length to be converted.
        length_type: The type of length ('radius' or 'diameter').
            Default is 'radius'.

    Returns:
        The volume.
    """
    if length_type == 'diameter':
        length = length / 2
    elif length_type == 'radius':
        pass
    else:
        raise ValueError('length_type must be radius or diameter')
    return (4/3)*np.pi*(length**3)


def kappa_volume_solute(
            volume_total: float,
            kappa: float,
            water_activity: float
        ) -> np.ndarray:
    """
    Calculate the volume of solute in a volume of total solution,
    given the kappa parameter and water activity.

    Parameters:
        volume_total: The volume of the total solution.
        kappa: The kappa parameter.
        water_activity: The water activity.

    Returns:
        The volume of solute as a numpy array.
    """

    kappa = max(kappa, 1e-16)  # Avoid division by zero

    vol_factor = (water_activity - 1) / (
            water_activity * (1 - kappa - 1 / water_activity)
        )
    return volume_total * np.array(vol_factor)


def kappa_volume_water(
            volume_solute: float,
            kappa: float,
            water_activity: float
        ) -> float:
    """
    Calculate the volume of water given volume of solute, kappa parameter,
    and water activity.

    Parameters:
        volume_solute: The volume of solute.
        kappa: The kappa parameter.
        water_activity: The water activity.

    Returns:
        The volume of water as a float.
    """
    # Avoid division by zero
    water_activity = min(water_activity, 1 - 1e-16)

    return volume_solute * kappa / (1 / water_activity - 1)


def kappa_from_volume(
            volume_solute: float,
            volume_water: float,
            water_activity: float
        ) -> float:
    """
    Calculate the kappa parameter from the volume of solute and water,
    given the water activity.

    Parameters:
        volume_solute: The volume of solute.
        volume_water: The volume of water.
        water_activity: The water activity.

    Returns:
        The kappa parameter as a float.
    """
    # Avoid division by zero
    water_activity = min(water_activity, 1 - 1e-16)

    return (1 / water_activity - 1) * volume_water / volume_solute


def mole_fraction_to_mass_fraction(
        mole_fraction0: float,
        molecular_weight0: float,
        molecular_weight1: float
        ) -> Tuple[float, float]:
    """
    Convert mole fraction to mass fraction.

    Parameters:
        mole_fraction0: The mole fraction of the first component.
        molecular_weight0: The molecular weight of the first component.
        molecular_weight1: The molecular weight of the second component.

    Returns:
        A tuple containing the mass fractions of the two components as floats.
    """
    mass_fraction0 = mole_fraction0 * molecular_weight0 / (
            mole_fraction0 * molecular_weight0
            + (1 - mole_fraction0) * molecular_weight1
        )
    mass_fraction1 = 1 - mass_fraction0
    return mass_fraction0, mass_fraction1


def mole_fraction_to_mass_fraction_multi(
            mole_fractions: list[float],
            molecular_weights: list[float]
        ) -> list[float]:
    """Convert mole fractions to mass fractions for N components.
    Assumes that sum(mole_fractions) == 1.

    Parameters:
        mole_fractions: A list of mole fractions.
        molecular_weights: A list of molecular weights.

    Returns:
        A list of mass fractions.
    """
    if np.sum(mole_fractions) != 1:
        raise ValueError('Sum of mole fractions must be 1')

    total_molecular_weight = np.sum(
            [mf * mw for mf, mw in zip(mole_fractions, molecular_weights)]
        )
    return [
            mf * mw / total_molecular_weight for mf,
            mw in zip(mole_fractions, molecular_weights)
        ]


def mass_fraction_to_volume_fraction(
    mass_fraction0: float, density0: float, density1: float
) -> Tuple[float, float]:
    """Convert mass fraction to volume fraction"""
    volume_fraction0 = (mass_fraction0 / density0) / (
        mass_fraction0 / density0 + (1 - mass_fraction0) / density1
    )
    volume_fraction1 = 1 - volume_fraction0
    return volume_fraction0, volume_fraction1


def volume_water_from_volume_fraction(
    volume_solute_dry: float,
    volume_fraction_water: float
) -> float:
    """
    Calculate the volume of water in a volume of solute,
    given the volume fraction of water.
    """
    return volume_fraction_water * volume_solute_dry / (
        1 - volume_fraction_water
    )


def effective_refractive_index(
            m_zero: Union[float, complex],
            m_one: Union[float, complex],
            volume_zero: float,
            volume_one: float
        ) -> Union[float, complex]:
    """Calculate the refractive index of a mixture of two solutes,
    given the refractive index of each solute and the volume of each solute.
    The volume can be volume fractions.
    The mixing is based on volume weight molar refraction.

    Parameters:
        m0: Refractive index of solute 0.
        m_one: Refractive index of solute 1.
        volume0: Volume of solute 0.
        volume_one: Volume of solute 1.

    Returns:
        The effective refractive index of the mixture.

    Reference:
        Liu, Y., &#38; Daum, P. H. (2008).
        Relationship of refractive index to mass density and self-consistency
        mixing rules for multicomponent mixtures like ambient aerosols.
        Journal of Aerosol Science, 39(11), 974-986.
        https://doi.org/10.1016/j.jaerosci.2008.06.006
    """
    volume_total = volume_zero + volume_one
    r_effective = (
        volume_zero/volume_total * (m_zero-1)/(m_zero+2)
        + volume_one/volume_total * (m_one-1)/(m_one+2)
        )  # molar refraction mixing

    # convert to refractive index
    return (2*r_effective + 1) / (1-r_effective)


# continue refactor from here:

def convert_sizer_dn(
            diameter: np.ndarray,
            dn_dlogdp: np.ndarray
        ) -> np.ndarray:
    """
    Converts the sizer data from dn/dlogdp to dn
    TODO: fix the over counting of the last bin or first?
    and write a test for this

    Parameters:
        diameter : diameter array
        dn_dlogdp : dn/dlogdp array

    Returns:
        dn array
    """

    delta = np.zeros_like(diameter)
    delta[:-1] = np.diff(diameter)
    delta[-1] = delta[-2]**2/delta[-3]

    lower = diameter-delta/2
    upper = diameter+delta/2

    return dn_dlogdp*np.log10(upper/lower)


def datetime64_from_epoch_array(epoch_array: np.ndarray, delta: int=0) -> np.datetime64:
    """
    Converts an array of epoch times to a numpy array of datetime64 objects.
    """
    return np.array([np.datetime64(int(epoch+delta),'s') for epoch in epoch_array])


def list_to_dict(list_of_str: list) -> dict:
    """
    Converts a list of strings to a dictionary. The keys are the strings
    and the values are the index of the string in the list.
    """
    return {list_of_str[i]:i for i in range(len(list_of_str))}


def get_values_in_dict(key_list: list, dict_to_check: dict) -> list:
    """
    Checks if keys in a list are in a dictionary. And get the values of the keys.

    Parameters
    ----------
    key_list : list
        List of keys to check.
    dict_to_check : dict
        Dictionary to check.
    
    Returns
    -------
    list
        List of values of the keys in the dictionary.
    """
    good_keys = []
    for key in key_list:
        if key in dict_to_check:
            good_keys.append(dict_to_check[key])
        else:
            print(dict_to_check.keys())
            raise KeyError(f"key {key} not in dictionary")
    return good_keys