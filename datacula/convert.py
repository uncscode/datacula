"""conversion functions common for aerosol processing
"""

import numpy as np
from typing import Union


def coerce_type(data, dtype):
    """
    Coerces data to dtype if it is not already of that type.
    """
    if not isinstance(data, dtype):
        try:
            if dtype == np.ndarray:
                return np.array(data)
            else:
                return dtype(data)
        except (ValueError, TypeError):
            raise ValueError(f'Could not coerce {data} to {dtype}')
    else:
        return data


def round(
        values: Union[float, list[float], np.ndarray],
        base: float = 1.0,
        mode: str = 'round',
        nonzero_edge: bool = False
        ) -> Union[float, list[float]]:
    """
    Rounds the input values to the nearest multiple of the base.

    For values exactly halfway between rounded decimal values, "Breakers
    rounding applies" rounds to the nearest even value. Thus 1.5 and 2.5
    round to 2.0, -0.5 and 0.5 round to 0.0, etc.

    Parameters:
        values (Union[float, List[float], np.ndarray])
            The values to be rounded.
        base (float): default 1.0
            The base to which the values should be rounded (default 1.0).
        mode (str): default 'round'
            The rounding mode: 'round', 'floor', 'ceil'
        nonzero_edge: False
            If true the zero values are replaced by the original values.

    Returns:
        Union[float, List[float]]
            The rounded values.
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


# def round(values, base=1.0, mode: str = 'round') -> float:
#         """
#         Rounds to the floor a number to the nearest base.
#         """
#         # check np.array
#         if type(values) is np.ndarray:
#             pass
#         else:
#             values = np.array(values)

#         if mode == 'round':
#             return base * np.round(values/base)
#         elif mode == 'floor':
#             return base * np.round(values/base-0.5)
#         elif mode == 'ceil':
#             return base * np.round(values/base+0.5)
#         elif mode == 'round_nonzero':
#             rounded = base * np.round(values/base)
#             return np.where(rounded != 0, rounded, values).tolist()
#         else:
#             raise ValueError('mode must be round, floor or ceil')