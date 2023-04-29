"""data processing funcitons that can be used on datalake objects."""
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import numpy as np
from scipy.interpolate import interp1d

from datacula.mie import kappa_fitting_caps_data
import datacula.size_distribution as size_distribution

def caps_processing(
        datalake: object,
        truncation_bsca: bool = True,
        truncation_interval_sec: int = 600,
        truncation_interp: bool = True,
        refractive_index: float = 1.45,
        calibration_wet=1,
        calibration_dry=1,
        kappa_fixed: float = None,
        ):
    """loader.
    Function to process the CAPS data, and smps for kappa fitting, and then add
    it to the datalake. Also applies truncation corrections to the CAPS data.
    
    Parameters
    ----------
    datalake : object
        DataLake object to add the processed data to.
    truncation_bsca : bool, optional
        Whether to calculate truncation corrections for the bsca data.
        The default is True.
    truncation_interval_sec : int, optional
        The interval to calculate the truncation corrections over. 
        The default is 600. This can take around 10 sec per data point.
    truncation_interp : bool, optional
        Whether to interpolate the truncation corrections to the caps data.
    refractive_index : float, optional 
        The refractive index of the aerosol. The default is 1.45.
    calibration_wet : float, optional
        The calibration factor for the wet data. The default is 1.
    calibration_dry : float, optional
        The calibration factor for the dry data. The default is 1.
    
    Returns
    -------
    datalake : object
        DataLake object with the processed data added.
    """
    # calc kappa and add to datalake
    if kappa_fixed is None:
        kappa_fit, _, _ = kappa_fitting_caps_data(
            datalake=datalake,
            truncation_bsca=False,
            refractive_index=refractive_index
        )
    else:
        kappa_len = len(datalake.datastreams['CAPS_data'].return_time(datetime64=False))
        kappa_fit = np.ones((kappa_len, 3)) * kappa_fixed

    datalake.datastreams['CAPS_data'].add_processed_data(
            data_new=kappa_fit.T,
            time_new=datalake.datastreams['CAPS_data'].return_time(datetime64=False),
            header_new=['kappa_fit', 'kappa_fit_lower', 'kappa_fit_upper'],
        )
    orignal_average = datalake.datastreams['CAPS_data'].average_base_sec

    # calc truncation corrections and add to datalake
    if truncation_bsca:
        datalake.reaverage_datastreams(
            truncation_interval_sec,
            stream_keys=['CAPS_data', 'smps_1D', 'smps_2D'],
        )
        # epoch_start=epoch_start,
        # epoch_end=epoch_end

        _, bsca_truncation_dry, bsca_truncation_wet = kappa_fitting_caps_data(
            datalake=datalake,
            truncation_bsca=truncation_bsca,
            refractive_index=refractive_index
        )

        if truncation_interp:
            interp_dry = interp1d(
                datalake.datastreams['CAPS_data'].return_time(datetime64=False),
                bsca_truncation_dry,
                kind='linear',
                fill_value='extrapolate'
            )
            interp_wet = interp1d(
                datalake.datastreams['CAPS_data'].return_time(datetime64=False),
                bsca_truncation_wet,
                kind='linear',
                fill_value='extrapolate'
            )

            time = datalake.datastreams['CAPS_data'].return_time(
                datetime64=False,
                raw=True
            )
            bsca_truncation_dry = interp_dry(time)
            bsca_truncation_wet = interp_wet(time)
        else:
            time = datalake.datastreams['CAPS_data'].return_time(
                datetime64=False)

        datalake.datastreams['CAPS_data'].add_processed_data(
            data_new=bsca_truncation_dry.T,
            time_new=time,
            header_new=['bsca_truncation_dry'],
        )
        datalake.datastreams['CAPS_data'].add_processed_data(
            data_new=bsca_truncation_wet.T,
            time_new=time,
            header_new=['bsca_truncation_wet'],
        )
    else:
        bsca_truncation_wet = np.array([1])
        bsca_truncation_dry = np.array([1])
        time = datalake.datastreams['CAPS_data'].return_time(
                datetime64=False,
                raw=True
            )

    # index for Bsca wet and dry
    index_dic = datalake.datastreams['CAPS_data'].return_header_dict()
    
    # check if raw in dict
    if 'raw_Bsca_dry_CAPS_450nm[1/Mm]' in index_dic:
        pass
    else:
        # save raw data
        datalake.datastreams['CAPS_data'].add_processed_data(
            data_new=datalake.datastreams['CAPS_data'].data_stream[index_dic['Bsca_wet_CAPS_450nm[1/Mm]'], :],
            time_new=time,
            header_new=['raw_Bsca_wet_CAPS_450nm[1/Mm]'],
        )
        datalake.datastreams['CAPS_data'].add_processed_data(
            data_new=datalake.datastreams['CAPS_data'].data_stream[index_dic['Bsca_dry_CAPS_450nm[1/Mm]'], :],
            time_new=time,
            header_new=['raw_Bsca_dry_CAPS_450nm[1/Mm]'],
        )
        index_dic = datalake.datastreams['CAPS_data'].return_header_dict()


    datalake.datastreams['CAPS_data'].data_stream[index_dic['Bsca_wet_CAPS_450nm[1/Mm]'], :] = datalake.datastreams['CAPS_data'].data_stream[index_dic['raw_Bsca_wet_CAPS_450nm[1/Mm]'], :] * bsca_truncation_wet.T * calibration_wet
    
    datalake.datastreams['CAPS_data'].data_stream[index_dic['Bsca_dry_CAPS_450nm[1/Mm]'], :] = datalake.datastreams['CAPS_data'].data_stream[index_dic['raw_Bsca_dry_CAPS_450nm[1/Mm]'], :] * bsca_truncation_dry.T * calibration_dry


    datalake.datastreams['CAPS_data'].reaverage(
        reaverage_base_sec=orignal_average
    )  # updates the averages to the original value

    return datalake


def albedo_processing(
    datalake,
    keys: list = None
    ):
    """
    Calculates the albedo from the CAPS data and updates the datastream.

    Parameters
    ----------
    datalake : object
        DataLake object with the processed data added.
    
    Returns
    -------
    datalake : object
        DataLake object with the processed data added.
    """

    ssa_wet = datalake.datastreams['CAPS_data'].return_data(keys=['Bsca_wet_CAPS_450nm[1/Mm]'])[0]/datalake.datastreams['CAPS_data'].return_data(keys=['Bext_wet_CAPS_450nm[1/Mm]'])[0]
    ssa_dry = datalake.datastreams['CAPS_data'].return_data(keys=['Bsca_dry_CAPS_450nm[1/Mm]'])[0]/datalake.datastreams['CAPS_data'].return_data(keys=['Bext_dry_CAPS_450nm[1/Mm]'])[0]

    babs_wet = datalake.datastreams['CAPS_data'].return_data(keys=['Bext_wet_CAPS_450nm[1/Mm]'])[0] - datalake.datastreams['CAPS_data'].return_data(keys=['Bsca_wet_CAPS_450nm[1/Mm]'])[0]
    babs_dry = datalake.datastreams['CAPS_data'].return_data(keys=['Bext_dry_CAPS_450nm[1/Mm]'])[0] - datalake.datastreams['CAPS_data'].return_data(keys=['Bsca_dry_CAPS_450nm[1/Mm]'])[0]

    time = datalake.datastreams['CAPS_data'].return_time(datetime64=False)

    datalake.datastreams['CAPS_data'].add_processed_data(
        data_new=ssa_wet,
        time_new=time,
        header_new=['SSA_wet_CAPS_450nm[1/Mm]'],
    )
    datalake.datastreams['CAPS_data'].add_processed_data(
        data_new=ssa_dry,
        time_new=time,
        header_new=['SSA_dry_CAPS_450nm[1/Mm]'],
    )
    datalake.datastreams['CAPS_data'].add_processed_data(
        data_new=babs_wet,
        time_new=time,
        header_new=['Babs_wet_CAPS_450nm[1/Mm]'],
    )
    datalake.datastreams['CAPS_data'].add_processed_data(
        data_new=babs_dry,
        time_new=time,
        header_new=['Babs_dry_CAPS_450nm[1/Mm]'],
    )
    return datalake


def ccnc_hygroscopicity(
        datalake,
        supersaturation_bounds=[0.3, 0.9],
        dp_crit_threshold=75
        ):
    """
    Calculate the hygroscopicity of CCNc using the activation diameter, and the
    smps data.

    Parameters
    ----------
    datalake : DataLake
        collection of datastreams.
    supersaturation_bounds : list, optional
        supersaturation bounds for the activation diameter. The default is [0.3, 0.9].
    dp_crit_threshold : float, optional
        dp_crit threshold for the activation diameter. The default is 75 nm.
    
    Returns
    -------
    datalake : object
        DataLake object with the processed data added.
    
    Petters, M. D.,; Kreidenweis, S. M. (2007). A single parameter
    representation of hygroscopic growth and cloud condensation nucleus
    activity, Atmospheric Chemistry and Physics, 7(8), 1961-1971.
    https://doi.org/10.5194/acp-7-1961-2007

    """

    time = datalake.datastreams['CCNc'].return_time(datetime64=False)
    ccnc_number = datalake.datastreams['CCNc'].return_data(keys=['CCN_Concentration_[#/cc]'])[0]
    super_sat_set = datalake.datastreams['CCNc'].return_data(keys=['CurrentSuperSaturationSet[%]'])[0]
    sizer_total_n = datalake.datastreams['smps_1D'].return_data(keys=['Total_Conc_(#/cc)'])[0]
    sizer_diameter = datalake.datastreams['smps_2D'].return_header_list().astype(float)
    sizer_diameter_fliped = np.flip(sizer_diameter)
    sizer_dndlogdp = np.nan_to_num(datalake.datastreams['smps_2D'].return_data())

    fitted_dp_crit = np.zeros_like(super_sat_set)
    activated_fraction = np.zeros_like(super_sat_set)

    for i in range(len(super_sat_set)):

        super_sat_set

        if ccnc_number[i] < sizer_total_n[i] and ccnc_number[i] > 50 and super_sat_set[i] > supersaturation_bounds[0] and super_sat_set[i] <= supersaturation_bounds[1]:
            sizer_dn = convert_sizer_dn(sizer_diameter, sizer_dndlogdp[:, i])
            sizer_dn = sizer_dn * sizer_total_n[i] / np.sum(sizer_dn)

            sizer_dn_cumsum = np.cumsum(np.flip(sizer_dn))

            fitted_dp_crit[i] = np.interp(ccnc_number[i], sizer_dn_cumsum, sizer_diameter_fliped, left=np.nan, right=np.nan)
            activated_fraction[i] = ccnc_number[i] / sizer_total_n[i]
        else:
            fitted_dp_crit[i] = np.nan
            activated_fraction[i] = np.nan


    kelvin_diameter = 1.06503 *2 # nm # update to be a function of temperature

    # Gohil, K., &#38; Asa-Awuku, A. A. (2022). Cloud condensation nuclei (CCN) activity analysis of low-hygroscopicity aerosols using the aerodynamic aerosol classifier (AAC). <i>Atmospheric Measurement Techniques</i>, <i>15</i>(4), 1007–1019. https://doi.org/10.5194/amt-15-1007-2022
    fitted_kappa = 4* kelvin_diameter**3 / (27 * fitted_dp_crit**3 * np.log(1+super_sat_set/100)**2)
    fitted_kappa_threshold = np.where(fitted_dp_crit > dp_crit_threshold, fitted_kappa, np.nan)

    datalake.add_processed_datastream(
            key='kappa_ccn',
            time_stream=time,
            data_stream=np.vstack((fitted_dp_crit, activated_fraction, fitted_kappa, fitted_kappa_threshold)),
            data_stream_header=['dp_critical', 'activated_fraction', 'kappa_CCNc', 'kappa_CCNc_threshold'],
            average_times=[90],
            average_base=[90]
            )
    return datalake


def sizer_mean_properties(
    datalake,
    ):
    """
    Calculates the mean properties of the size distribution. Using both the
    smps and aps data. (But does not merge to a single distribution)

    Parameters
    ----------
    datalake : DataLake
        The datalake to process.
    
    Returns
    -------
    datalake : DataLake
        The datalake with the mean properties added.
    """

    time = datalake.datastreams['smps_1D'].return_time(datetime64=False)
    sizer_total_n_smps = datalake.datastreams['smps_1D'].return_data(keys=['Total_Conc_(#/cc)'])[0]
    sizer_diameter_smps = datalake.datastreams['smps_2D'].return_header_list().astype(float)
    sizer_dndlogdp_smps = np.nan_to_num(datalake.datastreams['smps_2D'].return_data())

    sizer_diameter_aps = datalake.datastreams['aps_2D'].return_header_list().astype(float)*1000
    # TODO: fix aps data to concentrations
    sizer_dndlogdp_aps = datalake.datastreams['aps_2D'].return_data()/5

    total_concentration_PM800 = np.zeros_like(sizer_total_n_smps) * np.nan
    unit_mass_ugPm3_PM800 = np.zeros_like(sizer_total_n_smps) * np.nan
    mean_diameter_nm_PM800 = np.zeros_like(sizer_total_n_smps) * np.nan
    mean_vol_diameter_nm_PM800 = np.zeros_like(sizer_total_n_smps) * np.nan
    geometric_mean_diameter_nm_PM800 = np.zeros_like(sizer_total_n_smps) * np.nan
    mode_diameter_PM800 = np.zeros_like(sizer_total_n_smps) * np.nan
    mode_diameter_mass_PM800 = np.zeros_like(sizer_total_n_smps) * np.nan

    total_concentration_PM01 = np.zeros_like(sizer_total_n_smps) * np.nan
    unit_mass_ugPm3_PM01 = np.zeros_like(sizer_total_n_smps) * np.nan

    total_concentration_PM25 = np.zeros_like(sizer_total_n_smps) * np.nan
    unit_mass_ugPm3_PM25 = np.zeros_like(sizer_total_n_smps) * np.nan

    total_concentration_PM10 = np.zeros_like(sizer_total_n_smps) * np.nan
    unit_mass_ugPm3_PM10 = np.zeros_like(sizer_total_n_smps) * np.nan

    for i in range(len(time)):
        total_concentration_PM800[i], unit_mass_ugPm3_PM800[i], mean_diameter_nm_PM800[i], mean_vol_diameter_nm_PM800[i], geometric_mean_diameter_nm_PM800[i], mode_diameter_PM800[i], mode_diameter_mass_PM800[i] = size_distribution.mean_properties(
            sizer_dndlogdp_smps[:, i],
            sizer_diameter_smps,
            sizer_total_n_smps[i],
            sizer_limits=[0, 800]
        )

        total_concentration_PM01[i], unit_mass_ugPm3_PM01[i], _, _, _, _, _ = size_distribution.distribution_mean_properties(
            sizer_dndlogdp_smps[:, i],
            sizer_diameter_smps,
            sizer_total_n_smps[i],
            sizer_limits=[0, 100]
        )

        total_concentration_PM25[i], unit_mass_ugPm3_PM25[i], _, _, _, _, _ = size_distribution.distribution_mean_properties(
            sizer_dndlogdp_aps[:, i],
            sizer_diameter_aps,
            total_concentration=None,
            sizer_limits=[800, 2500]
        )

        total_concentration_PM10[i], unit_mass_ugPm3_PM10[i], _, _, _, _, _ = size_distribution.distribution_mean_properties(
            sizer_dndlogdp_aps[:, i],
            sizer_diameter_aps,
            total_concentration=None,
            sizer_limits=[800, 10000]
        )

    total_concentration_PM25 = total_concentration_PM25 + total_concentration_PM800
    unit_mass_ugPm3_PM25 = unit_mass_ugPm3_PM25 + unit_mass_ugPm3_PM800
    total_concentration_PM10 = total_concentration_PM10 + total_concentration_PM800
    unit_mass_ugPm3_PM10 = unit_mass_ugPm3_PM10 + unit_mass_ugPm3_PM800

    mass_ugPm3_PM800 = unit_mass_ugPm3_PM800 * 1.5
    mass_ugPm3_PM01 = unit_mass_ugPm3_PM01 * 1.5
    mass_ugPm3_PM25 = unit_mass_ugPm3_PM25 * 1.5
    mass_ugPm3_PM10 = unit_mass_ugPm3_PM10 * 1.5

    # combine the data for datalake
    combinded = np.vstack((
        total_concentration_PM01,
        total_concentration_PM800,
        mean_diameter_nm_PM800,
        geometric_mean_diameter_nm_PM800,
        mode_diameter_PM800,
        mean_vol_diameter_nm_PM800,
        mode_diameter_mass_PM800,
        unit_mass_ugPm3_PM01,
        mass_ugPm3_PM01,
        unit_mass_ugPm3_PM800,
        mass_ugPm3_PM800,
        total_concentration_PM25,
        unit_mass_ugPm3_PM25,
        mass_ugPm3_PM25,
        total_concentration_PM10,
        unit_mass_ugPm3_PM10,
        mass_ugPm3_PM10,
    ))
    header = [
        'Total_Conc_(#/cc)_N100',
        'Total_Conc_(#/cc)_smps',
        'Mean_Diameter_(nm)_smps',
        'Geometric_Mean_Diameter_(nm)_smps',
        'Mode_Diameter_(nm)_smps',
        'Mean_Diameter_Vol_(nm)_smps',
        'Mode_Diameter_Vol_(nm)_smps',
        'Unit_Mass_(ugPm3)_N100',
        'Mass_(ugPm3)_N100',
        'Unit_Mass_(ugPm3)_smps',
        'Mass_(ugPm3)_smps',
        'Total_Conc_(#/cc)_PM2.5',
        'Unit_Mass_(ugPm3)_PM2.5',
        'Mass_(ugPm3)_PM2.5',
        'Total_Conc_(#/cc)_PM10',
        'Unit_Mass_(ugPm3)_PM10',
        'Mass_(ugPm3)_PM10',
    ]
    
    datalake.add_processed_datastream(
        key='size_properties',
        time_stream=time,
        data_stream=combinded,
        data_stream_header=header,
        average_times=[90],
        average_base=[90]
        )
    return datalake


def pass3_processing(
        datalake: object,
        babs_405_532_781=[1, 1, 1],
        bsca_405_532_781=[1, 1, 1],
        ):
    """
    Processing PASS3 data applying the calibration factors
    TODO: add the background correction

    Parameters
    ----------
    datalake : object
        DataLake object to add the processed data to.
    babs_405_532_781 : list, optional
        Calibration factors for the absorption channels. The default is [1,1,1].
    bsca_405_532_781 : list, optional
        Calibration factors for the scattering channels. The default is [1,1,1].
    
    Returns
    -------
    datalake : object
        DataLake object with the processed data added.
    """
    # index for Bsca wet and dry
    index_dic = datalake.datastreams['pass3'].return_header_dict()
    time = datalake.datastreams['pass3'].return_time(
                datetime64=False,
                raw=True
            )
    babs_list = ['Babs405nm[1/Mm]', 'Babs532nm[1/Mm]', 'Babs781nm[1/Mm]']
    bsca_list = ['Bsca405nm[1/Mm]', 'Bsca532nm[1/Mm]', 'Bsca781nm[1/Mm]']

    if 'raw_Babs405nm[1/Mm]' in index_dic:
        pass
    else:     # check if raw in dict
        print('Copy raw babs Pass-3')
        for i, babs in enumerate(babs_list):
            raw_name = 'raw_' + babs

            datalake.datastreams['pass3'].add_processed_data(
                data_new=datalake.datastreams['pass3'].data_stream[index_dic[babs], :],
                time_new=time,
                header_new=[raw_name],
            )
    if 'raw_Bsca405nm[1/Mm]' in index_dic:
        pass
    else:
        print('Copy raw bsca Pass-3')
        for i, bsca in enumerate(bsca_list):
            raw_name = 'raw_' + bsca

            datalake.datastreams['pass3'].add_processed_data(
                data_new=datalake.datastreams['pass3'].data_stream[index_dic[bsca], :],
                time_new=time,
                header_new=[raw_name],
            )

    index_dic = datalake.datastreams['pass3'].return_header_dict()

    # calibration loop babs.
    print('Calibrated raw Pass 3')
    for i, babs in enumerate(babs_list):
        raw_name = 'raw_' + babs
        datalake.datastreams['pass3'].data_stream[index_dic[babs], :] = datalake.datastreams['pass3'].data_stream[index_dic[raw_name], :] * babs_405_532_781[i]
    # calibration loop bsca
    for i, bsca in enumerate(bsca_list):
        raw_name = 'raw_' + bsca
        datalake.datastreams['pass3'].data_stream[index_dic[bsca], :] = datalake.datastreams['pass3'].data_stream[index_dic[raw_name], :] * bsca_405_532_781[i]

    return datalake
