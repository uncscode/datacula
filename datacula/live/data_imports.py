# -*- coding: utf-8 -*-
"""
Created on Tue Apr 12 13:07:46 2022

Cleaning data imports from CAFE instruments
All have datetime dtype as index column, labeled 'datetime'
Most have to rewrite to new files to get clean data --> deleteing headers/erroneous lines

@author: Spencer Jordan
"""
# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import pandas as pd
import numpy as np
import os
import time
import datetime

## Time adjustment --> This correlates with the converstion between GMT and whatever timezone YOU are in
## 'time_adj' is the number of seconds btwn GMT and the timezone listed
## 'time_delta' is only necessary when converting between mtn time and other timezones --> since several instruments like the SP2 are set to mountain time
# Mountain time
#time_adj = 21600
time_delta = 0

# Central Time
time_adj = 18000
#time_delta = 3600


## Names of columns to import from data files
pass_names = ['DATE','TIME','Babs405nm_1/Mm','Babs532nm_1/Mm','Babs781nm_1/Mm','Bsca405nm_1.Mm',
              'Bsca532nm_1.Mm','Bsca781nm_1.Mm']

sp2_names = ['Time (sec)','Sample Flow Set (vccm)','Sample Flow LFE (vccm)',
             'Power (V)','Incand. Conc. (#/cc)','Sheath Flow Set (vccm)',
             'Sheath Flow Read (vccm)','YAG Crystal Temp (C)','YAG Heat Sink Temp (C)',
             'mber Temp (C)','Chamber Pressure (mBar)','Pump Diode Power (V)',
             'Duty Cycle (%)','Timestamp','Elapsed Time','Error Code',
             'Electronics Temp (C)','Aux Input 0 (V)','Sheath Flow Read (sccm)',
             'Purge Flow Read (sccm)','Chan 0 HV (V)','Aux Input 1 (V)','Aux Input 2 (V)',
             'Chan 3 HV (V)','Purge Flow Read (vccm)','Purge Flow Set (vccm)','Exhaust Valve Set (Volts)',
             'Sheath Flow Set (sccm)','Scatter. Conc. (#/cc)','Primary Threshold',
             'Secondary Threshold','Num of Buffers','Num of Particles','Num Written to File',
             'Num in File','Laser Current (mA)','Laser Curr Setpoint (mA)','Laser Voltage (V)',
             'Laser Temp (C)','Laser TEC Temp (C)','Laser TEC Temp Setpoint','Laser TEC Current (mA)']

sp2_desired = ['Time (sec)','Incand. Conc. (#/cc)','Scatter. Conc. (#/cc)','Num of Particles']

picarro_names = ['EPOCH_TIME','CO','CO2','CH4','H2O']
no2_names = ['EPOCH_TIME','NO2']
n2o_names = ['EPOCH_TIME','N2O_aeris','H2O_aeris','CO_aeris']
form_names = ['EPOCH_TIME','HCHO','CH3OH']

## Picarro
def picarro(filename,skipRows):
    try:
        picarro = pd.read_csv(filename,comment='D',names=picarro_names,usecols=[5,17,18,20,22],delim_whitespace=True,skiprows=skipRows,on_bad_lines='skip')
        picarro['EPOCH_TIME'] = picarro['EPOCH_TIME']-time_adj
        picarro['datetime'] = pd.to_datetime(picarro['EPOCH_TIME'],unit='s')
        picarro.set_index('datetime',inplace=True)
        picarro = picarro.drop('EPOCH_TIME',axis=1)
        picarro['CO'] = pd.to_numeric(picarro['CO'])
        picarro['CO2'] = pd.to_numeric(picarro['CO2'])
        picarro['CH4'] = pd.to_numeric(picarro['CH4'])
        picarro['H2O'] = pd.to_numeric(picarro['H2O'])
#        print(picarro.head(2))
    except Exception as e:
        print(e)
        print('****************PICARRO DATA FAILED*****************')
    return picarro

## SP2
def SP2(filename,skipRows):
    try:
        filename_new = filename.split('.')[0]+'_new.hk'
        with open(filename, 'r') as f_in, open(filename_new, 'w') as f_out:
            lines = f_in.readlines()
            header_1 = 'Time (sec)'
            for line in lines:
                if (header_1 not in line):
                    f_out.write(line)
        ## Pulls the date from the filename
        date = filename.split('/')[-1][0:8]
        sp2 = pd.read_csv(filename_new,delim_whitespace=True,names=sp2_names,on_bad_lines='skip',
                          skiprows=skipRows)
        for col in sp2:
            if col not in sp2_desired:
                sp2=sp2.drop(col,axis=1)
        sp2['datetime'] = pd.to_datetime(date,format='%Y%m%d')+pd.to_timedelta(sp2['Time (sec)'],unit='s')+pd.to_timedelta(time_delta,unit='s')
        sp2.set_index('datetime',inplace=True)
        os.remove(filename_new)
#        print(sp2.head(2))
    except Exception as e:
        print(e)
        print('******************SP2 DATA FAILED******************')
        os.remove(filename_new)
    return sp2

## NO2
def NO2(filename,skipRows):
    try:
        filename_new = filename.split('.')[0]+'_new.dat'
        with open(filename, 'r') as f_in, open(filename_new, 'w') as f_out:
            lines = f_in.readlines()
            #header_1 = 'TEST'
            header_2 = 'Logfile'
            header_3 = 'Starttime:'
            #header_4 = 'Local Time:'
            header_5 = 'Epoch_time(s since 1/1/1970 UTC)'
            for line in lines:
                if (header_2 not in line) & (header_3 not in line) & (header_5 not in line):
                    f_out.write(line)
        time.sleep(0.3)
        no2 = pd.read_csv(filename_new,names=no2_names,usecols=[0,11],skiprows=skipRows,on_bad_lines='warn',comment='E')
        no2['EPOCH_TIME'] = no2['EPOCH_TIME']-time_adj
        no2['datetime'] = pd.to_datetime(no2['EPOCH_TIME'],unit='s')
        no2.set_index('datetime',inplace=True)
        no2 = no2.drop('EPOCH_TIME',axis=1)
        no2['NO2'] = pd.to_numeric(no2['NO2'],errors='coerce')
        os.remove(filename_new)
#        print(no2.head(2))
    except Exception as e:
        print(e)
        print('***********NO2 DATA FAILED**************')
        os.remove(filename_new)
        os.rename(filename,filename.split('.')[0]+'_error.dat')
    return no2


def PASS(filename,skipRows):
    try:
        pas = pd.read_csv(filename,usecols=[0,1,5,6,7,33,34,35],names=pass_names,
                          skiprows=skipRows,on_bad_lines='skip',low_memory=False,comment='D')
        pas['DATE'] = pd.to_datetime(pas['DATE'],format='%Y%m%d')
        pas['TIME'] = pd.to_timedelta(pas['TIME'])
        pas['datetime'] = pas['DATE']+pas['TIME']+pd.to_timedelta(time_delta,unit='s')
        pas.set_index('datetime',inplace=True)
        pas = pas.drop('DATE',axis=1)
        pas = pas.drop('TIME',axis=1)
#        print(pas.head(2))
    except Exception as e:
        print(e)
        print('************PASS3 DATA FAILED*****************')
    return pas

## N2O
def N2O(filename,skipRows):
    filename_new = filename.split('.')[0]+'_new.dat'
    try:
        with open(filename,'r') as f_in, open(filename_new,'w') as f_out:
            lines = f_in.readlines()
            header_1 = 'TEST'
            header_2 = 'Logfile'
            header_3 = 'UTC Time:'
            header_4 = 'Local Time:'
            header_5 = 'Epoch_time(s since 1/1/1970 UTC)'
            header_6 = '4'
            for line in lines:
                if (header_1 not in line) & (header_2 not in line) & (header_3 not in line) & (header_4 not in line) & (header_5 not in line) & (header_6 != line[0]):
                    f_out.write(line)
        time.sleep(0.2)
        n2o = pd.read_csv(filename_new,names=n2o_names,usecols=[0,5,6,7],skiprows=skipRows,on_bad_lines='skip')
        n2o['EPOCH_TIME'] = n2o['EPOCH_TIME']-time_adj
        n2o['datetime'] = pd.to_datetime(n2o['EPOCH_TIME'],unit='s')
        n2o.set_index('datetime',inplace=True)
        n2o = n2o.drop('EPOCH_TIME',axis=1)
        n2o['H2O_aeris'] = pd.to_numeric(n2o['H2O_aeris'])
        n2o['CO_aeris'] = pd.to_numeric(n2o['CO_aeris'])
        n2o['N2O_aeris'] = pd.to_numeric(n2o['N2O_aeris'])
        os.remove(filename_new)
#        print(n2o.head(2))
    except Exception as e:
        print(e)
        print('***********N2O DATA FAILED**************')
        os.remove(filename_new)
#    print(n2o.tail(2))
    return n2o

## Formaldehyde
def CHOH(filename,skipRows):
    filename_new = filename.split('.')[0]+'_new.dat'
    try:
        with open(filename,'r') as f_in, open(filename_new,'w') as f_out:
            lines = f_in.readlines()
            header_1 = 'TEST'
            header_2 = 'Logfile'
            header_3 = 'UTC Time:'
            header_4 = 'Local Time:'
            header_5 = 'Epoch_time(s since 1/1/1970 UTC)'
            for line in lines:
                if (header_1 not in line) & (header_2 not in line) & (header_3 not in line) & (header_4 not in line) & (header_5 not in line) & ('4' != line[0]):
                    f_out.write(line)
        form = pd.read_csv(filename_new,names=form_names,usecols=[0,6,8],skiprows=skipRows,on_bad_lines='skip')
        form['EPOCH_TIME'] = pd.to_numeric(form['EPOCH_TIME'])
        form['EPOCH_TIME'] = form['EPOCH_TIME']-time_adj
        form['datetime'] = pd.to_datetime(form['EPOCH_TIME'],unit='s')
        form.set_index('datetime',inplace=True)
        form = form.drop('EPOCH_TIME',axis=1)
        time.sleep(1.0)
        os.remove(filename_new)
#        print(form.head(2))
    except Exception as e:
        print(e)
        print('*******CHOH DATA FAILED********')
        os.remove(filename_new)
    return form

pax_names = ['Date','Time','Bscat(1/Mm)','Babs(1/Mm)','Bext(1/Mm)','SSA','BC Mass(ug/m3)']
def PAX(filename,skipRows):
    try:
        pax = pd.read_csv(filename,names=pax_names,skiprows=skipRows,on_bad_lines='skip',comment='D')
        pax['datetime'] = pax['Date'] + ' ' +  pax['Time']
        pax['datetime'] = pd.to_datetime(pax['datetime'],format='%Y-%m-%d %H:%M:%S')
        ### PAX Time Adjustment --> Needs some work
        #time_file = '/home/pi/server_project/server_files/PAX_timestamp/'+filename.split('/')[-1].split('.')[0]+'_timestamp.csv'
        #time_data = pd.read_csv(time_file,usecols=[7],names=['Pi_time'],on_bad_lines='skip',comment='D')
        #print('time file opened')
        #time_diff = pax['datetime'].iloc[-1] - pd.to_datetime(time_data['Pi_time'].iloc[-1],format='%Y%m%d %X')
        #print('time diff calculated')
        #pax['datetime'] = pax['datetime'] - time_diff
        #print('time diff applied')
        ###
        pax.set_index('datetime',inplace=True)
        pax = pax.drop(['Date','Time'],axis=1)
#        print(pax.head(2))
    except Exception as e:
        print(e)
        print('***********PAX DATA FAILED************')
    return pax

def CPC_3010(filename,skipRows):
    cpc_names = ['EPOCH_TIME','CPC_3010_one_sec']
    try:
       cpc = pd.read_csv(filename,names=cpc_names,usecols=[0,1],skiprows=skipRows,on_bad_lines='skip',comment='C')
       cpc['EPOCH_TIME'] = cpc['EPOCH_TIME'] - time_adj
       cpc['datetime'] = pd.to_datetime(cpc['EPOCH_TIME'],unit='s')
       cpc = cpc.drop('EPOCH_TIME',axis=1)
       cpc['CPC_3010_concentration [#/cm3]'] = cpc['CPC_3010_one_sec']*0.06
       cpc.set_index('datetime',inplace=True)
       cpc = cpc[cpc['CPC_3010_one_sec'] > 120000]
       cpc = cpc.drop('CPC_3010_one_sec',axis=1)
       cpc['CPC_3010_concentration [#/cm3]'] = pd.to_numeric(cpc['CPC_3010_concentration [#/cm3]'])
#       print(cpc.head(2))
    except Exception as e:
        print(e)
        print('***************CPC 3010 DATA FAILED*************')
    return cpc

def CPC_3022(filename,skipRows):
    cpc_names = ['EPOCH_TIME','CPC_3022_one_sec','CPC_3022_flowrate']
    try:
       cpc = pd.read_csv(filename,names=cpc_names,usecols=[0,1,3],skiprows=skipRows,on_bad_lines='skip',comment='C')
       cpc['EPOCH_TIME'] = cpc['EPOCH_TIME'] - time_adj
       cpc['datetime'] = pd.to_datetime(cpc['EPOCH_TIME'],unit='s')
       cpc = cpc.drop('EPOCH_TIME',axis=1)
       cpc['CPC_3022_concentration [#/cm3]'] = cpc['CPC_3022_one_sec']*(1/cpc['CPC_3022_flowrate'])
       cpc.set_index('datetime',inplace=True)
       cpc = cpc.drop('CPC_3022_one_sec',axis=1)
       cpc = cpc.drop('CPC_3022_flowrate',axis=1)
       cpc['CPC_3022_concentration [#/cm3]'] = pd.to_numeric(cpc['CPC_3022_concentration [#/cm3]'])
#       print(cpc.head(2))
    except Exception as e:
        print(e)
        print('***************CPC 3022 DATA FAILED*************')
    return cpc

def NOx(filename,skipRows):
    nox_names = ['NO2_2bt','NO_2bt','NOx_2bt','EPOCH_TIME']
    try:
        nox = pd.read_csv(filename,names=nox_names,usecols=[1,2,3,15],skiprows=skipRows,on_bad_lines='skip')
        nox['EPOCH_TIME'] = nox['EPOCH_TIME']-time_adj
        nox['datetime'] = pd.to_datetime(nox['EPOCH_TIME'],unit='s')
        nox.set_index('datetime',inplace=True)
        nox = nox.drop('EPOCH_TIME',axis=1)
    except Exception as e:
        print(e)
        print('*******************NOx Data Failed**************')
    return nox

## Aeris CH4
ch4_names = ['EPOCH_TIME','CH4_aeris_me','H2O_aeris_me','C2H6_aeris_me']

def CH4(filename,skipRows):
    try:
        filename_new = filename.split('.')[0]+'_new.dat'
        with open(filename, 'r') as f_in, open(filename_new, 'w') as f_out:
            lines = f_in.readlines()
            #header_1 = 'TEST'
            header_2 = 'Logfile'
            header_3 = 'UTC Time:'
            header_4 = 'Local Time:'
            header_5 = 'Epoch_time(s since 1/1/1970 UTC)'
            for line in lines:
                if (header_2 not in line) & (header_3 not in line) & (header_4 not in line) & (header_5 not in line) & ('4' != line[0]):
                    f_out.write(line)
        ch4 = pd.read_csv(filename_new,names=ch4_names,usecols=[0,4,5,6],skiprows=skipRows,on_bad_lines='warn',comment='E')
        ch4['EPOCH_TIME'] = ch4['EPOCH_TIME']-time_adj
        ch4['datetime'] = pd.to_datetime(ch4['EPOCH_TIME'],unit='s')
        ch4.set_index('datetime',inplace=True)
        ch4 = ch4.drop('EPOCH_TIME',axis=1)
        #ch4['CH4_aeris_me'] = pd.to_numeric(CH4['CH4_aeris_me'],errors='coerce')
        #ch4['H2O_aeris_me'] = pd.to_numeric(CH4['H2O_aeris_me'],errors='coerce')
        #ch4['C2H6_aeris_me'] = pd.to_numeric(CH4['C2H6_aeris_me'],errors='coerce')
        time.sleep(1.0)
        os.remove(filename_new)
        #print(ch4.tail(3))
    except Exception as e:
        print(e)
        print('***********CH4 DATA FAILED**************')
        os.remove(filename_new)
        os.rename(filename,filename.split('.')[0]+'_error.dat')
    return ch4

anemometer_names = ['EPOCH_TIME','U','V','W','T','PI','RO']
def anemometer(filename,skipRows):
    try:
        ane = pd.read_csv(filename,names=anemometer_names,usecols=[0,2,4,6,8,10,12],delim_whitespace=True,skiprows=skipRows,on_bad_lines='warn')
        ane['EPOCH_TIME'] = ane['EPOCH_TIME']-time_adj
        ane['datetime'] = pd.to_datetime(ane['EPOCH_TIME'],unit='s')
        ane.set_index('datetime',inplace=True)
        ane = ane.drop('EPOCH_TIME',axis=1)
        ane['U'] = pd.to_numeric(ane['U'],errors='coerce')
        ane['V'] = pd.to_numeric(ane['V'],errors='coerce')
        ane['W'] = pd.to_numeric(ane['W'],errors='coerce')
        ane['T'] = pd.to_numeric(ane['T'],errors='coerce')
        ane['PI'] = pd.to_numeric(ane['PI'],errors='coerce')
        ane['RO'] = pd.to_numeric(ane['RO'],errors='coerce')
        ane['X-Y_magnitude'] = (ane['U']**2 + ane['V']**2)**(1/2)
    except Exception as e:
        print(e)
        print('************* ANEMOMETER DATA FAILED ************')
    return ane

