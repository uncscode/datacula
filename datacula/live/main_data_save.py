#%%
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 21 08:38:24 2022
Script for Server to save all files to master DataFrame
@author: Spencer Jordan
"""

import pandas as pd
import glob
import time
##************Data loading functions, add as needed*****************##
from cafe_data_imports import SP2,NO2,PASS,picarro,N2O,CHOH,PAX,CPC_3010,CPC_3022,NOx,CH4,anemometer
from datetime import datetime
import numpy as np
import os
import subprocess

#%%
## Needed for every data source - can turn this into a .json file!
skipRowsDict = {'SP2_data':0,'NO2_data':0,'PASS3_data':0,
               'picarro_data':0,'N2O_data':0,'CHOH_data':0,
               'PAX_data':0,'CPC_3010_data':0,'CPC_3022_data':0,
               'NOx_data':0,'CH4_aeris_data':0,'anemometer_data':0}

currentFileDict = {'SP2_data':'none','NO2_data':'none','PASS3_data':'none',
               'picarro_data':'none','N2O_data':'none','CHOH_data':'none',
               'PAX_data':'none','CPC_3010_data':'none','CPC_3022_data':'none',
               'NOx_data':'none','CH4_aeris_data':'none','anemometer_data':'none'}

cols = ['datetime','NO2','N2O_aeris','H2O_aeris','CO_aeris','Babs405nm_1/Mm','Babs532nm_1/Mm',
              'Babs781nm_1/Mm','Bsca405nm_1.Mm','Bsca532nm_1.Mm','Bsca781nm_1.Mm','CO',
              'CO2','CH4','H2O','CH3OH','HCHO','Incand. Conc. (#/cc)','Scatter. Conc. (#/cc)',
              'Bscat(1/Mm)','Babs(1/Mm)','Bext(1/Mm)','SSA','BC Mass(ug/m3)',
              'CPC_3010_concentration [#/cm3]','CPC_3022_concentration [#/cm3]','NO2_2bt',
              'NO_2bt','NOx_2bt','CH4_aeris_me','H2O_aeris_me','C2H6_aeris_me','U','V','W','T','PI','RO',
              'X-Y_magnitude']


def save_main_data():
    ## Predefining dataframe
    main_data = pd.DataFrame(columns=cols)
    ## Makes sure all data is set to be numeric
    for key in main_data.keys():
        main_data[key] = pd.to_numeric(main_data[key])
    ## List of files NOT to loop through
    file_drop_list = []
    while True:
        now = datetime.now()
        ## Key to determine the creation day and label dataframe csv
        current_day = now.strftime('%Y%m%d')
        ## Main file list for that day
        file_list = glob.glob(f'/home/pi/server_project/server_files/*/*{current_day}*')
        ## Clears out pax timestamp data
        pax_time = glob.glob('/home/pi/server_project/server_files/PAX_timestamp/*')
        file_drop_list = file_drop_list + pax_time
        # Removing old daily files from the main loop
        for file in file_drop_list:
            try:
                file_list.remove(file)
            except:
                continue

        for file in file_list:
            ## Main file directoy --> e.g. 'NO2_data'
            file_type = file.split("/")[-2]
            ## Checks for only most RECENT files
            temp_list = []
            for f_name in file_list:
                if file_type in f_name:
                    temp_list.append(f_name)
            most_recent = max(temp_list,key=os.path.getmtime)

            # File is the most current file, function should iterate over this
            if (file == most_recent) & (current_day in file):
                ## Catches if the most recent file has changed, sets skiprows to zero
                if file != currentFileDict[file_type]:
                    skipRowsDict[file_type] = 0
                else:
                    pass
                ## Number of rows to skip when reading data
                skip_rows = skipRowsDict[file_type]
                ## Main data loading functions
                ## ADD new instrument here, could probably do this with a .json
                try:
                    if file_type == 'SP2_data':
                        data_to_add = SP2(file,skip_rows)
                    elif file_type == 'NO2_data':
                        data_to_add = NO2(file,skip_rows)
                    elif file_type == 'PASS3_data':
                        data_to_add = PASS(file,skip_rows)
                    elif file_type == 'picarro_data':
                        data_to_add = picarro(file,skip_rows)
                    elif file_type == 'N2O_data':
                        data_to_add = N2O(file,skip_rows)
                    elif file_type == 'CHOH_data':
                        data_to_add = CHOH(file,skip_rows)
                    elif file_type == 'PAX_data':
                        data_to_add = PAX(file,skip_rows)
                    elif file_type == 'CPC_3010_data':
                        data_to_add = CPC_3010(file,skip_rows)
                    elif file_type == 'CPC_3022_data':
                        data_to_add = CPC_3022(file,skip_rows)
                    elif file_type == 'NOx_data':
                        data_to_add = NOx(file,skip_rows)
                    elif file_type == 'CH4_aeris_data':
                        data_to_add = CH4(file,skip_rows)
                    elif file_type == 'anemometer_data':
                        data_to_add = anemometer(file,skip_rows)
#                    print(data_to_add.head(3))
                    #Data is being appended to dataFrame
                    if len(data_to_add) != 0:
                        main_data = pd.concat([main_data,data_to_add])
                        time.sleep(0.2)
                        print(f'concating data from {file}')
                    elif len(data_to_add) == 0:
                        print(f'Nothing to Concat From {file}')
                    currentFileDict[file_type] = file
                    skipRowsDict[file_type] += len(data_to_add)
                except:
                    print(f'{file} is a bad file, skipping for dataframe and visualization')
                    file_drop_list.append(file)
                    continue
            # Files are from the same day, but not current file --> Add to data but don't change skipRows or currentFile
            elif (file != most_recent) & (current_day in file):
                ## Always want whole file, so sets skiprows to zero
                skip_rows = 0
                try:
                    if file_type == 'SP2_data':
                        data_to_add = SP2(file,skip_rows)
                    elif file_type == 'NO2_data':
                        data_to_add = NO2(file,skip_rows)
                    elif file_type == 'PASS3_data':
                        data_to_add = PASS(file,skip_rows)
                    elif file_type == 'picarro_data':
                        data_to_add = picarro(file,skip_rows)
                    elif file_type == 'N2O_data':
                        data_to_add = N2O(file,skip_rows)
                    elif file_type == 'CHOH_data':
                        data_to_add = CHOH(file,skip_rows)
                    elif file_type == 'PAX_data':
                        data_to_add = PAX(file,skip_rows)
                    elif file_type == 'CPC_3010_data':
                        data_to_add = CPC_3010(file,skip_rows)
                    elif file_type == 'CPC_3022_data':
                        data_to_add = CPC_3022(file,skip_rows)
                    elif file_type == 'NOx_data':
                        data_to_add = NOx(file,skip_rows)
                    elif file_type == 'CH4_aeris_data':
                        data_to_add = CH4(file,skip_rows)
                    elif file_type == 'anemometer_data':
                        data_to_add = anemometer(file,skip_rows)
 #                   print(data_to_add.head(3))
                    #Data is being appended to dataFrame
                    if len(data_to_add) != 0:
                        main_data = pd.concat([main_data,data_to_add])
                        time.sleep(0.2)
                        print(f'concating data from {file}')
                    elif len(data_to_add) == 0:
                        print(f'Nothing to Concat from {file}')
                    ## Skip this file in the future
                    file_drop_list.append(file)
                except:
                    print(f'{file} is a bad file, skipping for dataframe and visualization')
                    file_drop_list.append(file)
                    continue
            # File is not from daily list --> Should not happen, but here just in case
            else:
                file_drop_list.append(file)
                print('non-daily files made it onto the list')

        ## Adjustments for index and datetime
        try:
            main_data.index = pd.to_datetime(main_data.index)
            print('Changed index to datetime')
        except:
            print('Did not need to change index to datetime')
            pass
        ## Sort the datetime index
        main_data = main_data.sort_index()
        #print(main_data.tail(3))
        print(main_data.head(3))
#        main_data.to_csv(f'/home/pi/server_project/dataframes/main_data_{current_day}_debug.csv',sep=',',index=True,na_rep=np.nan)
        ## Resample at desired frequency with mean values
        ##*********IF CHANGED: CHANGE DASH TIMEFRAME SPLICING*************##
        main_data_to_send = main_data.resample('2s').mean()
        #main_data_to_send = main_data
        main_data_to_send.to_csv(f'/home/pi/server_project/dataframes/main_data_{current_day}.csv',sep=',',index=True,na_rep=np.nan)
        print('data_saved')
        ## Interval to load new data files in at
        time.sleep(3)

def run():
    while True:
        try:
            ## Ensures all temporary files were properly deleted
            bash_command = str('rm /home/pi/server_project/server_files/*/*new*')
            process = subprocess.Popen(bash_command.split(),stdout=subprocess.PIPE)
            output, error = process.communicate()
            ## run main function
            save_main_data()
        except Exception as e:
            print(e)
            print('*********Save failed, restarting script***********')
            time.sleep(2)
            continue

if __name__ == '__main__':
    run()

