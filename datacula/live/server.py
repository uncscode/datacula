# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 16:19:00 2022
Reads in data from client and pre-processes for ease of use with the Dash script
@author: Spencer Jordan
"""

# linting disabled until reformatting of this file
# pylint: disable=all
# flake8: noqa
# pytype: skip-file

import socket
import threading
import time
import os
import glob
import datetime

## Header size
HEADER = 64
## Size of data chunks
SIZE = 8192
PORT = 5556
SERVER = '192.168.0.81'
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(ADDR)



def handle_client(conn, addr):
    """
    Handles client connections
    """
    connected = True
    while connected:
        data = str()
        # Will include both the file and directory --> e.g. 'NO2_data/filename.dat'
        file_name = conn.recv(SIZE).decode(FORMAT)
        print(file_name)

        # Backup initiated
        if file_name == 'SAVE':
            current_time = datetime.datetime.now()
            ## Only backing up the current day's files
            current_day = current_time.strftime('%Y%m') # monthly
            files_to_send = glob.glob('/home/pi/server_project/server_files/*/*')
            data_frames = glob.glob('/home/pi/server_project/dataframes/*')
            files_to_send = files_to_send + data_frames
            for file in files_to_send:
                if current_day not in file:
                    pass
                else:
                    with open(file,'r') as f:
                        data_to_save = (f.read()).encode(FORMAT)
                    data_length = str(len(data_to_save)).encode(FORMAT)
                    # Sending filename
                    conn.send(file.encode(FORMAT))
                    time.sleep(0.3)
                    # Sending length of file
                    conn.send(data_length)
                    time.sleep(0.3)
                    # Sending the data
                    conn.send(data_to_save)
                    time.sleep(0.3)
                    print(f'[BACKUP] Sending backup {file}')
            conn.send('END'.encode(FORMAT))

        #Send the master_dataframe to dash client
        elif file_name == 'DASH':
            file_list = glob.glob('/home/pi/server_project/dataframes/*.csv')
            ## Sending most recently EDITED dataframe
            file_to_send = max(file_list,key=os.path.getmtime)
            with open(file_to_send,'r') as f:
                data_to_send = f.read().encode(FORMAT)
            data_length = str(len(data_to_send)).encode(FORMAT)
            # Sending only the filename, no directories
            file_to_send = file_to_send.split('/')[-1]
            conn.send(file_to_send.encode(FORMAT))
            time.sleep(0.3)
            # Sending length of data
            conn.send(data_length)
            time.sleep(0.3)
            # Sending the data
            conn.send(data_to_send)
            time.sleep(0.6)
            print('Data sent to Dash Client')

        elif len(file_name) == 0:
            print('**Invalid File Name or Command, Closing Connection**')
            conn.close()
            pass

        ## Client data needs to be processed
        else:
            # Length of data file
            data_length = conn.recv(HEADER).decode(FORMAT)
            try:
                data_length = int(data_length)
            except:
                print(f'%%%%% ERROR: File size not read, disconnecting {addr} %%%%%')
                conn.close()
            # Recieves data until the specified length is reached
            while len(data) < data_length:
                msg = conn.recv(SIZE).decode(FORMAT)
                data += msg
            # Check to ensure data is not empty --> may want to wait for data...
            if len(data) == 0:
                print(f'%%%%% ERROR: No data streaming from {addr}, please check connection %%%%%')
                error = str(f'{file_name} data is empty, closing connection')
                conn.send(error.encode(FORMAT))
                conn.close()

            # Single data file is being passed to server
            else:
                # Make sure the file exists, if it does, leaves it intact
                os.makedirs(os.path.dirname(f'/home/pi/server_project/server_files/{file_name}'),exist_ok=True)
                # Remove the original file if rewriting all data --> going off length, could fail
                if (len(data) > 10000) or ('CFKAD' in file_name):
                    print('Starting new file')
                    with open(f'/home/pi/server_project/server_files/{file_name}','w') as f:
                        f.write(data)
                # Write the data to server_files
                else:
                    with open(f'/home/pi/server_project/server_files/{file_name}','a') as f:
                        f.write(data)
                msg = str(f'{file_name} data written to server')
                # Send success message back to client
                conn.send(msg.encode(FORMAT))
    conn.close()

def start():
    """
    Starts the server"""
    server.listen()
    print(f'[LISTENING] Server is listening on {SERVER}')
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client,args=(conn,addr))
        thread.start()
        print(f'[ACTIVE Connections] {threading.activeCount()-1}')



def main():
    try:
        print('[STARTING] Server is starting')
        start()
    except:
        print('Something went wrong, attempting to leave server open')

if __name__ == '__main__':
    main()
