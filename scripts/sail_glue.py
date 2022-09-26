"""
Script to concatenate, or "glue", CSU X-Band radar elevation scans into a single volume

Written: Joe O'Brien <obrienj@anl.gov> - 15 Sept 2022
"""

import os
import sys
import numpy as np
import time
import datetime

from dask.distributed import Client, LocalCluster

import pyart

#-----------------
# Define Functions
#-----------------
# Define syntax. 
def help_message():
  print('\n')
  print('Syntax: sail_glue.py input_path output_path\n\n')
  print('PURPOSE:                                                    ')
  print('     To Create Volume Scans from Individual SAIL CSU X-Band Elevation Scans \n')
  print('  INPUT:                                                    ')
  print('     input_path    - Directory Path to SAIL Data ')
  print('     output_path   - Directory Path to Output Data\n')
  print('  Example: python sail_glue.py /202203/ /202203/glued \n')

def radar_glue(b_radar, radar_list):
    for rad in radar_list:
        b_radar = pyart.util.join_radar(b_radar, rad)
    
    return b_radar

def volume_from_list(vlist):
    try:
        base_radar = pyart.io.read(vlist[0])
        radars = [pyart.io.read(sw) for sw in vlist[1::]]
        glue = radar_glue(base_radar, radars)
        del base_radar
        del radars
    except:
        glue = []
        pass
    
    return glue

def granule(Dvolume, OUT_DIR):
    #OUTPUT_DIR = '/gpfs/wolf/atm124/proj-shared/gucxprecipradarS2.00/glue_files/202203_glue/'
    if len(Dvolume) == 8:
        try:
            base_rad = pyart.io.read(Dvolume[0])

            out_radar = volume_from_list(Dvolume)
            if out_radar:
                ff = time.strptime(out_radar.time['units'][14:], '%Y-%m-%dT%H:%M:%SZ')
                dt = datetime.datetime.fromtimestamp(time.mktime(ff)) + datetime.timedelta(seconds = int(out_radar.time['data'][0]))
                strform = dt.strftime(OUTPUT_DIR + 'xprecipradar_guc_volume_%Y%m%d-%H%M%S.b1.nc')
                print(strform)
                #FIX for join issue.. to be fixed in Py-ART
                out_radar.sweep_mode['data'] = np.tile(base_rad.sweep_mode['data'], N_TILTS)
                nwrite = pyart.io.write_cfradial(strform, out_radar)
                del out_radar
                del nwrite
            del base_rad
        except:
            print("FAILED GRANULE")
            pass

#-----------------
# Input Parameters
#-----------------
# Check input parameters. 
for param in sys.argv:
  if param.startswith('-h') | param.startswith('-help') | param.startswith('--h'):
    help_message()
    exit()
# Check to make sure correct number of input parameters are sent. 
if (len(sys.argv) > 2):
  OUTPUT_DIR = sys.argv[-2]
  DATA_DIR = sys.argv[-1]
else:
  help_message()
  exit()

#----------------------------
# Define processing variables
#----------------------------
# Define location of the raw data - NOTE: Must be untarred!
#DATA_DIR = '/Users/jrobrien/ARM/data/CSU-XPrecipRadar/raw/tmp/'
# Define the suffix of the base scan
BASE_SCAN_PPI = '1_PPI.nc'
# Define the desired suffix of the volume file
PPI_PATTERN = 'PPI.nc'
# Define the number of elevation levels
N_TILTS = 8

# Select the days to process
DAY = 'gucxprecipradarS2.00.' + '202203*'

#--------------------
# Create Volume Scans
#--------------------
# sort the input files
all_files = glob.glob(DATA_DIR + '*.nc')
all_files.sort()

# Iterate over the files within the directory.
# Determine which are base scans and which are ppi scans
# NOTE: There are RHI scans within the tar file not used.
base_scans = []
volumes = []
ppis = []
in_volume = False
for file in all_files:
    if PPI_PATTERN in file:
        ppis.append(file)
    if BASE_SCAN_PPI in file:
        base_scans.append(file)

# Determine the scan volumes
volumes = []
for base in base_scans:
    base_scan_index = np.where(np.array(ppis) == base)[0][0]
    volume = ppis[base_scan_index: base_scan_index + N_TILTS]
    volumes.append(volume)

#--------------------
# Setup Dask Cluster
#--------------------

# Start up a Dask Cluster for Processing the Granule function
from dask.distributed import Client, LocalCluster

cluster = LocalCluster()

#cluster.scale(16)  # Sets the number of workers to 10
#cluster.adapt(minimum=8, maximum=16)
client = Client(cluster)

# Use Dask distributed map utility to call the granule function
future = client.map(granule, volumes, OUTPUT_DIR)

my_data = client.gather(future)

# Check on the client
#print(client)