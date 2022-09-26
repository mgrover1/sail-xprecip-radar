"""
Script to process a month of the Extracted Radar Columns and In-Situ Sensors (RadCLss)

Written: Joe O'Brien <obrienj@anl.gov> - 26 Sept 2022
"""

import glob
import os
import datetime

import numpy as np
import matplotlib.pyplot as plt
import xarray as xr
import pandas as pd
from matplotlib.dates import DateFormatter
from matplotlib import colors
from calendar import monthrange

import pyart
import act

#-----------------
# Define Functions
#-----------------
# Define syntax. 
def help_message():
  print('\n')
  print('Syntax: sail_radclss.py input_month output_dir\n\n')
  print('PURPOSE:                                                    ')
  print('     To Create RadCLss Dataset for input month \n')
  print('  INPUT:                                                    ')
  print('     input_month [YYYYMM] - SAIL CMAC2.0 Data Month to Process')
  print('     output_dir           - Directory to hold the RadCLss dataset\n')
  print('  Example: python sail_radclss.py 202203/ /202203/glued \n')

def subset_points(file, lats, lons, sites):
    """Subset a radar file for a set of latitudes and longitudes"""
    
    # Read in the file
    radar = pyart.io.read(file)
    
    column_list = []
    for lat, lon in zip(lats, lons):
        # Make sure we are interpolating from the radar's location above sea level
        # NOTE: interpolating throughout Troposphere to match sonde to in the future
        da = pyart.util.columnsect.get_field_location(radar, lat, lon).interp(height=np.arange(np.round(radar.altitude['data'][0]), 10100, 100))
        # Add the latitude and longitude of the extracted column
        da["latitude"], da["longitude"] = lat, lon
        # Time is based off the start of the radar volume
        dt = pd.to_datetime(radar.time["data"], unit='s')[-1]
        da["time"] = [dt]
        column_list.append(da)
    # Concatenate the extracted radar columns for this scan across all sites    
    ds = xr.concat(column_list, dim='site')
    ds["site"] = sites
    # Add attributes for Time, Latitude, Longitude, and Sites
    ds.time.attrs.update(long_name=('Time in Seconds that Cooresponds to the Start'
                                    + " of each Individual Radar Volume Scan before"
                                    + " Concatenation"),
                         description=('Time in Seconds that Cooresponds to the Minimum'
                                      + ' Height Gate'))
    ds.site.attrs.update(long_name="SAIL/SPLASH In-Situ Ground Observation Site Identifers")
    ds.latitude.attrs.update(long_name='Latitude of SAIL Ground Observation Site',
                             units='Degrees North')
    ds.longitude.attrs.update(long_name='Longitude of SAIL Ground Observation Site',
                             units='Degrees East')
    return ds

def match_datasets_act(column, ground, site, discard, resample='sum', DataSet=False):
    """
    Time synchronization of a Ground Instrumentation Dataset to 
    a Radar Column for Specific Locations using the ARM ACT package
    
    Parameters
    ----------
    column : Xarray DataSet
        Xarray DataSet containing the extracted radar column above multiple locations.
        Dimensions should include Time, Height, Site
             
    ground : str; Xarray DataSet
        String containing the path of the ground instrumentation file that is desired
        to be included within the extracted radar column dataset. 
        If DataSet is set to True, ground is Xarray Dataset and will skip I/O. 
             
    site : str
        Location of the ground instrument. Should be included within the filename. 
        
    discard : list
        List containing the desired input ground instrumentation variables to be 
        removed from the xarray DataSet. 
    
    resample : str
        Mathematical operational for resampling ground instrumentation to the radar time.
        Default is to sum the data across the resampling period. Checks for mean. 
    
    DataSet : boolean
        Boolean flag to determine if ground input is an Xarray Dataset.
        Set to True if ground input is Xarray DataSet. 
             
    Returns
    -------
    ds : Xarray DataSet
        Xarray Dataset containing the time-synced in-situ ground observations with
        the inputed radar column 
    """
    # Check to see if input is xarray DataSet or a file path
    if DataSet == True:
        grd_ds = ground
    else:
        # Read in the file using ACT
        grd_ds = act.io.armfiles.read_netcdf(ground, cleanup_qc=True, drop_variables=discard)
        # Default are Lazy Arrays; convert for matching with column
        grd_ds = grd_ds.compute()
        
    # Remove Base_Time before Resampling Data since you can't force 1 datapoint to 5 min sum
    if 'base_time' in grd_ds.data_vars:
        del grd_ds['base_time']
        
    # Check to see if height is a dimension within the ground instrumentation. 
    # If so, first interpolate heights to match radar, before interpolating time.
    if 'height' in grd_ds.dims:
        grd_ds = grd_ds.interp(height=np.arange(column['height'].data[0], 10100, 100), method='linear')
        
    # Check to see if ground instrumentation is the RWP, as it has conflicting variable names
    # with the CMAC2.0 extracted radar columns
    if 'signal_to_noise_ratio' in grd_ds.data_vars:
        grd_ds = grd_ds.rename_vars(signal_to_noise_ratio='rwp_signal_to_noise_ratio')

    # Resample the ground data to 5 min and interpolate to the CSU X-Band time. 
    # Keep data variable attributes to help distingish between instruments/locations
    if resample.split('=')[-1] == 'mean':
        matched = grd_ds.resample(time='5Min', 
                                  closed='right').mean(keep_attrs=True).interp(time=column.time, 
                                                                               method='linear') 
    else:
        matched = grd_ds.resample(time='5Min', 
                                  closed='right').sum(keep_attrs=True).interp(time=column.time, 
                                                                              method='linear')
    
    # Add SAIL site location as a dimension for the Pluvio data
    matched = matched.assign_coords(coords=dict(site=site))
    matched = matched.expand_dims('site')
   
    # Remove Lat/Lon Data variables as it is included within the Matched Dataset with Site Identfiers
    if 'lat' in matched.data_vars:
        del matched['lat']
    if 'lon' in matched.data_vars:
        del matched['lon']
        
    # Update the individual Variables to Hold Global Attributes
    # global attributes will be lost on merging into the matched dataset.
    # Need to keep as many references and descriptors as possible
    for var in matched.data_vars:
        matched[var].attrs.update(source=matched.datastream)
        
    # Merge the two DataSets
    column = xr.merge([column, matched])
   
    return column

#---------------------
# Processing Variables
#---------------------
# Define the directory where the CSU-X Band CMAC2.0 files are located.
RADAR_DIR = "/gpfs/wolf/atm124/proj-shared/gucxprecipradarcmacS2.c1/ppi/"
# Define an output directory for downloaded ground instrumentation
PLUVIO_DIR = '/gpfs/wolf/atm124/proj-shared/gucwbpluvio2M1.a1/'
MET_DIR = '/gpfs/wolf/atm124/proj-shared/gucmetM1.b1/'
LD_M1_DIR = '/gpfs/wolf/atm124/proj-shared/gucldM1.b1/'
LD_S2_DIR = '/gpfs/wolf/atm124/proj-shared/gucldS2.b1/'
SONDE_DIR = '/gpfs/wolf/atm124/proj-shared/gucsondewnpnM1.b1/'
RWP_DIR = '/gpfs/wolf/atm124/proj-shared/guc915rwpprecipmomenthighM1.a0/'

# Define ARM Username and ARM Token with ARM Live service for downloading ground instrumentation via ACT.DISCOVERY
# With your ARM username, you can find your ARM Live token here: https://adc.arm.gov/armlive/
ARM_USERNAME = os.getenv("ARM_USERNAME")
ARM_TOKEN = os.getenv("ARM_TOKEN")

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
  INPUT_MONTH = sys.argv[-2]
  OUTPUT_DIR = sys.argv[-1]
else:
  help_message()
  exit()

#-------------------------------
# Define Location of SAIL Sites
#-------------------------------
# Define the splash locations [lon,lat]
kettle_ponds = [-106.9731488, 38.9415427]
brush_creek = [-106.920259, 38.8596282]
avery_point = [-106.9965928, 38.9705885]
pumphouse_site = [-106.9502476, 38.9226741]
roaring_judy = [-106.8530215, 38.7170576]
M1 = [-106.987, 38.9267]
snodgrass = [-106.978929, 38.926572]

sites = ["M1", "kettle_ponds", "brush_creek", "avery_point", 
         "pumphouse_site", "roaring_judy", "snodgrass"]

# Zip these together!
lons, lats = list(zip(M1,
                      kettle_ponds,
                      brush_creek,
                      avery_point,
                      pumphouse_site,
                      roaring_judy,
                      snodgrass))

#-----------------------------------
# Match with Ground Instrumentation
#-----------------------------------

discard_var = {'LD' : ['base_time', 'time_offset', 'equivalent_radar_reflectivity_ott',
                      'laserband_amplitude', 'qc_equivalent_radar_reflectivity_ott',
                      'qc_laserband_amplitude', 'sensor_temperature',
                      'heating_current', 'qc_heating_current', 'sensor_voltage',
                      'qc_sensor_voltage', 'moment1', 'moment2', 'moment3', 'moment4',
                      'moment5', 'moment6', 'lat', 'lon', 'alt'
                      ],
               'Pluvio' : ['base_time', 'time_offset', 'load_cell_temp', 'heater_status',
                          'elec_unit_temp', 'supply_volts', 'orifice_temp', 'volt_min',
                          'ptemp', 'lat', 'lon', 'alt'
                          ],
               'Met' : ['base_time', 'time_offset', 'time_bounds', 'logger_volt', 'qc_logger_volt',
                       'logger_temp', 'qc_logger_temp', 'lat', 'lon', 'alt'
                       ],
               'Sonde' : ['base_time', 'time_offset', 'lat', 'lon'],
               'RWP' : ['base_time', 'time_offset', 'time_bounds', 'height_bounds',
                        'lat', 'lon', 'alt'
                       ]
              }

#------------------------------------------------------------------------------
# Loop through each day within the month directory, making daily RadCLss files
#------------------------------------------------------------------------------
for i in range(1, monthrange(INPUT_MONTH[0:4], INPUT_MONTH[4:])[1]):
    print(i)

    # Define the date.
    if i < 10:
        NDATE = INPUT_MONTH[0:4] + '0' + str(i)
    else:
        NDATE =  INPUT_MONTH[0:4] + str(i)

    #-------------------------------------------------------------
    # Grab all the CMAC processed files and Extract Radar Columns
    #-------------------------------------------------------------
    # With the user defined RADAR_DIR, grab all the XPRECIPRADAR CMAC files for the defined DATE
    file_list = sorted(glob.glob(RADAR_DIR + '/' + NDATE + '/*.nc'))
    file_list[:10]

    ##ds_list = []
    ##for file in file_list[:]:
    ##    print(file)
    ##    ds_list.append(subset_points(file, lats, lons, sites))

    # Concatenate all extracted columns across time dimension to form daily timeseries
    ##ds = xr.concat(ds_list, dim='time')

    ##ds.sel(site='M1').sel(height=slice(3300, 4500)).corrected_reflectivity.plot(x='time',
    ##                                                                            cmap='pyart_HomeyerRainbow')

    # Remove Global Attributes from the Column Extraction
    # Attributes make sense for single location, but not collection of sites. 
    ##ds.attrs = {}

    # Remove the Base_Time variable from extracted column
    ##del ds['base_time']

    #------------------------------------------
    # 1) Add in the Pluvio Weighing Bucket Data
    #------------------------------------------
    # Define the file path
    npluvio = sorted(glob.glob(PLUVIO_DIR + 'gucwbpluvio2M1.a1.' + NDATE + '*'))
    print(npluvio[:])
    # Define the site location based on the filename
    PLUV_SITE = 'M1'

    # Call Match Datasets ACT 
    ##ds = match_datasets_act(ds, npluvio[0], PLUV_SITE, discard=discard_var['Pluvio'])

    #-----------------------------------------------
    # 2) Add in the Surface Meteorological Station
    #-----------------------------------------------
    # Define the file path
    nmet = sorted(glob.glob(MET_DIR + 'gucmetM1.b1.' + NDATE + '*'))
    print(nmet)
    # Define the site location based on the filename
    MET_SITE = 'M1'

    # Call Match Datasets ACT
    ##ds = match_datasets_act(ds, nmet[0], MET_SITE, discard=discard_var['Met'])

    #----------------------------------------
    # 3) Add the Laser Disdrometer - M1 Site
    #----------------------------------------
    # Define the file path
    nld = sorted(glob.glob(LD_M1_DIR + 'gucldM1.b1.' + NDATE + '*'))
    print(nld)
    # Define the site location based on the filename
    LD_M1_SITE = 'M1'

    # Call Match Datasets ACT
    ##ds = match_datasets_act(ds, nld[0], LD_M1_SITE, discard=discard_var['LD'])

    #----------------------------------------
    # 4) Add the Laser Disdrometer - S2 Site
    #----------------------------------------
    # Define the file path
    ld_2 = sorted(glob.glob(LD_S2_DIR + 'gucldS2.b1.' + NDATE + '*'))
    print(ld_2)
    # Define the site location based on the filename
    LD_S2_SITE = 'S2'

    # Call Match Datasets ACT
    ##ds = match_datasets_act(ds, ld_2[0], LD_S2_SITE, discard=discard_var['LD'])

    #--------------------------------
    # 5) Add the Radar Wind Profiler
    #--------------------------------
    # Define the file path
    nrwp = sorted(glob.glob(RWP_DIR+ 'guc915rwpprecipmomenthighM1.a0.' + NDATE + '*'))
    print(nrwp)
    if len(nrwp) > 0:
        # Define the site location based on the filename
        RWP_SITE = 'M1'

        # Call Match Datasets ACT
        #ds = match_datasets_act(ds, nrwp[0], RWP_SITE, resample='mean', discard=discard_var['RWP'])

        # Check to make sure the RWP datastream merged in correctly
        #ds.sel(site='M1').doppler_velocity.plot(x='time')

    #----------------------------
    # 6) Add the Radiosonde Data
    #----------------------------
    # Search for all sonde files
    nsonde = sorted(glob.glob(SONDE_DIR+ "gucsondewnpnM1.b1." + NDATE + '*'))
    print(nsonde)

    # Open each individual Sonde File and Merge together
    sonde_list = []
    #for nfile in nsonde:
        #sonde = act.io.armfiles.read_netcdf(nfile)
        # Check to make sure launch was successful (e.g. time > 1 obs)
        #if sonde.time.shape[0] > 1:
         #   sonde_list.append(sonde)
    # Concatenate together the sonde xarray DataSets
    #ds_sonde = xr.concat(sonde_list, dim='time').compute()
    # Define the site location based on the filename
    SONDE_SITE = 'M1'

    #ds = match_datasets_act(ds, ds_sonde, sonde_site, DataSet=True, discard=discard_var['Sonde'])

    #-----------------------------------------------
    # Define Meta Data Standards for Matched Dataset
    #-----------------------------------------------
    # Call the Data Object Definitions for this datastream. 
    # Will create an xarray dataset which will contain the necessary meta data and variables. 
    #mdims = {"time": ds['time'].data.shape[0], "height": 70, "site": 8, "particle_size": 32, "raw_fall_velocity": 32}
    #out_ds = act.io.create_obj_from_arm_dod('xprecipradarradclss.c2', mdims, version='1.0')

    # Transform the matched dataset for consistent dimensions
    #ds = ds.transpose('time', 'height', 'site', 'particle_size', 'raw_fall_velocity')

    # Output Dataset has correct data attributes, supplied by the DOD. 
    # Update the output dataset variable values with the matched dataset. 
    #for var in out_ds.variables:
    #    if var not in out_ds.dims:
            # check to see if variable is within the matched dataset
            # note: it may not be if file is missing.
     #       if var in ds.variables:
    #            out_ds[var].data = ds[var].data

    # Update the coordinates with the matched dataset values
    #out_ds = out_ds.assign_coords(time = ds['time'].data, 
    #                              height = ds['height'].data, 
    #                              site = ds['site'].data, 
    #                              particle_size = ds['particle_size'].data,
    #                              raw_fall_velocity = ds['raw_fall_velocity'].data)

    #------------------
    # Save the Dataset
    #------------------
    # define a filename
    nout = 'xprecipradarradclss.c2.' + NDATE + '.000000.nc'
    print('output: ', nout)
    #out_ds.to_netcdf('xprecipradarradclss.c2.' + DATE.replace('-', '') + '.000000.nc')