import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)

import glob
import time
import datetime
import numpy as np
import xarray as xr
import tempfile
from pathlib import Path
import shutil
import argparse

from dask.distributed import Client, LocalCluster, progress, wait

import pyart

#-----------------
# Define Functions
#-----------------
def glue_fix(ds):
    # Define the required encodings for the glue files
    encodings = {'DBZ' : {'_FillValue' : -32768.0},
                 'VEL' : {'_FillValue' : -32768.0},
                 'WIDTH' : {'_FillValue' : -32768.0},
                 'ZDR' : {'_FillValue' : -32768.0},
                 'PHIDP' : {'_FillValue' : -32768.0},
                 'RHOHV' : {'_FillValue' : -32768.0},
                 'NCP' : {'_FillValue' : -32768.0},
                 'DBZhv' : {'_FillValue' : -32768.0},
                }   
        
    # Loop over all the variables; update the FillValue and Data Type
    for var in encodings:
        # Convert all values within the DataArray to the correct Fill Value
        # NOTE: xr.where(condition, value_when_condition_is_not_met); so if every index is MVC, check for opposite
        mask = ds[var].where(ds[var] > -99800, encodings[var]['_FillValue'])
        # Append the corrected data to the variable.
        ds[var] = mask
    
    return ds

def radar_glue(b_radar, radar_list):
    if radar_list is not None:
        for rad in radar_list:
            b_radar = pyart.util.join_radar(b_radar, rad)
            del rad
    else:
        b_radar = None
    return b_radar

def volume_from_list(base_radar, vlist):
    try:
        radars = [pyart.io.read(sw) for sw in vlist[1::]]
    except:
        radars = None
    return radar_glue(base_radar, radars)

def fix_times(ds):
    # Determine number of specific times within the file
    specific = set(ds.time.data)
    # Loop through the specific times, and add offset
    for value in specific:
        dt = np.arange(0, len(ds.sel(time=slice(value, value)).time.data))
        dt = dt.astype('timedelta64[ms]')
        # add the time offset to the original times
        new_times = ds.sel(time=slice(value, value)).time.data + dt
        ds.sel(time=slice(value, value)).time.data[:] = new_times
    # Send back to the main
    return ds

def granule(Dvolume):
    print('in granule')
    n_tilts = 8
    #data_dir = "/gpfs/wolf/atm124/proj-shared/gucxprecipradarS2.00/nc_files/" + month + "_nc/"
    #out_dir = "/gpfs/wolf/atm124/proj-shared/gucxprecipradarS2.00/glue_files/" + month + "_glued/"
    month = Dvolume[0].split('/')[-2].split('_')[0]
    out_dir = Dvolume[0].split('nc_files')[0] + "glue_files/" + month + "_glued/"

    # Read the base scan to determine if it can be read in
    if len(Dvolume) == 8:
        try:
            base_rad = pyart.io.read(Dvolume[0])
        except:
            base_rad = None
        # Read all scans and join with base scan
        if base_rad is not None:
            out_radar = volume_from_list(base_rad, Dvolume)
            if out_radar is not None:
                # Define the filename time from the radar object
                ff = time.strptime(out_radar.time['units'][14:], '%Y-%m-%dT%H:%M:%SZ')
                dt = datetime.datetime.fromtimestamp(time.mktime(ff)) + datetime.timedelta(seconds= int(out_radar.time['data'][0]))
                strform = dt.strftime(out_dir + 'xprecipradar_guc_volume_%Y%m%d-%H%M%S.b1.nc')
                #FIX for join issue.. to be fixed in Py-ART
                out_radar.sweep_mode['data']=np.tile(base_rad.sweep_mode['data'], n_tilts)
                try:
                    pyart.io.write_cfradial(strform, out_radar, arm_time_variables=True)
                    print('SUCCESS', strform)
                except:
                    print('FAILURE', strform)
                # Delete the radars to free up memory
                del base_rad
                del out_radar
            # Fix the times and encodings of the generated file
            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_path = Path(tmpdir)
                with xr.open_dataset(strform, mask_and_scale=False) as ds:
                    ds = ds.load()
                    ds = fix_times(ds)
                    ds = glue_fix(ds)
                    out_path = str(tmp_path) + '/' + strform.split('/')[-1]
                    # set time encoding for miliseconds
                    ds.time.encoding['units'] = 'milliseconds since 1970-01-01'
                    ds.to_netcdf(path=out_path)
                shutil.copy(out_path, strform)

def main(args):
    print("process start time: ", time.strftime("%H:%M:%S"))
    # Define directories
    month = args.month
    path = '/gpfs/wolf/atm124/proj-shared/gucxprecipradarS2.00/nc_files/%s_nc/*.nc' % month
    out_path = '/gpfs/wolf/atm124/proj-shared/gucxprecipradarS2.00/glue_files/%s_glued/' % month

    # Define files and determine volumes
    all_files = sorted(glob.glob(path))
    base_scan_ppi = '1_PPI.nc'
    ppi_pattern = 'PPI.nc'
    base_scans = []
    volumes = []
    ppis = []
    in_volume = False
    for nfile in all_files:
        if ppi_pattern in nfile:
            ppis.append(nfile)
        if base_scan_ppi in nfile:
            base_scans.append(nfile)
    
    n_tilts = 8

    volumes = []
    for base in base_scans:
        base_scan_index = np.where(np.array(ppis) == base)[0][0]
        volume = ppis[base_scan_index: base_scan_index+n_tilts]
        volumes.append(volume)
    
    if args.serial is True:
        granule(volumes[0])
    else:
        cluster = LocalCluster(n_workers=20, processes=True, threads_per_worker=1)
        with Client(cluster) as c:
            results = c.map(granule, volumes)
            wait(results)
        print("processing finished: ", time.strftime("%H:%M:%S"))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description="Creation of .b1 glue files for SAIL")

    parser.add_argument("--month",
                        default="202203",
                        dest='month',
                        type=str,
                        help="Month to process in YYYYMM format"
    )
    parser.add_argument("--serial",
                        default=False,
                        dest='serial',
                        type=bool,
                        help="Process in Serial"
    )
    args = parser.parse_args()

    main(args)
