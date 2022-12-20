#!/usr/bin/python

import pyart
import matplotlib.pyplot as plt
import numpy as np
import glob
import xarray as xr
from pathlib import Path
from distributed import Client, LocalCluster, wait
import dask
import act
import gc
import matplotlib as mpl
import dask.bag as db

def compute_number_of_points(extent, resolution):
    """
    Create a helper function to determine number of points
    """
    return int((extent[1] - extent[0])/resolution) + 1

def grid_radar(file,
               x_grid_limits=(-20_000.,20_000.),
               y_grid_limits=(-20_000.,20_000.),
               z_grid_limits = (500.,5_000.),
               grid_resolution = 250,

               ):
    """
    Grid the radar using some provided parameters
    """

    try:
        radar = pyart.io.read(file)
    except KeyError:
        print('Issue with reading latitude for ', file)


    x_grid_points = compute_number_of_points(x_grid_limits, grid_resolution)
    y_grid_points = compute_number_of_points(y_grid_limits, grid_resolution)
    z_grid_points = compute_number_of_points(z_grid_limits, grid_resolution)

    grid = pyart.map.grid_from_radars(radar,
                                      grid_shape=(z_grid_points,
                                                  y_grid_points,
                                                  x_grid_points),
                                      grid_limits=(z_grid_limits,
                                                   y_grid_limits,
                                                   x_grid_limits),
                                      method='nearest',
                                      constant_roi=250,
                                     )
    del radar
    return grid.to_xarray()


def subset_lowest_vertical_level(ds, additional_fields=["corrected_reflectivity"]):
    """
    Filter the dataset based on the lowest vertical level
    """
    snow_fields = [var for var in list(ds.variables) if "snow" in var] + additional_fields

    # Create a new 4-d height field
    ds["lowest_height"] = (ds.z * (ds[snow_fields[0]]/ds[snow_fields[0]])).fillna(5_000)

    # Find the minimum height index
    min_index = ds.lowest_height.argmin(dim='z',
                                        skipna=True)

    # Subset our snow fields based on this new index and the lowest height + reflectivity
    subset_ds = ds[snow_fields + ['lowest_height', 'DBZ', 'rain_rate_A', 'corrected_reflectivity', 'gate_id']].isel(z=min_index)

    ds["lowest_height"] = ds.lowest_height.where(ds.lowest_height < 10_000, other=np.nan)
    ds.close()
    del ds

    return subset_ds


def setup_output_dataset(ds, datastream='xprecipradarsquire.c1', preload_dod=True):
    dims = {'time':1, 'x':ds.y.shape[0], 'y':ds.x.shape[0]}
    if not preload_dod:
        out_ds = act.io.create_obj_from_arm_dod(datastream, set_dims=dims, version='1.0')
        out_ds.to_netcdf('xprecipradarsquire.c1.dod.nc')
    else:
        out_ds = xr.open_dataset('xprecipradarsquire.c1.dod.nc')
    # Create an empty dataset to write to
    new_ds = xr.Dataset()
    for variable in out_ds.variables:
        new_ds[variable] = ds[variable]
        new_ds[variable].attrs = out_ds[variable].attrs

    # Make sure the attributes match
    new_ds.attrs = out_ds.attrs
    ds.close()
    out_ds.close()
    del out_ds
    del ds
    return new_ds

def run_squire(file):
    """
    Runs the SQUIRE workflow
    """

    # Read the file, and grid to a cartesian grid
    try:
        ds = grid_radar(file)

        # Subset the lowest vertical level
        out_ds = subset_lowest_vertical_level(ds)

        # Make sure the dataset is compliant to metadata standards
        compliant_ds = setup_output_dataset(out_ds)

        # Create an output path
        out_path = f"data/{Path(file).stem}.gridded.nc"

        # Write the dataset to a netcdf file in the specified directory
        compliant_ds.to_netcdf(out_path)

        # Close and delete everything
        compliant_ds.close()

        del ds
        del out_ds
        del compliant_ds
        gc.collect()

    except:
        pass

if __name__ == "__main__":
    cluster = LocalCluster(n_workers=20, processes=True, threads_per_worker=1)
    files = sorted(glob.glob("/gpfs/wolf/atm124/proj-shared/gucxprecipradarcmacS2.c1/ppi/202203/gucxprecipradarcmacS2.c1.202203*"))
    with Client(cluster) as c:
        print(c)
        for interval in np.arange(0, len(files), 100):
            file_bag = db.from_sequence(files[interval-100:interval], 5)
            computation = file_bag.map(run_squire)
            computation.compute()
            print("done with ", interval)
