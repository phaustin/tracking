import zarr
from netCDF4 import Dataset
import glob
import re
#from numcodecs import Blosc

timestep=re.compile('.*_(\d+)\.nc')
nc_files=glob.glob('variables/*nc')
nc_files=sorted(nc_files)
#print(nc_files)

for the_file in nc_files[:1]:
    the_time=timestep.match(the_file).group(1)
    out_name=f'bomex_{the_time}_zarr'
    print(out_name)
    with Dataset(the_file) as ncin:
        store = zarr.DirectoryStore(out_name)
        the_group=zarr.hierarchy.open_group(store=store, mode='a', synchronizer=None, path=None)
        for varname in ncin.variables.keys():
            the_var=ncin.variables[varname][...]
            if the_var.shape==4:
                the_var=the_var.squeeze()
            the_group.array(varname,the_var,shape=the_var.shape,dtype=the_var.dtype,compressor=zarr.Blosc(cname='snappy', clevel=3),chunks=None)

