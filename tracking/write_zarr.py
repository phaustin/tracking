import zarr
from netCDF4 import Dataset
import glob
import re
import time
timestep=re.compile('.*_(\d+)\.nc')
from joblib import Parallel

def convert_file(time_start,src_dir,filename):
    the_time=timestep.match(filename).group(1)
    out_name=f'{src_dir}/vars_{the_time}_zarr'
    with Dataset(filename) as ncin:
        store = zarr.DirectoryStore(out_name)
        the_group=zarr.hierarchy.open_group(store=store, mode='a', synchronizer=None, path=None)
        for varname in ncin.variables.keys():
            the_var=ncin.variables[varname][...]
            if the_var.shape==4:
                the_var=the_var.squeeze()
            the_group.array(varname,the_var,shape=the_var.shape,dtype=the_var.dtype,compressor=zarr.Blosc(cname='snappy', clevel=3),chunks=None)
    elapsed=(time.perf_counter() - time_start)/60.
    print(f'{out_name}: {elapsed}')


if __name__ == "__main__":
    import argparse
    linebreaks=argparse.RawTextHelpFormatter
    descrip=__doc__.lstrip()
    parser = argparse.ArgumentParser(formatter_class=linebreaks,description=descrip)
    parser.add_argument('src_dir',nargs='*',type=str,help='dir with nc files')
    args=parser.parse_args()
    src_dir=args.src_dir
    nc_files=glob.glob(f'{src_dir}/*nc')
    nc_files=sorted(nc_files)
    time_start=time.perf_counter()
    fun_list=[(convert_file,(time_start,src_dir,the_file),{}) for the_file in nc_files]
    Parallel(n_jobs=25)(fun_list)






