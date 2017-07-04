"""
single zarr file conversion
"""
import zarr
from netCDF4 import Dataset
import time

def convert_file(nc_file,zarr_dir):
    with Dataset(nc_file) as ncin:
        store = zarr.DirectoryStore(zarr_dir)
        the_group=zarr.hierarchy.open_group(store=store, mode='a', synchronizer=None, path=None)
        for varname in ncin.variables.keys():
            print(f'variable: {varname}')
            the_var=ncin.variables[varname][...]
            if the_var.shape==4:
                the_var=the_var.squeeze()
            the_group.array(varname,the_var,shape=the_var.shape,dtype=the_var.dtype,
                            compressor=zarr.Blosc(cname='snappy', clevel=3),chunks=None)

if __name__ == "__main__":
    import argparse
    linebreaks=argparse.RawTextHelpFormatter
    descrip=__doc__.lstrip()
    parser = argparse.ArgumentParser(formatter_class=linebreaks,description=descrip)
    parser.add_argument('nc_file',type=str,help='input ncfile name')
    parser.add_argument('zarr_dir',type=str,help='output zarr directory')
    args=parser.parse_args()
    convert_file(args.nc_file,args.zarr_dir)
