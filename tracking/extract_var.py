#/usr/bin/env python
"""
python -m tracking.single_zarr wrfout_d03_2016-01-04_09:00:00_post.nc
"""
import zarr
from netCDF4 import Dataset
from .read_meta import dump_meta
from pathlib import Path
from .chmod import chmod

def convert_file(nc_file,zarr_dir,varnamelist):
    """
    extracts var_name from nc_file and writes
    to zarr_dir
    """
    with Dataset(nc_file) as ncin:
        store = zarr.DirectoryStore(zarr_dir)
        the_group=zarr.hierarchy.open_group(store=store, mode='w', synchronizer=None, path=None)
        print(f'writing to {zarr_dir}')
        for varname in varnamelist:
            print(f'processing variable {varname}')
            the_var=ncin.variables[varname][...]
            if the_var.shape==4:
                the_var=the_var.squeeze()
            the_group.array(varname,the_var,shape=the_var.shape,dtype=the_var.dtype,
                        compressor=zarr.Blosc(cname='zlib', clevel=5),chunks=None)
    chmod(zarr_dir)

if __name__ == "__main__":
    import argparse
    linebreaks=argparse.RawTextHelpFormatter
    descrip=__doc__.lstrip()
    parser = argparse.ArgumentParser(formatter_class=linebreaks,description=descrip)
    parser.add_argument('nc_file',type=str,help='input ncfile name')
    parser.add_argument('-v','--vars', nargs='+', help='list of variables to extract', required=True)
    parser.add_argument('-m','--meta_dict',nargs=1,type=str,help='name of optional metadata file')
    args=parser.parse_args()
    print(args.vars)
    nc_path=Path(args.nc_file)
    varnames='_'.join(args.vars)
    new_file=Path(f'{nc_path.stem}_{varnames}.zarr')
    zarr_dir=(nc_path.parent / new_file).resolve()
    convert_file(args.nc_file,zarr_dir,args.vars)
    
    # meta_dict=dump_meta(args.nc_file)
    # store = zarr.DirectoryStore(zarr_dir)
    # the_group=zarr.hierarchy.open_group(store=store, mode='a',
    #                                     synchronizer=None, path=None)
    # for key,value in meta_dict['global'].items():
    #     the_group.attrs[key]=value
    # del meta_dict['global']
    # for key,value in meta_dict['dimension_sizes'].items():
    #     dim_name=f'dim_{key}'
    #     the_group.attrs[dim_name]=value
    # del meta_dict['dimension_sizes']
    # for var,var_dict in meta_dict.items():
    #     for key, value in var_dict.items():
    #         the_group[var].attrs[key]=value
    #         print(var,list(the_group[var].attrs.keys()))
    
    
    
    
    
    
