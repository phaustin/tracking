"""
example of how to copy a zarr file to netcdf
"""
from netCDF4 import Dataset
from pathlib import Path
import zarr

def to_nc(zarr_path,nc_path):
    store = zarr.DirectoryStore(str(zarr_path))
    the_group=zarr.hierarchy.open_group(store=store, mode='r', synchronizer=None, path=None)
    dimension_list=[]
    dim_dict={}
    #
    # find all the unique dimensions and name them "dim_{size}"
    #
    for item in the_group:
        for a_dim in the_group[item].shape:
            dimension_list.append(a_dim)
    unique_dims=set(dimension_list)
    for a_dim in unique_dims:
        dim_dict[a_dim]=f'dim_{a_dim}'
    with Dataset(str(nc_path),'w') as nc_out:
        #
        # copy all global attributes
        #
        global_attrs=the_group.attrs
        for key,value in global_attrs.items():
            setattr(nc_out,key,value)
        #
        # create all dimensions in dim_dict
        #
        for a_dim,dim_name in dim_dict.items():
            nc_out.createDimension(dim_name,a_dim)
        #
        # copy all variables and their attributes, if any
        #
        for var_name in the_group:
            print(f'copying variable {var_name}')
            var_array=the_group[var_name][...]
            dim_list=[]
            for a_dim in var_array.shape:
                dim_list.append(dim_dict[a_dim])
            the_var=nc_out.createVariable(var_name,var_array.dtype,dimensions=dim_list,zlib=True)
            the_var[...]=var_array[...]
            var_attrs=the_group[var_name].attrs
            for key,value in var_attrs:
                setattr(the_var,key,value)
    
if __name__ == "__main__":
    import argparse
    linebreaks=argparse.RawTextHelpFormatter
    descrip=__doc__.lstrip()
    parser = argparse.ArgumentParser(formatter_class=linebreaks,description=descrip)
    parser.add_argument('zarr_file',type=str,help='input zarr folder')
    args=parser.parse_args()
    zarr_path=Path(args.zarr_file)
    nc_path = zarr_path.with_suffix('.nc')
    to_nc(zarr_path,nc_path)
