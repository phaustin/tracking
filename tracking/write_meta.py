"""
python -m tracking.single_zarr wrfout_d03_2016-01-04_09:00:00_post.nc
"""
import zarr
from netCDF4 import Dataset
from . import read_meta
from .read_meta import dump_meta
from pathlib import Path
from zarr import blosc
import numpy as np
import json

if __name__ == "__main__":
    import argparse
    linebreaks=argparse.RawTextHelpFormatter
    descrip=__doc__.lstrip()
    parser = argparse.ArgumentParser(formatter_class=linebreaks,description=descrip)
    parser.add_argument('nc_file',type=str,help='input ncfile name')
    parser.add_argument('varname',type=str,help='variable to extract')
    args=parser.parse_args()
    nc_path=Path(args.nc_file).resolve()
    zarr_dir=Path('.').resolve()
    new_name=f'{nc_path.stem}_{args.varname}'
    zarr_dir= zarr_dir / Path(new_name)
    zarr_dir=str(zarr_dir.with_suffix('.zarr'))
    store = zarr.DirectoryStore(zarr_dir)
    the_group=zarr.hierarchy.open_group(store=store, mode='a',
                                        synchronizer=None, path=None)
    meta_path=Path(read_meta.__file__)
    with open (meta_path.parent / Path('sam_metadata.json')) as json_file:
        sam_meta_dict=json.load(json_file)['variables']
    array_names=[item[0] for item in the_group.arrays()]
    for var_name in array_names:
        print(f'{var_name}:\n{sam_meta_dict[var_name]}')
        for key,value in sam_meta_dict[var_name].items():
            print(f'{var_name} attribute:  {key}={value}')
            the_group[var_name].attrs[key]=value

