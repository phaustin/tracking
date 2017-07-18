"""
python -m tracking.read_meta wrfout_d03_2016-01-04_09:00:00.nc test.json
"""

from netCDF4 import Dataset
from collections import defaultdict, OrderedDict
import json
import numpy as np

def check_type(item):
    if isinstance(item,str):
        item=item.strip()
    elif isinstance(item,np.number):
        item=float(item)
    else:
        raise ValueError(f'{type(item)} is unexpected for attribute')
    return item

def dump_meta(nc_filename):
    """
    Dump all metadata from a flat wrf netcdf file

    Parameters
    ----------

    nc_filename: str
        name of input netcdf file

    Returns
    -------

    file_dict: defaultdict
       nested dictionary with attributes
    """
    file_dict = defaultdict(lambda: defaultdict(OrderedDict))
    file_dict['global']['filename']=nc_filename
    with Dataset(nc_filename,'r') as infile:
        for key,value in infile.dimensions.items():
            dimname=f'dim_{value.name}'
            file_dict['dimension_sizes'][dimname]=value.size
        global_attrs=list(infile.__dict__.items())
        if len(global_attrs) > 0:
            for (name,value) in global_attrs:
               file_dict['global'][name]=check_type(value)
        for (inname,invalue) in infile.variables.items():
            for (attname,attvalue) in invalue.__dict__.items():
                file_dict[inname][attname]=check_type(attvalue)
            file_dict[inname]['shape']=invalue.shape
            dimnames=[f'dim_{item}' for item in invalue.dimensions]
            file_dict[inname]['dimensions']=dimnames
    return file_dict

if __name__ == "__main__":
    import argparse
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(formatter_class=linebreaks,
                                     description=descrip)
    parser.add_argument('nc_filename', type=str, help='input netcdf file')
    parser.add_argument('out_json', type=str, help='output json file with metadata')
    args=parser.parse_args()
    meta_dict=dump_meta(args.nc_filename)
    with open(args.out_json,'w') as out:
        json.dump(meta_dict,out,indent=4)
