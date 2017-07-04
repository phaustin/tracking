"""
python -m tracking.read_meta wrfout_d03_2016-01-04_09:00:00.nc test.json
"""

from netCDF4 import Dataset
from collections import defaultdict
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

def dump_meta(nc_file):
    file_dict = defaultdict(lambda: defaultdict(dict))
    file_dict['global']['filename']=nc_file
    with Dataset(nc_file,'r') as infile:
        global_attrs=list(infile.__dict__.items())
        if len(global_attrs) > 0:
            for (name,value) in global_attrs:
               print("copying global attribute: ",name)
               file_dict['global'][name]=check_type(value)

        for (inname,invalue) in infile.variables.items():
            print("copying variable: ",inname)
            for (attname,attvalue) in invalue.__dict__.items():
                file_dict[inname][attname]=check_type(attvalue)
    return file_dict

if __name__ == "__main__":
    import argparse
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(formatter_class=linebreaks,
                                     description=descrip)
    parser.add_argument('nc_file', type=str, help='input netcdf file')
    parser.add_argument('out_json', type=str, help='output json file with metadata')
    args=parser.parse_args()
    meta_dict=dump_meta(args.nc_file)
    with open(args.out_json,'w') as out:
        json.dump(meta_dict,out,indent=4)
