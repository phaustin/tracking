#/usr/bin/env python
"""
example usage: python extract_var_GATE.py GATE_1920x1920x512_50m_1s_ent_comp_0000033000.nc 
               -v W -m /nodessd/phil/clusters/andrew/updraft_json/180301_x755to822_y520to593.json
"""
import zarr
import json
import numpy as np
from netCDF4 import Dataset
import argparse
import sys
from pathlib import Path
from tracking.chmod import chmod
import os
import glob


def convert_file(nc_file,zarr_dir,varnamelist,meta_file=None):
    """
    extracts var_name from nc_file and writes
    to zarr_dir
    """
    if meta_file:
        with open(meta_file, 'r') as f:
            sub_dict = json.load(f)
            y_section = np.arange(np.amin(sub_dict['y']), np.amax(sub_dict['y'])+1)
            x_section = np.arange(np.amin(sub_dict['x']), np.amax(sub_dict['x'])+1)
    else:
        y_section=slice(0,None)
        x_section=slice(0,None)
    with Dataset(nc_file) as ncin:
        store = zarr.DirectoryStore(zarr_dir)
        the_group=zarr.hierarchy.open_group(store=store, mode='w', synchronizer=None, path=None)
        print(f'writing to {zarr_dir}')
        for varname in varnamelist:
            print(f'processing variable {varname}')
            if varname == 'x':
                the_var=ncin.variables[varname][x_section]
            elif varname == 'y':
                the_var=ncin.variables[varname][y_section]
            elif varname == 'z':
                the_var=ncin.variables[varname][:]
            elif varname == 'p':
                the_var=ncin.variables[varname][:]
            else:
                the_var=ncin.variables[varname][:, y_section, x_section]
            if the_var.shape==4:
                the_var=the_var.squeeze()
            print(f'writing {varname}: {the_var}')
            the_group.array(varname,the_var,shape=the_var.shape,dtype=the_var.dtype,
                        compressor=zarr.Blosc(cname='zlib', clevel=5),chunks=None)
    chmod(zarr_dir)


def make_parser():
    linebreaks=argparse.RawTextHelpFormatter
    descrip=__doc__.lstrip()
    parser = argparse.ArgumentParser(formatter_class=linebreaks,description=descrip)
    parser.add_argument('nc_file',type=str, help='input GATE ncfile name or directory containing GATE ncfiles')
    parser.add_argument('zarr_file',type=str, help='path to putput zarr file')
    parser.add_argument('-s','--start',type=int, help='start timestep (if directory)')
    parser.add_argument('-e','--end',type=int, help='end timestep (if directory)')
    parser.add_argument('-v','--vars', nargs='+', help='list of variables to extract', required=True)
    parser.add_argument('-m','--meta_dict',type=str,help='path to metadata file', required=False)
    return parser


def main(args=None):
    '''
    args are required
    '''
    parser = make_parser()
    args=parser.parse_args()
    print(args.vars)
    if os.path.isdir(args.nc_file):
        nc_filelist = sorted(glob.glob(f'{args.nc_file}/*.nc'))
        for the_file in nc_filelist[args.start:args.end]:
            nc_path=Path(the_file)
            nc_split = the_file.split('/')
            if nc_split[-2] == 'core_entrain':
                newdir = 'core_entrain_zarr'
            else:
                newdir = 'variable_zarr'
            fullpath = f'{the_file}'
            varnames='_'.join(args.vars)
            if args.meta_dict:
                meta_split = args.meta_dict.split('/')
                meta_end = meta_split[-1].split('.')
                meta_name = meta_end[0]
            else:
                meta_name='full'
            new_file=Path(f'/Phil8TB/PhilShare/phil/{newdir}/{nc_path.stem}_{varnames}_{meta_name}.zarr')
            zarr_dir=(nc_path.parent / new_file).resolve()
            convert_file(fullpath,zarr_dir,args.vars,args.meta_dict)
    else:
        the_file = args.nc_file
        nc_path=Path(the_file)
        nc_split = the_file.split('_')
        if nc_split[1] == 'CORE':
            fullpath = f'/newtera/loh/data/GATE/core_entrain/{the_file}'
        else:
            fullpath = f'./{the_file}'
        varnames='_'.join(args.vars)
        if args.meta_dict:
            meta_split = args.meta_dict.split('/')
            meta_end = meta_split[-1].split('.')
            meta_name = meta_end[0]
        else:
            meta_name='full'
        zarr_dir=args.zarr_file
        convert_file(args.nc_file,args.zarr_file,args.vars,args.meta_dict)
 

if __name__ == "__main__":
    sys.exit(main())
