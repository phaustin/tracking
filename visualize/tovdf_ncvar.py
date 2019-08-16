'''
    convert a GATE netcdf file to a raw binary file for vapor and turn that file into vapor vdf
    example: python tovdf_ncvar.py nc_file tropical -v QN QP TABS U V W PP
'''

from netcdf4 import Dataset
import numpy as np
import argparse
import sys
import subprocess
import pdb

def write_error(the_in):
    namelist = []
    for name, var in the_in.variables.items():
        if len(var.shape) == 4:
            namelist.append(name)
    return namelist


def dump_bin(ncfile, varname, outname):
    '''
       establishes the vdf using vdfcreate and the metadata file,
       then pulls data from nc files to populate the vdf with data using raw2vdf
       
       Parameters
       ----------

       ncfile: string
          name of the netcdf file containing the vield
       
       varname: string
          name of the LES variable to be visualized (e.g. QN, W, TR01)

       outname: string
          root name of the resulting vdf file -- .vdf will be appended

       Returns
       -------

       xvals.txt: txt file
          txt file containing the x coordinates of the vdf

       yvals.txt: txt file
          txt file containing the y coordinates of the vdf

       zvals.txt: txt file
          txt file containing the z coordinates of the vdf

       temp.bin: binary file
          binary file used in establishing the memory map for raw2vdf

       outfilenmae:
          the name of the vdf file
    '''

    meters2km = 1.e-3
    print(ncfile)
    with Dataset(ncfile, mode='r') as the_in:
        xvals=the_in['x'][...]*meters2km
        yvals=the_in['y'][...]*meters2km
        zvals=the_in['z'][...]*meters2km
    filenames = ['xvals.txt', 'yvals.txt', 'zvals.txt']
    arrays = [xvals, yvals, zvals]
    #pdb.set_trace()
    for name, vals in zip(filenames, arrays):
        with open(name, 'w') as outfile:
            [outfile.write('{:6.3f} '.format(item)) for item in vals[:-1]]
            outfile.write('{:6.3f}\n'.format(vals[-1]))
    lenx, leny, lenz = len(xvals), len(yvals), len(zvals)
    num_ts = 30
    the_shape = (num_ts, lenx, leny, lenz)
    string_shape = f'{lenx}x{leny}x{lenz}'
    vdfcreate = "/usr/local/vaporapp/vapor-2.6.0/bin/vdfcreate"
    thecmd = f'{vdfcreate} -xcoords xvals.txt -ycoords yvals.txt -zcoords zvals.txt \
             -gridtype stretched -dimension {string_shape} -vars3d U:V:W:QN:QP:TABS:PP -numts {num_ts} {outname}.vdf'
    status1, output1 = subprocess.getstatusoutput(thecmd)
    print('writing an array of {}(t,x,y,z) shape {}x{}x{}x{}'.format(varname, *the_shape))

    with Dataset(ncfile, mode='r') as the_in:
    try:
        var_data = the_in[varname][:, :, :]
        print(var_data.shape)
        rev_shape = (var_data.shape[::-1])
        string_shape = "{}x{}x{}".format(*rev_shape)
    except KeyError:
        print('variable names are: ', write_error(the_in))
        sys.exit(1)
    tmpname = 'temp.bin'
    fp = np.memmap(tmpname, dtype=np.float32, mode='w+',
                   shape=var_data.shape)
    fp[...] = var_data[...]
    del fp
    raw2vdf = "/usr/local/vaporapp/vapor-2.6.0/bin/raw2vdf"
    thecmd = f'{raw2vdf} -varname {varname} -ts 0 {outname}.vdf {tmpname}'
    print(f'running {thecmd}')
    status2, output2 = subprocess.getstatusoutput(thecmd)
    print(status2, output2)
    return out_name, string_shape


def make_parser():
    '''
    command line arguments for calling program
    '''
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('ncfile',type=str,help='full path to nc file')
    parser.add_argument('vdffile',type=str,help='name of vdf output file')
    parser.add_argument('-v', '--varname', dest='varname', type=str, help='name of 3d variable', required=True)
    return parser


def main(args=None):
    '''
    args are required
    '''
    parser = make_parser()
    args = parser.parse_args(args)
    binfile, rev_shape = dump_bin(args.ncfile, args.varname)


if __name__ == "__main__":
    sys.exit(main())
