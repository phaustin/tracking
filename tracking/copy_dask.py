'''
   Generate a df for core coordinates, an ordered df containing cluster sizes and a pickle file containing the 30 largest clusters from GATE timesteps
   example: python GATE_get_coords.py -s 10 -e 40 -t 24
'''
from dask.dataframe import read_parquet
import numpy as np
from pathlib import Path
import glob
from multiprocessing.pool import ThreadPool
import dask
import sys
import argparse
import pickle


def sort_parq(filename):
    """
    sort key returns 241 with filename
         clusters_00000030_241.parq
    """
    the_name=Path(filename).name
    parts=the_name.split('_')
    number=parts[-1].split('.')
    out=int(number[0])
    return out


def make_ddf(thread_num, tstep, number):
    print(f'copying tstep {tstep}')
    pool = ThreadPool(processes=thread_num)
    in_dir='/nodessd/phil/clusters'
    out_dir='/Phil8TB/PhilShare/phil/gate_clusters'
    in_dir=out_dir
    file_list=list(glob.glob(f'{in_dir}/*00{tstep}_*.parq'))
    file_list.sort(key=sort_parq)
    with dask.set_options(pool=pool):
        da_frame=read_parquet(file_list[0],index='index')
        for new_file in file_list[1:]:
                new_frame=read_parquet(new_file,index='index')
                da_frame=dask.dataframe.multi.concat([da_frame,new_frame],interleave_partitions=False)
    parts=file_list[0].split('_')
    newname=f'clusters_{parts[-2]}.parq'
    out_name=Path(out_dir) / Path(newname)
    print(newname)
    with dask.set_options(pool=pool):
        dask.dataframe.to_parquet(str(out_name),da_frame,compression='SNAPPY',write_index=True)
    return None


def make_parser():
    '''
    command line arguments for calling program
    '''
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('-s', '--start', dest='start_ts', type=int, help='starting timestep in GATE to be processed', required=True)
    parser.add_argument('-e', '--end', dest='end_ts', type=int, help='last timestep in GATE to be processed (-1 with python indexing)', required=True)
    parser.add_argument('-t', '--threads', dest='threads', type=int, help='number of threads to parallel process in dask', required=True)
    return parser


def main(args=None):
    '''
    args are required
    '''
    parser = make_parser()
    args = parser.parse_args(args)
    timesteps = np.arange(args.start_ts, args.end_ts)
    for the_ts in timesteps:
        make_ddf(args.threads, the_ts, -1)


if __name__ == "__main__":
    sys.exit(main())
