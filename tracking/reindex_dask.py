"""
add a continue index
"""
#http://dask.pydata.org/en/latest/shared.html
from dask.dataframe import read_parquet
import numpy as np
import pandas as pd
from pathlib import Path

def sort_parq(filename):
    """
    sort key returns 241 with filename
         clusters_00000030_241.parq
    """
    the_name=Path(filename).name
    parts=the_name.split('_')
    number=parts[-1].split('.')
    return int(number[0])

import glob
from multiprocessing.pool import ThreadPool
import dask

def reindex(file_regex,nprocesses):
    file_list=list(glob.glob(file_regex))
    file_list.sort(key=sort_parq)
    print('found: ',file_list)
    pool = ThreadPool(processes=nprocesses)
    with dask.set_options(pool=pool):
        da_frame=read_parquet(file_list[0])
        print(f'read {file_list[0]}')
        print(f'read first frame: length is {len(da_frame)}')
        print(f'read first frame: divisions {da_frame.divisions}')
        stop=len(da_frame)
        index=np.arange(0,stop)
        index=dask.dataframe.from_pandas(pd.Series(index),npartitions=1)
        da_frame.divisions=(0,stop-1)
        index.divisions=da_frame.divisions
        da_frame['index']=index
        da_frame.set_index('index',inplace=True,drop=False)
        filename = str(Path(file_list[0]).name)
        dask.dataframe.to_parquet(filename,da_frame,compression='SNAPPY',
                                  write_index=True)
        for new_file in file_list[1:]:
            start=stop
            new_frame=read_parquet(new_file)
            nrecs=len(new_frame)
            stop = start + nrecs
            index=np.arange(start,stop)
            print(f'{new_file}: {index[0]},{index[-1]}')
            index=dask.dataframe.from_pandas(pd.Series(index),npartitions=1)
            new_frame.divisions=(start, stop -1)
            index.divisions=new_frame.divisions
            new_frame['index']=index
            new_frame.set_index('index',inplace=True,drop=False)
            print(f'{new_file} index: {new_frame.index}')
            print(f'{new_file}  index column: {new_frame["index"]}') 
            filename = str(Path(new_file).name)
            dask.dataframe.to_parquet(filename,new_frame,compression='SNAPPY',
                                  write_index=True)

if __name__ == "__main__":
    import argparse
    linebreaks=argparse.RawTextHelpFormatter
    descrip=__doc__.lstrip()
    parser = argparse.ArgumentParser(formatter_class=linebreaks,description=descrip)
    parser.add_argument('file_regex',type=str,help='regex that expands to files')
    parser.add_argument('nprocesses',nargs='?',type=int,default=18,help='number of cores to use, default 18')
    args=parser.parse_args()
    print(args.file_regex)
    regex_list=[]
    for i in np.arange(50,63):
        lead_zeros=f'{i:03d}'
        regex_list.append(f'pqfiles/*{lead_zeros}_*.parq')
    for item in regex_list:
        reindex(item,args.nprocesses)
    
