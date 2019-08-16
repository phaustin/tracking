import h5py
import glob
import re
import pandas as pd
import dask.dataframe
from joblib import Parallel
import time
import numpy as np

#cloud types
cloud_type_id={"condensed": 0,
         "condensed_edge": 1,
         "condensed_env": 2,
         "condensed_shell": 3,
         "core": 4,
         "core_edge": 5,
         "core_env": 6,
         "core_shell": 7,
         "plume": 8}

cluster_type_id={'condensed':0,
                 'core':1,
                 'merge_connections':2,
                 'past_connections':3,
                 'plume':4,
                 'split_connections':5}

def write_frame(h5file,cloud_id,int_time,start,cloud_type='core'):
    cloud_num=int(cloud_id)
    the_coords=h5file[cloud_id][cloud_type][...]
    nrows, = the_coords.shape
    id_col=np.repeat(cloud_num,nrows).astype(np.int32)
    cloud_type=np.repeat(cluster_type_id[cloud_type],nrows).astype(np.uint8)
    time_step=np.repeat(int_time,nrows).astype(np.int32)
    stop=start + nrows
    if nrows > 1.e6:
        print(f'pulling {cloud_id} {cloud_type} length {nrows} {start}-{stop}')
    index=np.arange(start,stop,dtype=np.int64)
    the_frame=pd.DataFrame(index,index=index,columns=['index'])
    the_frame.set_index('index',drop=False)
    the_frame['cloud_id']=pd.Series(id_col,index=index)
    the_frame['type']=pd.Series(cloud_type,index=index)
    the_frame['time_step']=pd.Series(time_step,index=index)
    the_frame['coord']=pd.Series(the_coords,index=index)
    return stop,the_frame

def convert_file(time_start,case_dir,the_file):
    the_time=timestep.match(the_file).group(1)
    filename=f'{case_dir}/clusters_{the_time}.parq'
    int_time=int(the_time)
    pandas_row_max=int(1.5e6)
    chunksize=int(50.e6)
    pandas_append=False
    accum_frame=None
    dask_append=False
    start=0
    print_interval=500
    last_print = 0
    with h5py.File(the_file) as h5file:
        cloud_ids = list(h5file.keys())
        tot_ids=len(cloud_ids)
        print(f'processing {len(cloud_ids)} clusters')
        cloud_ids=sorted(cloud_ids)
        df_recs=0
        for count,cloud_id in enumerate(cloud_ids):
            for cloud_type in ['condensed','core']:
                stop,df_frame=write_frame(h5file,cloud_id,int_time,start,cloud_type=cloud_type)
                nrecs=stop - start
                if nrecs == 0:
                    continue
                if (count - last_print) > print_interval:
                    print(f'time step {int_time}: cloud id: {cloud_id}, read: {nrecs}, {count}/{tot_ids}')
                    last_print = count + print_interval
                df_recs += nrecs
                start=stop
                if pandas_append:
                    accum_frame=pd.concat([accum_frame,df_frame])
                else:
                    accum_frame=pd.DataFrame.copy(df_frame)
                    pandas_append = True
                if df_recs > pandas_row_max:
                    print('writing to dask')
                    print(accum_frame.head())
                    new_dask_frame=dask.dataframe.from_pandas(accum_frame,chunksize=chunksize)
                    dask.dataframe.to_parquet(filename,new_dask_frame,compression='SNAPPY',
                                              write_index=True,append=dask_append)
                    print('finished writing to dask')
                    pandas_append=False
                    accum_frame=None
                    df_recs=0
                    if not dask_append:
                        dask_append=True
        print('final write to dask')
        new_dask_frame=dask.dataframe.from_pandas(accum_frame,chunksize=chunksize)
        dask.dataframe.to_parquet(filename,new_dask_frame,compression='SNAPPY',
                                          write_index=True,append=dask_append)

        

if __name__ == "__main__":
    timestep=re.compile('.*_(\d+)\.h5')
    case_dir='/Phil8TB/PhilShare/phil/gate_clusters/hdf5'
    in_pat=f'{case_dir}/*h5'
    the_files=sorted(glob.glob(in_pat))
    for count,item in enumerate(the_files):
        print(count,item)
    time_start=time.perf_counter()
    fun_list=[(convert_file,(time_start,case_dir,the_file),{}) for the_file in the_files[37:]]
    #fun_list=fun_list[:1]
    print(f'jobs: {fun_list}')
    Parallel(n_jobs=15,backend='multiprocessing')(fun_list)

