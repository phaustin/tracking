import h5py
import glob
import re
import pandas as pd
from dask.dataframe import read_parquet
from joblib import Parallel
import time

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


def convert_file(time_start,case_dir,the_file):
    the_time=timestep.match(the_file).group(1)
    int_time=int(the_time)
    with h5py.File(the_file) as h5file:
        cloud_ids = list(h5file.keys())
        cloud_ids=sorted(cloud_ids)
        first_cloud=cloud_ids[0]
        cloud_num=int(cloud_id)
        core_coords=h5file[first_cloud]['core'][...]
#         id_column=
#         for cloud_id in h5file.keys():
#             #add condensed ids
#             rec_count=0
#             print(f'full: {the_time}:{cloud_id}',flush=True)
#             cloud_num=int(cloud_id)
#             if cloud_num == -1:
#                 continue
#             for the_type in h5file[cloud_id]:
#                 type_num=type_id[the_type]
#                 for coord in h5file[cloud_id][the_type]:
#                     keep_recs.append((cloud_num,type_num,int_time,coord))
#                     if rec_count % window == 0:
#                         print(f'time/id/rec: {int_time}:{cloud_num}:{rec_count}',flush=True)
#                     rec_count+=1
#         pq_name=f'{case_dir}/clouds_{the_time}.pq'
#         df=pd.DataFrame.from_records(keep_recs,columns=['cloud_id','type','time_step','coord'])
#         table = pa.Table.from_pandas(df)
#         elapsed=(time.perf_counter() - time_start)/60.
#         print(f'wrote {pq_name}, elapsed: {elapsed}')
#         pq.write_table(table, pq_name,compression='snappy')


timestep=re.compile('.*_(\d+)\.h5')
case_dir='/Phil8TB/PhilShare/phil/gate_clusters/hdf5'
in_pat=f'{case_dir}/*h5'
the_files=sorted(glob.glob(in_pat))
time_start=time.perf_counter()
fun_list=[(convert_file,(time_start,case_dir,the_file),{}) for the_file in the_files]
fun_list=fun_list[:1]
print(f'jobs: {fun_list}')
Parallel(n_jobs=len(fun_list),backend='multiprocessing')(fun_list)

