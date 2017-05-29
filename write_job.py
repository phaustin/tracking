import h5py
import glob
import re
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from joblib import Parallel

type_id={"condensed": 0,
         "condensed_edge": 1,
         "condensed_env": 2,
         "condensed_shell": 3,
         "core": 4,
         "core_edge": 5,
         "core_env": 6,
         "core_shell": 7,
         "plume": 8}

def convert_file(the_file):
    the_time=timestep.match(the_file).group(1)
    int_time=int(the_time)
    with h5py.File(the_file) as h5file:
        keep_recs=[]
        for cloud_id in h5file.keys():
            print(f'full: {the_time}:{cloud_id}')
            cloud_num=int(cloud_id)
            if cloud_num == -1:
                continue
            for the_type in h5file[cloud_id]:
                type_num=type_id[the_type]
                for coord in h5file[cloud_id][the_type]:
                    keep_recs.append((cloud_num,type_num,int_time,coord))
        pq_name=f'clouds_{the_time}.pq'
        df=pd.DataFrame.from_records(keep_recs,columns=['cloud_id','type','time_step','coord'])
        table = pa.Table.from_pandas(df)
        print(f'{pq_name}')
        pq.write_table(table, pq_name,compression='snappy')

timestep=re.compile('.*_(\d+)\.h5')
the_files=sorted(glob.glob('*h5'))[0:2]
fun_list=[(convert_file,(the_file,),{}) for the_file in the_files]
Parallel(n_jobs=4)(fun_list)

