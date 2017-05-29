import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pdir
import glob
import re
import ujson

timestep=re.compile('.*_(\d+)\.pq')


def index_to_zyx(index, nz, ny, nx):
    z = np.floor_divide(index, (ny * nx))
    xy = np.mod(index, (ny * nx))
    y = np.floor_divide(xy, nx)
    x = np.mod(xy, nx)
    return (z, y, x)

pq_files=sorted(glob.glob('*pq'))

parquet_file=pq.ParquetFile(pq_files[0])
col_names=parquet_file.schema.names
from collections import OrderedDict as od
col_dict=od(zip(col_names,range(len(col_names))))

cum_list=[]
for the_file in pq_files:
    the_time=timestep.match(the_file).group(1)
    int_time=int(the_time)
    print(f'timestep: {the_time}')
    cloud_id=col_dict['cloud_id']
    parquet_file=pq.ParquetFile(the_file)
    the_table=parquet_file.read_row_group(0)
    index_vals=the_table.column(cloud_id).to_pylist()
    unique_ids=sorted(set(index_vals))
    tup_list=[(the_id,int_time) for the_id in unique_ids]
    cum_list.extend(tup_list)

with open('unique_list.json','w') as out:
    ujson.dump(cum_list,out,indent=4)
    
