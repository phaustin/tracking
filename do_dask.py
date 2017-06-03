from dask.distributed import Client, progress
import glob
import dask.array as da
import zarr

#
# with the multiprocessing threadpool
#
from multiprocessing.pool import ThreadPool
pool = ThreadPool(10)
da.set_options(pool=pool)  # set global threadpool
#or
#with set_options(pool=pool)  # use threadpool throughout with block




# client = Client('tcp://142.103.36.115:8786')
# print(client)
#
# 
#
print(pool)
zarr_files=sorted(glob.glob("*_zarr"))
pq_files=sorted(glob.glob("*.pq"))



qn_list=[]
for zfile in zarr_files:
    root = zarr.open_group(zfile, mode='r')
    qn_list.append(root['QN'])

data = [da.from_array(the_qn,chunks=the_qn.chunks) for the_qn in qn_list]
x = da.concatenate(data, axis=0)
print(x[20,5,5,5].compute())
y=x**2.
#print('print: check this: ',list(y.dask.keys()))





