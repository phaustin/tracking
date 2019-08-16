"""
 * example code that converts hdf5 tracking into parquet
"""
import time

from tracking.write_job import convert_file, cloudlet_type_id, cluster_type_id,\
                  cloud_type_id

import glob
case_dir='/tera/loh/cloudtracker/cloudtracker/hdf5'
the_file='/tera/loh/cloudtracker/cloudtracker/hdf5/clusters_00000060.h5'
the_file=glob.glob('./clusters_00000020.h5')[0]
#from tracking.write_job import make_out_file
#print(make_out_file(the_file,10))
# time_start=time.perf_counter()
convert_file(cluster_type_id,the_file)

# with h5py.File(the_file) as h5file:
#     keys=list(h5file.keys())
#     print(len(keys))
    
