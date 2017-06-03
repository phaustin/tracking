import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pdir
import glob
import re
import ujson
import cProfile
import cProfile


def read_table(the_file):
    parquet_file=pq.ParquetFile(the_file)    
    the_table=parquet_file.read_row_group(0)
    return the_table

def write_table(the_table):
    outfile='test.pq'
    pq.write_table(the_table,outfile,compression='snappy')
    
def read_write(the_file):
    the_table=read_table(the_file)
    write_table(the_table)

cProfile.run('read_write("clouds_00000149.pq")',filename='prof_parquet.txt')            
