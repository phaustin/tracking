import glob, json, dask
import zarr
import numpy as np
from numba import jit
import pandas as pd
import dask.dataframe as dd
import pyarrow.parquet as pq
import pprint
from collections import defaultdict
import itertools
import sys
import matplotlib.pyplot as plt
import math
import multiprocessing as mp
from functools import partial

#------------------------------------------------------------------------------

dx = dz = 50

time = []
for t in range(180+1):
        time += ['00000%i'%(t*60+32400)]

path = '/Phil8TB/PhilShare/andrew'
plume_regions = ['180301_x437to563_y469to607','0_x1151to1257_y1404to1501','180301_x616to765_y517to661','0_x1124to1360_y236to446',
                                        '0_x662to998_y666to992','0_x515to721_y837to1090','0_x1350to1568_y1172to1420']
plume_lifetime_start = [20,0,0,20,20,35,84]
plume_lifetime_end = [39,30,30,59,80,100,160]

#MFTETCOR (for 0_x662to998), ETETCOR, DTETCOR, EQTETCOR, DQTETCOR ; U, V, W, QN, TABS

cloud_id = 5
cloud_time = np.arange(plume_lifetime_start[cloud_id],plume_lifetime_end[cloud_id]+1)

zarr_var = []
zarr_ent = []
zarr_mf = []

for i in cloud_time:
        zarr_ent += [path+'/core_entrain_zarr/GATE_CORE_ENTRAIN_1920x1920x512_50m_1s_ent_comp_'+time[i]+'_ETETCOR_DTETCOR_EQTETCOR_DQTETCOR_x_y_z_'+plume_regions[cloud_id]+'.zarr']
        zarr_var += [path+'/variable_zarr/GATE_1920x1920x512_50m_1s_ent_comp_'+time[i]+'_U_V_W_QN_TABS_x_y_'+plume_regions[cloud_id]+'.zarr']
        zarr_mf += [path+'/core_entrain_zarr/GATE_CORE_ENTRAIN_1920x1920x512_50m_1s_ent_comp_'+time[i]+'_MFTETCOR_'+plume_regions[cloud_id]+'.zarr']

zarr_ent = np.asarray(zarr_ent)
zarr_var = np.asarray(zarr_var)
zarr_mf = np.asarray(zarr_mf)


def calc_prop(var):
        if var == 'entrainment':
                entrainment = []
                for t in cloud_time:
                        zarrfile_in_var = zarr.open_group(zarr_var[t],'r')
                        zarrfile_in_ent = zarr.open_group(zarr_ent[t],'r')
                        ent = []
                        for z in range(320):
                                ent_z = []
                                w = zarrfile_in_var['W'][z,:,:]
                                x = zarrfile_in_var['x'][:]
                                y = zarrfile_in_var['y'][:]
                                for i in range(x.shape[0]):
                                        for j in range(y.shape[0]):
                                                if w[j,i] >= 1:
                                                        ent_z += [zarrfile_in_ent['ETETCOR'][z,j,i]]
                                ent += [np.asarray(ent_z).mean()]
                        entrainment.append(ent)
                entrainment = np.asarray(entrainment)
                return entrainment
        elif var == 'detrainment': 
                detrainment = []
                for t in cloud_time:
                        zarrfile_in_var = zarr.open_group(zarr_var[t],'r')
                        zarrfile_in_det = zarr.open_group(zarr_ent[t],'r')
                        det = []
                        for z in range(320):
                                det_z = []
                                w = zarrfile_in_var['W'][z,:,:]
                                x = zarrfile_in_var['x'][:]
                                y = zarrfile_in_var['y'][:]
                                for i in range(x.shape[0]):
                                        for j in range(y.shape[0]):
                                                if w[j,i] >= 1:
                                                        det_z += [zarrfile_in_det['DTETCOR'][z,j,i]]
                                det += [np.asarray(det_z).mean()]
                        detrainment.append(det)
                detrainment = np.asarray(detrainment)
                return detrainment
        elif var == 'massflux': 
                massflux = []
                for t in cloud_time:
                        zarrfile_in_var = zarr.open_group(zarr_var[t],'r')
                        zarrfile_in_mf = zarr.open_group(zarr_mf[t],'r')
                        mf = []
                        for z in range(320):
                                mf_z = []
                                w = zarrfile_in_var['W'][z,:,:]
                                x = zarrfile_in_var['x'][:]
                                y = zarrfile_in_var['y'][:]
                                for i in range(x.shape[0]):
                                        for j in range(y.shape[0]):
                                                if w[j,i] >= 1:
                                                        mf_z += [zarrfile_in_mf['MFTETCOR'][z,j,i]]
                                mf += [np.asarray(mf_z).mean()]
                        massflux.append(mf)
                massflux = np.asarray(massflux)
                return massflux
        else: 
                radius = []
                vertical_velocity = []
                for t in cloud_time:
                        zarrfile_in_var = zarr.open_group(zarr_var[t],'r')
                        r = []
                        vvel = []
                        for z in range(320):
                                w_z = [] 
                                w = zarrfile_in_var['W'][z,:,:]
                                x = zarrfile_in_var['x'][:]
                                y = zarrfile_in_var['y'][:]
                                for i in range(x.shape[0]):
                                        for j in range(y.shape[0]):
                                                if w[j,i] >= 1:
                                                        w_z += [w[j,i]]
                                r_z = math.sqrt((len(w_z)*dx*dx))
                                r += [r_z]
                                vvel += [np.asarray(w_z).mean()]
                        radius.append(r)
                        vertical_velocity.append(vvel)
                radius = np.asarray(radius)
                vertical_velocity = np.asarray(vertical_velocity)
                return radius, vertical_velocity


if __name__ == "__main__":
        variables = ['entrainment', 'detrainment',' massflux', 'radius']
        pool = mp.Pool(mp.cpu_count())
        results = pool.map(calc_prop,variables)

                
                




        

