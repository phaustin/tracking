
# coding: utf-8

# In[17]:


import zarr
import numpy as np
import json
from netCDF4 import Dataset
from tracking.read_meta import dump_meta
import pprint
pp=pprint.PrettyPrinter(indent=4)


# # open a zarr file and read all metadata

# In[2]:


nc_name = 'wrfout_d03_2016-01-04_09:00:00.nc'
zarr_dir='wrfout_d03_2016-01-04_09:00:00.zarr'
store = zarr.DirectoryStore(zarr_dir)
the_group=zarr.hierarchy.Group(store=store, read_only=True,path=None)


# In[3]:


def dump_item(name,var):
    print(f"{'*'*12}")
    print(f'{name}: shape{var.shape}')
    print(f'dimensions: {var.attrs["dimensions"]}')
    if 'description' in var.attrs:
        print(f'description: {var.attrs["description"]}')
    if 'units' in var.attrs:
        print(f'units: {var.attrs["units"]}')
    if 'coordinates' in var.attrs:
        print(f'coordinates: {var.attrs["coordinates"]}')


# In[4]:


print(f'\n{"="*6} Global attributes{"="*6}\n')
for key,value in the_group.attrs.items():
    print(f'{key}: {value}')
print(f'\n{"="*6} Variable attributes{"="*6}\n')
for name, var in the_group.items():
    dump_item(name,var)


# ### Open the corresponding netcdf file and read all metadata

# In[5]:


nc_meta = dump_meta(nc_name)


# In[6]:


pp.pprint(nc_meta)


# In[7]:


out=list(nc_meta.keys())
print(out)


# ### dump to a json file for comparison

# In[8]:


meta_out='nc_meta.json'
with open(meta_out,'w') as out_json:
    json.dump(nc_meta,out_json,indent=4)


# ### Read in a zarr variable

# In[31]:


albedo_zarr = the_group['ALBEDO'][...]


# In[32]:


print(type(albedo_zarr),albedo_zarr.shape)


# ### Read in the corresponding netcdf variable

# In[26]:


with Dataset(nc_name,'r') as nc_in:
    albedo_nc = nc_in.variables['ALBEDO'][...]


# In[33]:


print(type(albedo_nc),albedo_nc.shape)


# In[35]:


albedo_nc[0,50,50] = -200


# In[36]:


np.testing.assert_almost_equal(albedo_nc,albedo_zarr)


# In[ ]:


### Write out 

