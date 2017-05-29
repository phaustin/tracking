import ujson
from collections import defaultdict

filename='unique_list.json'
with open(filename,'r') as infile:
    tup_list=ujson.load(infile)

cloud_list=[cloud_id for cloud_id,timestep in tup_list]
time_steps=[timestep for cloud_id,timestep in tup_list]

count_dict=defaultdict(list)
len_list=[]
for cloud_id,timestep in tup_list:
    count_dict[cloud_id].append(timestep)

for key,value in count_dict.items():
    the_len=len(count_dict[key])
    len_list.append((key,the_len))

def sort_len(item):
    return -item[1]

len_list.sort(key=sort_len)

for cloud_id,_ in len_list[:50]:
    print(cloud_id,count_dict[cloud_id])
    
          

    
    
