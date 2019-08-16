import json
import pprint
pp = pprint.PrettyPrinter(indent=4)

with open('metadata.json','r') as infile:
    meta_dict=json.load(infile)

pp.pprint(meta_dict)

