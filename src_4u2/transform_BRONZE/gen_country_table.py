# replace country json file into more easily readable table...

import json
from pathlib import Path
import pandas as pd
import numpy as np


def get_country_area_subarea(x):
    x = x.split('_')
    try:
        country = x[0]
    except:
        country = np.nan
    try: 
        area = x[1]
    except:
        area = np.nan 
    try:
        subarea = x[2]
    except:
        subarea = np.nan
    return [country, area, subarea]


ROOT = Path(__file__).parents[2]

with open(ROOT / "data/loc_codes/json_city_codes.json", "r") as f:
    data = json.load(f)

place_id_2_name = {}

for x in data['data']['countries']:
    name1 = x['name'].replace(' ', '')
    place_id_2_name[x['id']] = name1
    areas = x['areas']

    for x2 in areas:
        name2 = x2['name'].replace(' ', '')
        place_id_2_name[x2['id']] = name1 + '_' + name2

        if len(x2['subregion']) > 0: 
            for x3 in x2['subregion']:
                name3 = x3['name'].replace(' ', '')
                place_id_2_name[x3['id']] = name1 + '_' + name2 + "_" + name3

place = pd.DataFrame(place_id_2_name.items(), columns=['place_id', 'place_name'])
area = [get_country_area_subarea(x) for x in place.place_name.values]
place = pd.DataFrame({'place_id':place.place_id,
                          'country' : [x[0] for x in area],
                          'area' :  [x[1] for x in area],
                          'subarea' : [x[2] for x in area]})
        
place.to_csv(ROOT / "data/clean_tables/place")