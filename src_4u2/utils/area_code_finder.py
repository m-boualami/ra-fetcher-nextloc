## find area codes from json_city_codes.json which can then be used to fetch all events from RA

import json

if __name__ == "__main__":

    with open("../data/loc_codes/json_city_codes.json", "r") as f:
        data = json.load(f)

    country_areas = {}
    for c in data['data']['countries']:
        country_areas[c['name']] = [int(x['id']) for x in c['areas']]


    with open("../data/loc_codes/country2areaid.json", "w") as f:
        json.dump(country_areas, f)