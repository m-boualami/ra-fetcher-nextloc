# todo. make more modular for future queries 

import requests
import json
import time
import csv
import argparse
import numpy as np
from datetime import datetime
import calendar
import random
from pathlib import Path
import re
from tqdm import tqdm
import pandas as pd


URL = 'https://ra.co/graphql'
HEADERS = {
    'Content-Type': 'application/json',
    'Referer': 'https://ra.co/events/',
    'User-Agent': 'Mozilla/5.0'
}


DELAY = 1
MAX_RETRIES = 5


class EventMetaDataFetcher:
    def __init__(self, event_id):
        self.event_id = event_id


    def generate_payload(self):
        with open("queries/get_event_meta_data.json") as f:
            payload = json.load(f)
        payload["variables"]["id"] = self.event_id
        return payload


    def parse_lineup(self, lineup):
        artists = []
        pattern = r'<artist id="(\d+)">([^<]+)</artist>|([^<,\n\xa0]+)'
        for m in re.finditer(pattern, lineup):
            if m.group(1):  # tagged with ID
                artists.append({'id': m.group(1), 'name': m.group(2).strip()})
            elif m.group(3) and m.group(3).strip():  # plain text
                artists.append({'id': None, 'name': m.group(3).strip()})
        return artists


    def get_metadata(self):
        payload = self.generate_payload()

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.post(URL, headers=HEADERS, json=payload)


                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    print(f"Rate limited. Waiting {retry_after}s before retry.")
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()

                try:
                    data = response.json()
                    break  # success, exit retry loop
                except ValueError as e:
                    print(f"Corrupted JSON for event {self.event_id}: {e}")
                    raise  # permet retry

            except (requests.exceptions.RequestException, ValueError) as e:
                max_wait = 30  # secondes
                wait = min(2 ** (attempt - 1) + random.random(), max_wait)
                print(f"Error for {artist_id} (try {attempt}/{MAX_RETRIES}): {e}. Retry in {wait:.1f}s")
                time.sleep(wait)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch events: {response.status_code}")
        return data


    def fetch_all_metadata(self):
        response = self.get_metadata()

        if response["data"]["event"] is None:
            return None
        else:
            event_object = response["data"]["event"]
            event_meta_data = {'id': self.event_id, 
                                'title': event_object['title'], 
                                'description': event_object['content'], 
                                'start_time': event_object['startTime'],
                                'lineup': self.parse_lineup(event_object['lineup']),
                                'genres': [g['name'] for g in event_object['genres']],
                                'interested_count': event_object['interestedCount'],
                                'is_festival': event_object['isFestival'],
                                'has_secret_venue': event_object['hasSecretVenue'],
                                'is_ticketed': event_object['isTicketed'],
                                'flyer_photo': event_object['images'][0]['filename'] if len(event_object['images']) > 0 else None,
                                'venue_name': event_object['venue']['name'],
                                'venue_id': event_object['venue']['id'],
                                'venue_address': event_object['venue']['address'],
                                'venue_area_id': event_object['venue']['area']['id'],
                                'venue_lat': event_object['venue']['location']['latitude'],
                                'venue_lng': event_object['venue']['location']['longitude'],
                                'promoter_id': event_object['promoters'][0]['id'] if len(event_object['promoters']) > 0 else None,
                                'promoter_name': event_object['promoters'][0]['name'] if len(event_object['promoters']) > 0 else None}
            return event_meta_data


def fetch_event_metadata(event_ids):
    venue_data = []
    i = 0
    for event_id in tqdm(event_ids):
        if i % 5000 == 0:
            time.sleep(random.uniform(25, 40)) # random sleep time to sim. human
        fetcher = EventMetaDataFetcher(event_id)
        event_meta_data = fetcher.fetch_all_metadata()
        if event_meta_data is not None:
            venue_data.append(event_meta_data.values())
        time.sleep(0.1)
        i += 1
    df_cols = event_meta_data.keys()
    return venue_data, df_cols


def main():
    run_flag = 0

    with open("../data/loc_codes/country2areaid.json", "r") as f:
        country2aid = json.load(f)
    countries = sorted(set(country2aid.keys()))

    dir = Path("../data/event_ids")
    f_all = sorted([str(f) for f in dir.iterdir()])

    venue_data = []

    # get all files associated with the country
    for country in countries:
        # todo. uncomment after debugging - 17628 (error)
        if country == "United States of America":
            run_flag = 1
        if run_flag == 1:
            f_country = [f for f in f_all if country in f]  # get file names
            event_ids = np.concatenate([np.sort(np.load(fname)) for fname in f_country]) # get all event ids for the country
            if len(event_ids) > 0:
                print(f"* Computing meta-data for {len(event_ids)} events in {country}")
                meta_data, cols = fetch_event_metadata(event_ids)
                # save as df
                pd.DataFrame(meta_data, columns=cols).to_csv(f"./data/event_meta_data/{country}.csv", index=False) 

        # todo. filter those >= 2026-03-01


if __name__ == "__main__":

    # time execution for testing
    start = time.time()
    main()
    print(f"Done in {(time.time() - start) / 60:.2f} minutes")
