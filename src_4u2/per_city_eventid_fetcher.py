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


URL = 'https://ra.co/graphql'
HEADERS = {
    'Content-Type': 'application/json',
    'Referer': 'https://ra.co/events/',
    'User-Agent': 'Mozilla/5.0'
}


DELAY = 1
MAX_RETRIES = 5


class EventIDFetcher:
    def __init__(self, area, date_gte, date_lte):
        self.area = area
        self.date_gte = date_gte
        self.date_lte = date_lte


    def generate_payload(self, page):
        with open("queries/get_event_ids_for_city.json") as f:
            payload = json.load(f)
        payload["variables"]["filters"]["areas"]["any"] = self.area
        payload["variables"]["filters"]["listingDate"]["gte"] = self.date_gte
        payload["variables"]["filters"]["listingDate"]["lte"] = self.date_lte
        payload["variables"]["page"] = page
        return payload


    def get_events(self, page):
        payload = self.generate_payload(page)

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
                except ValueError as e:
                    print(f"Corrupted JSON for page {page}: {e}")
                    raise  # permet retry

            except (requests.exceptions.RequestException, ValueError) as e:
                max_wait = 30  # secondes
                wait = min(2 ** (attempt - 1) + random.random(), max_wait)
                print(f"Error for {artist_id} (try {attempt}/{MAX_RETRIES}): {e}. Retry in {wait:.1f}s")
                time.sleep(wait)

        if response.status_code != 200:
            raise Exception(f"Failed to fetch events: {response.status_code}")

        return data


    def fetch_all_events(self):
        all_events = []
        
        # todo. remove after debugging
        page = 1
        #page = 376

        while True:
            response = self.get_events(page)
            events = response["data"]["eventListings"]["data"]

            if not events:
                break

            # could be thrown due to server error on their side
            if (None in events) == False:
                event_ids_page = [event["event"]["id"] for event in events]
                all_events.extend(event_ids_page)
                print(f"Page {page} fetched ({len(events)} events)")
                page += 1
                time.sleep(DELAY)
            else: 
                time.sleep(20) # sleep for 20s, then try again

        all_events = np.array(all_events)
        return all_events



def fetch_and_save_eventids(save_fname, area_list, date_gte, date_lte):
        fetcher = EventIDFetcher(area_list, date_gte, date_lte)

        print(f"\nFetching events for {save_fname} ...")
        event_ids = fetcher.fetch_all_events()

        np.save(f"./data/event_ids/{save_fname}.npy", event_ids)
        print(f"{len(event_ids)} unique event ids saved.")
        time.sleep(2)



def last_day_of_month(timestamp: str) -> str:
    dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(day=last_day).strftime("%Y-%m-%dT%H:%M:%S.") + \
           f"{dt.microsecond // 1000:03d}Z"



def main():
    # todo: ensure that we filter 2026-03-01 from the analysis once all done


    # assert cont. from : United Kingdom_344_2025-04-01T00:00:00.000Z
    cont_from = {"country": "United Kingdom", "area_id": 344, "month": "2025-04-01T00:00:00.000Z"} # city, area_id, date_idx
    run_flag = 0

    countries_with_more_10K_events = ['United Kingdom','Germany','Spain','Japan','United States of America']


    start_dates = ["2025-03-01T00:00:00.000Z", 
                    "2025-04-01T00:00:00.000Z",
                    "2025-05-01T00:00:00.000Z",
                    "2025-06-01T00:00:00.000Z",
                    "2025-07-01T00:00:00.000Z",
                    "2025-08-01T00:00:00.000Z",
                    "2025-09-01T00:00:00.000Z",
                    "2025-10-01T00:00:00.000Z",
                    "2025-11-01T00:00:00.000Z",
                    "2025-12-01T00:00:00.000Z",
                    "2026-01-01T00:00:00.000Z",
                    "2026-02-01T00:00:00.000Z"]
    end_dates = [last_day_of_month(date) for date in start_dates]
    

    with open("./data/loc_codes/country2areaid.json", "r") as f:
        country2aid = json.load(f)

    for country_name, area_list in country2aid.items():

        for date_gte, date_lte in zip(start_dates, end_dates):

            if country_name in countries_with_more_10K_events:
                for area_id in area_list:
                
                    # todo. uncomment after debugging
                    if (country_name == cont_from["country"]) & (date_gte == cont_from["month"]) & (area_id == cont_from['area_id']):
                        run_flag = 1
                    
                    # todo. uncomment after debugging
                    if run_flag == 1:
                        fname = country_name + "_" + str(area_id) + "_" + date_gte
                        fetch_and_save_eventids(fname, [area_id], date_gte, date_lte)

            # todo. uncomment after debugging
            # else:
            #.    fname = country_name + "_" + date_gte
            #.    fetch_and_save_eventids(fname, area_list, date_gte, date_lte)



if __name__ == "__main__":

    # time execution for testing
    start = time.time()
    main()
    print(f"Done in {(time.time() - start) / 60:.2f} minutes")
