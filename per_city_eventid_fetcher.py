# todo. make more modular for future queries 

import requests
import json
import time
import csv
import argparse
import numpy as np


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
        page = 1

        while True:
            response = self.get_events(page)
            events = response["data"]["eventListings"]["data"]

            if not events:
                break

            event_ids_page = [event["event"]["id"] for event in events]
            all_events.extend(event_ids_page)
            
            print(f"Page {page} fetched ({len(events)} events)")
            page += 1
            time.sleep(DELAY)


        all_events = np.array(all_events)
        return all_events


def main():

    # not sure if this is correct fmt, lets see if it works
    date_gte = "2025-02-01T00:00:00.000Z"
    date_lte = "2025-02-07T23:59:59.999Z" # 2 days test case

    with open("./data/loc_codes/country2areaid.json", "r") as f:
        country2aid = json.load(f)


    for country_name, area_list in country2aid.items():
        fetcher = EventIDFetcher(area_list, date_gte, date_lte)

        print(f"\nFetching events for {country_name} ...")
        event_ids = fetcher.fetch_all_events()

        np.save(f"./data/event_ids/{country_name}.npy", event_ids)
        print(f"{len(event_ids)} unique event ids saved.")
        time.sleep(2)
 

if __name__ == "__main__":

    # time execution for testing
    start = time.time()
    main()
    print(f"Done in {(time.time() - start) / 60:.2f} minutes")
