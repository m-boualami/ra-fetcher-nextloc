import requests
import json
import time
import csv
import argparse

URL = 'https://ra.co/graphql'
HEADERS = {
    'Content-Type': 'application/json',
    'Referer': 'https://ra.co/events/',
    'User-Agent': 'Mozilla/5.0'
}

DELAY = 1

class AreaArtistFetcher:

    def __init__(self, area, date_gte, date_lte):
        self.area = area
        self.date_gte = date_gte
        self.date_lte = date_lte

    def generate_payload(self, page):
        return {
            "operationName": "GET_EVENT_LISTINGS",
            "variables": {
                "filters": {
                    "areas": {"eq": self.area},
                    "listingDate": {
                        "gte": self.date_gte,
                        "lte": self.date_lte
                    }
                },
                "filterOptions": {"genre": True},
                "pageSize": 20,
                "page": page
            },
            "query": """
            query GET_EVENT_LISTINGS($filters: FilterInputDtoInput, $filterOptions: FilterOptionsInputDtoInput, $page: Int, $pageSize: Int) {
              eventListings(filters: $filters, filterOptions: $filterOptions, pageSize: $pageSize, page: $page) {
                data {
                  event {
                    artists {
                      id
                      name
                    }
                  }
                }
              }
            }
            """
        }

    def get_events(self, page):
        payload = self.generate_payload(page)
        response = requests.post(URL, headers=HEADERS, json=payload)
        data = response.json()
        return data["data"]["eventListings"]["data"]

    def fetch_all_events(self):
        all_events = []
        page = 1

        while True:
            events = self.get_events(page)

            if not events:
                break

            all_events.extend(events)
            print(f"Page {page} fetched ({len(events)} events)")
            page += 1
            time.sleep(DELAY)

        return all_events


def extract_artists(events):
    artists = {}
    for item in events:
        for artist in item["event"]["artists"]:
            artists[artist["id"]] = artist["name"]
    return artists


def save_artists(artists, output_prefix):
    with open(f"{output_prefix}_artists.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["artist_id", "artist_name"], delimiter=';')
        writer.writeheader()
        for aid, name in artists.items():
            writer.writerow({"artist_id": aid, "artist_name": name})

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("area", type=int)
    parser.add_argument("start_date", type=str)
    parser.add_argument("end_date", type=str)
    parser.add_argument("-o", "--output", default="city")
    args = parser.parse_args()

    date_gte = f"{args.start_date}T00:00:00.000Z"
    date_lte = f"{args.end_date}T23:59:59.999Z"

    fetcher = AreaArtistFetcher(args.area, date_gte, date_lte)
    events = fetcher.fetch_all_events()
    artists = extract_artists(events)
    save_artists(artists, args.output)

    print(f"{len(artists)} unique artists saved.")


if __name__ == "__main__":
    main()
