import requests
import json
import time
import csv
import argparse
import random

URL = 'https://ra.co/graphql'
HEADERS = {
    'Content-Type': 'application/json',
    'Referer': 'https://ra.co/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0'
}

DELAY = 1
TIMEOUT = 10
MAX_RETRIES = 5

GRAPHQL_QUERY_TEMPLATE = {
    "operationName": "GET_ARTIST_EVENTS_ARCHIVE",
    "variables": {
        "id": "__ARTIST_ID__"
    },
    "query": "query GET_ARTIST_EVENTS_ARCHIVE($id: ID!) { artist(id: $id) { id events(limit: 2000, type: PREVIOUS) { id interestedCount date venue { id name area { id urlName country { id name } } } __typename } __typename } }"
}


def fetch_artist_events(artist_id):
    """Fetch events for a single artist with retries, timeout, and JSON error handling."""
    payload = json.loads(json.dumps(GRAPHQL_QUERY_TEMPLATE))
    payload["variables"]["id"] = artist_id

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(URL, headers=HEADERS, json=payload, timeout=TIMEOUT)
            
            # Gestion du rate-limit 429
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                print(f"Rate limited for artist {artist_id}. Waiting {retry_after}s before retry.")
                time.sleep(retry_after)
                continue

            response.raise_for_status()

            try:
                data = response.json()
            except ValueError as e:
                print(f"Corrupted JSON for artist {artist_id}: {e}")
                raise  # permet retry

            artist_data = data.get("data", {}).get("artist")
            if not artist_data:
                print(f"No data for artist {artist_id}")
                return []

            events = artist_data.get("events", [])
            all_results = []
            for event in events:
                venue = event.get("venue") or {}
                area = venue.get("area") or {}
                country = area.get("country") or {}
                all_results.append({
                    "artist_id": artist_id,
                    "event_id": event.get("id"),
                    "date": event.get("date"),
                    "interested_count": event.get("interestedCount"),
                    "venue_id": venue.get("id"),
                    "venue_name": venue.get("name"),
                    "venue_area_id": area.get("id"),
                    "venue_area": area.get("urlName"),
                    "country_id": country.get("id"),
                    "country_name": country.get("name")
                })

            return all_results

        except (requests.exceptions.RequestException, ValueError) as e:
            max_wait = 30  # secondes
            wait = min(2 ** (attempt - 1) + random.random(), max_wait)
            print(f"Error for {artist_id} (try {attempt}/{MAX_RETRIES}): {e}. Retry in {wait:.1f}s")
            time.sleep(wait)

    print(f"Failed to fetch {artist_id} after {MAX_RETRIES} tries.")
    return []


def normalize_and_save(events, output_prefix="output"):
    events_list = []
    venues = {}
    countries = {}

    for event in events:
        venue_id = event["venue_id"]
        country_id = event["country_id"]

        if venue_id not in venues:
            venues[venue_id] = {
                "venue_name": event["venue_name"],
                "venue_area_id": event["venue_area_id"],
                "venue_area": event["venue_area"],
                "country_id": country_id
            }

        if country_id not in countries:
            countries[country_id] = event["country_name"]

        events_list.append({
            "artist_id": event["artist_id"],
            "event_id": event["event_id"],
            "date": event["date"],
            "interested_count": event["interested_count"],
            "venue_id": venue_id
        })

    # write CSV
    with open(f"{output_prefix}_events.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["artist_id", "event_id", "date", "interested_count", "venue_id"], delimiter=';')
        writer.writeheader()
        writer.writerows(events_list)

    with open(f"{output_prefix}_venues.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["venue_id", "venue_name", "venue_area_id", "venue_area", "country_id"], delimiter=';')
        writer.writeheader()
        for vid, vinfo in venues.items():
            row = {"venue_id": vid, **vinfo}
            writer.writerow(row)

    with open(f"{output_prefix}_countries.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["country_id", "country_name"], delimiter=';')
        writer.writeheader()
        for cid, cname in countries.items():
            writer.writerow({"country_id": cid, "country_name": cname})

    print(f"Saved {len(events_list)} events, {len(venues)} venues, {len(countries)} countries.")


def read_artist_ids_from_csv(path):
    artist_ids = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            artist_ids.append(row["artist_id"].strip())
    return artist_ids


def main():
    parser = argparse.ArgumentParser(description="Fetch artist events from RA.co and save normalized CSVs.")
    parser.add_argument("--artists-file", type=str, help="CSV file containing artist_id column")
    parser.add_argument("-o", "--output", type=str, default="output", help="Output prefix for CSV files.")
    args = parser.parse_args()

    artist_ids = read_artist_ids_from_csv(args.artists_file)
    all_events = []

    for i, artist_id in enumerate(artist_ids, 1):
        if i % 10 == 0:
            print(f"{i} artists processed... total events so far: {len(all_events)}")
        events = fetch_artist_events(artist_id)
        all_events.extend(events)
        time.sleep(DELAY)

    normalize_and_save(all_events, args.output)


if __name__ == "__main__":
    main()

