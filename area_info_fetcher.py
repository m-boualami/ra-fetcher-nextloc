import requests
import json
import time
import csv
import argparse

URL = "https://ra.co/graphql"
HEADERS = {
    "Content-Type": "application/json",
    "Referer": "https://ra.co/",
    "User-Agent": "Mozilla/5.0"
}
DELAY = 1

GRAPHQL_QUERY_TEMPLATE = {
    "operationName": "GET_ABOUT_REGION",
    "variables": {
        "id": "__AREA_ID__"
    },
    "query": "query GET_ABOUT_REGION($id: ID!) { area(id: $id) { blurb eventsCount population } }"
}


def read_unique_area_ids(path):
    area_ids = set()
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            area_ids.add(row["venue_area_id"].strip())
    return list(area_ids)


def build_payload(area_id):
    payload = json.loads(json.dumps(GRAPHQL_QUERY_TEMPLATE))
    payload["variables"]["id"] = area_id
    return payload

def fetch_area_info(area_id):
    payload = build_payload(area_id)
    response = requests.post(URL, headers=HEADERS, json=payload)
    data = response.json()

    area = data.get("data", {}).get("area")
    if area is None:
        return None

    return {
        "venue_area_id": area_id,
        "blurb": area.get("blurb", ""),
        "eventsCount": area.get("eventsCount", 0),
        "population": area.get("population", 0)
    }

def save_areas_to_csv(rows, output_prefix="output"):
    with open(f"{output_prefix}.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["venue_area_id", "blurb", "eventsCount", "population"]
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    print(f"Saved {len(rows)} areas infos.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--areas-file", type=str, required=True, help="CSV containing venue_area_id column")
    parser.add_argument("-o", "--output", type=str, default="area_info", help="Output prefix for CSV files.")
    args = parser.parse_args()

    area_ids = read_unique_area_ids(args.areas_file)

    results = []
    for area_id in area_ids:
        print(f"Fetching area {area_id}")
        info = fetch_area_info(area_id)

        if info is not None:
            results.append(info)
            
        time.sleep(DELAY)

    save_areas_to_csv(results, args.output)


if __name__ == "__main__":
    main()
