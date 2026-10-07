import sys
import requests
from datetime import datetime, timedelta

API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
PAGE_SIZE = 50000


def extract_restaurant_data(extraction_date):
    date = datetime.strptime(extraction_date, "%Y-%m-%d")
    next_date = date + timedelta(days=1)

    records = []
    offset = 0

    while True:
        params = {
            "$limit": PAGE_SIZE,
            "$offset": offset,
            "$where": (
                f"inspection_date >= '{extraction_date}T00:00:00' "
                f"AND inspection_date < '{next_date.strftime('%Y-%m-%d')}T00:00:00'"
            )
        }

        response = requests.get(
            API_URL,
            params=params,
            timeout=30
        )
        response.raise_for_status()

        batch = response.json()

        if not batch:
            break

        records.extend(batch)

        if len(batch) < PAGE_SIZE:
            break

        offset += len(batch)

    return records


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python3 extract_data_daily.py YYYY-MM-DD")

    data = extract_restaurant_data(sys.argv[1])
    print(f"Extracted {len(data)} records for {sys.argv[1]}")