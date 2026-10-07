import requests


API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"


params = {
    "$select": "count(*)",
    "$where": (
        "inspection_date > '1900-01-01T00:00:00' "
        "AND inspection_date < '2026-09-30T00:00:00'"
    )
}


response = requests.get(
    API_URL,
    params=params,
    timeout=30
)

response.raise_for_status()

data = response.json()

print(f"Actual inspection rows through 2026-09-29: {data[0]['count']}")