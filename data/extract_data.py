import json
import requests

API_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
STATE_FILE = "data/extraction_state.json"
PAGE_SIZE = 50000


def extract_restaurant_data():
    with open(STATE_FILE, "r") as file:
        state = json.load(file)

    offset = state["offset"]

    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30
    )
    response.raise_for_status()

    records = response.json()


    state["offset"] = offset + len(records)

    with open(STATE_FILE, "w") as file:
        json.dump(state, file)

    return records


if __name__ == "__main__":
    data = extract_restaurant_data()