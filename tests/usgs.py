import requests
import json

URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"

params = {
    "format": "geojson",
    "minmagnitude": 2.5,
    "limit": 10,
    "orderby": "time"
}

response = requests.get(
    URL,
    params=params,
    timeout=30
)

response.raise_for_status()

data = response.json()

print(json.dumps(
    data,
    indent=4,
    ensure_ascii=False
))