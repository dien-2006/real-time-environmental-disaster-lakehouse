import requests
import json

URL = "https://api.open-meteo.com/v1/forecast"

LOCATIONS = {
    "hanoi": {
        "latitude": 21.0285,
        "longitude": 105.8542,
    },
    "danang": {
        "latitude": 16.0544,
        "longitude": 108.2022,
    },
    "hochiminh": {
        "latitude": 10.8231,
        "longitude": 106.6297,
    },
}


def fetch_weather():
    for city, coordinates in LOCATIONS.items():

        params = {
            "latitude": coordinates["latitude"],
            "longitude": coordinates["longitude"],
            "current": ",".join([
                "temperature_2m",
                "relative_humidity_2m",
                "precipitation",
                "weather_code",
                "wind_speed_10m",
            ]),
            "timezone": "Asia/Bangkok",
        }

        response = requests.get(
            URL,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        print(f"\n===== {city.upper()} =====")
        print(json.dumps(
            data,
            indent=4,
            ensure_ascii=False
        ))


if __name__ == "__main__":
    fetch_weather()