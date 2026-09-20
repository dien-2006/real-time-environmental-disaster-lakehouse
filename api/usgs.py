from common.http_client import create_http_session
from common.events import envelope


class USGSClient:
    TOPIC = "earthquake"

    def __init__(self):
        self.session = create_http_session()
        self.url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    def fetch(self):
        response = self.session.get(self.url, timeout=30)
        response.raise_for_status()
        for feature in response.json()["features"]:
            yield envelope("usgs", "earthquake", feature["id"], feature)
