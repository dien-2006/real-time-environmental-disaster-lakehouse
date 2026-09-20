from datetime import datetime, timedelta, timezone
from common.http_client import create_http_session
from common.events import envelope, digest
from config.settings import setting


class OpenAQClient:
    TOPIC = "air_quality"

    def __init__(self):
        if not setting.OPENAQ_API_KEY:
            raise ValueError("OPENAQ_API_KEY is missing")
        
        self.session = create_http_session()

        self.session.headers.update({"X-API-Key": setting.OPENAQ_API_KEY})
        self.url = "https://api.openaq.org/v3"

    def _pages(self, path, **params):
        page, limit = 1, 100
        while True:
            response = self.session.get(f"{self.url}/{path}",
                params={**params, "page": page, "limit": limit}, timeout=30)
            response.raise_for_status()
            rows = response.json()["results"]
            yield from rows

            if len(rows) < limit:
                break
            page += 1

    def fetch(self):
        # Overlap to capture late measurements; long outages require backfill.
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=1)
        countries = list(self._pages("countries"))
        
        country_id = next(c["id"] for c in countries if c["code"] == "VN")

        for location in self._pages("locations", countries_id=country_id):
            for sensor in location.get("sensors", []):
                for row in self._pages(f"sensors/{sensor['id']}/measurements",
                        datetime_from=start.isoformat(), datetime_to=end.isoformat()):
                    yield envelope("openaq", "air_quality",
                        [sensor["id"], row.get("period", {}).get("datetimeFrom") or digest(row)], row,
                        context={"location": location, "sensor": sensor})

    def fetch_openaq(self):
        return list(self.fetch())
