import os
import csv
import io
import json
import hashlib
from typing import Iterator
from dotenv import load_dotenv
from datetime import datetime, timezone
from common.http_client import create_http_session

class NASAFirmsClient:

    TOPIC = "nasa_firms"
    SOURCE = "VIIRS_SNPP_NRT"
    VN_BOX = "102.14,8.18,109.46,23.39"
    BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

    def __init__(self):
        load_dotenv()

        self.session = create_http_session()
        self.map_key = os.getenv("NASA_FIRMS_MAP_KEY")

        # self.url = (
        #     f"{self.BASE_URL}/"
        #     f"{self.map_key}/"
        #     f"{self.SOURCE}/"
        #     f"{self.VN_BOX}/"
        #     f"1"
        # )
        self.url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{self.map_key}/VIIRS_SNPP_NRT/world/1"

    def fetch(self) -> Iterator[dict]:

        response = self.session.get(
            self.url,
            timeout=30
        )

        response.raise_for_status()

        reader = csv.DictReader(
            io.StringIO(response.text)
        )

        for row in reader:

            raw_id = (f"nasa_firms|"
                f"{row['latitude']}|"
                f"{row['longitude']}|"
                f"{row['acq_date']}|"
                f"{row['acq_time']}|"
                f"{row['satellite']}")

            event_id = hashlib.sha256(raw_id.encode("utf-8")).hexdigest()

            event = {
                "event_id" : event_id,
                "source": "nasa_firms",
                "event_type": "wildfire",
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "observed_date": row.get("acq_date"),
                "observed_time": row.get("acq_time"),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "data": row
            }

            yield event

if __name__ == "__main__":
    client = NASAFirmsClient()
    # for event in client.fetch():
    #     print(json.dumps(event))
