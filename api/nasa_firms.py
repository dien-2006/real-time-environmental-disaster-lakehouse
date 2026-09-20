import csv
import io
import os
from common.http_client import create_http_session
from common.events import envelope
from config.settings import setting


class NASAFirmsClient:
    TOPIC = "nasa_firms"
    SOURCE = "VIIRS_SNPP_NRT"
    VN_BOX = "102.14,8.18,109.46,23.39"
    BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

    def __init__(self):
        if not setting.NASA_FIRMS_MAP_KEY:
            raise ValueError("NASA_FIRMS_MAP_KEY is missing")
        
        self.session = create_http_session()

        area = os.getenv("NASA_FIRMS_AREA", self.VN_BOX)

        self.url = f"{self.BASE_URL}/{setting.NASA_FIRMS_MAP_KEY}/{self.SOURCE}/{area}/1"

    def fetch(self):

        response = self.session.get(self.url, timeout=30)
        response.raise_for_status()

        reader = csv.DictReader(io.StringIO(response.text))

        fields = ["latitude", "longitude", "acq_date", "acq_time", "satellite"]

        if not reader.fieldnames or not set(fields).issubset(reader.fieldnames):
            raise ValueError("NASA FIRMS returned an invalid CSV response")
        
        for row in reader:
            yield envelope("nasa_firms", "wildfire", [row[f] for f in fields], row)
