from datetime import datetime, timezone
from common.http_client import create_http_session
import json
class OpenMeteoClient:

    def __init__(self):

        self.url = "https://api.open-meteo.com/v1/forecast"
        TOPIC = "weather"

        self.session = create_http_session()

    def fetch_weather(self):

        events= []

        LOCATIONS = {
            "ha_noi": (21.0285, 105.8542),
            "hue": (16.4637, 107.5909),
            "lai_chau": (22.3864, 103.4703),
            "dien_bien": (21.3860, 103.0230),
            "son_la": (21.3270, 103.9140),
            "lang_son": (21.8537, 106.7615),
            "cao_bang": (22.6666, 106.2640),
            "tuyen_quang": (21.8236, 105.2143),
            "lao_cai": (22.4809, 103.9755),
            "thai_nguyen": (21.5942, 105.8482),
            "phu_tho": (21.3227, 105.4019),
            "bac_ninh": (21.1861, 106.0763),
            "hung_yen": (20.6464, 106.0511),
            "hai_phong": (20.8449, 106.6881),
            "ninh_binh": (20.2506, 105.9745),
            "thanh_hoa": (19.8067, 105.7852),
            "nghe_an": (18.6796, 105.6813),
            "ha_tinh": (18.3559, 105.8877),
            "quang_tri": (16.8163, 107.1003),
            "da_nang": (16.0544, 108.2022),
            "quang_ngai": (15.1214, 108.8044),
            "gia_lai": (13.9833, 108.0000),
            "dak_lak": (12.6667, 108.0500),
            "khanh_hoa": (12.2388, 109.1967),
            "lam_dong": (11.9404, 108.4583),
            "ho_chi_minh": (10.8231, 106.6297),
            "dong_nai": (10.9574, 106.8426),
            "tay_ninh": (11.3352, 106.1099),
            "can_tho": (10.0452, 105.7469),
            "vinh_long": (10.2537, 105.9722),
            "dong_thap": (10.4938, 105.6882),
            "an_giang": (10.5216, 105.1259),
            "ca_mau": (9.1769, 105.1524),
        }

        for city, (latitude, longitude) in LOCATIONS.items():

            params = {
                "latitude": latitude,
                "longitude": longitude,
                "current": [
                    "temperature_2m",
                    "relative_humidity_2m",
                    "precipitation",
                    "rain",
                    "cloud_cover",
                    "pressure_msl",
                    "surface_pressure",
                    "wind_speed_10m",
                    "wind_direction_10m",
                    "wind_gusts_10m"
                ],
                "timezone": "Asia/Bangkok"
            }

            response = self.session.get(self.url, params= params, timeout= 30)
            response.raise_for_status()

            data = response.json()

            event = {
                "source": "open_meteo",
                "event_type": "weather",
                "location": city,
                "latitude": data.get("latitude"),
                "longitude": data.get("longitude"),
                "observed_at": data.get("current", {}).get("time"),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "data": data.get("current", {})
            }

            events.append(event)

        return events

if __name__ == "__main__":
    client = OpenMeteoClient()
    events = client.fetch_weather()
    for event in events:
        print(json.dumps(event))