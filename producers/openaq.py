from datetime import datetime, timezone

from common.http_client import create_http_session
from config.settings import OPENAQ_API_KEY


BASE_URL = "https://api.openaq.org/v3"

TOPIC = "air_quality"


def fetch_openaq():

    if not OPENAQ_API_KEY:
        raise ValueError(
            "OPENAQ_API_KEY is missing"
        )

    session = create_http_session()

    headers = {
        "X-API-Key": OPENAQ_API_KEY
    }

    # Lấy station Việt Nam
    response = session.get(
        f"{BASE_URL}/locations",
        headers=headers,
        params={
            "iso": "VN",
            "limit": 100,
        },
        timeout=30,
    )

    response.raise_for_status()

    locations = response.json().get(
        "results",
        []
    )

    events = []

    for location in locations:

        location_id = location.get("id")

        if not location_id:
            continue

        latest_response = session.get(
            f"{BASE_URL}/locations/"
            f"{location_id}/latest",
            headers=headers,
            params={
                "limit": 100
            },
            timeout=30,
        )

        latest_response.raise_for_status()

        latest = latest_response.json().get(
            "results",
            []
        )

        for measurement in latest:

            sensor_id = measurement.get(
                "sensorsId"
            )

            dt = measurement.get(
                "datetime",
                {}
            )

            observed_at = (
                dt.get("utc")
                if isinstance(dt, dict)
                else dt
            )

            event_key = (
                f"{location_id}-"
                f"{sensor_id}"
            )

            event = {
                "source": "openaq",
                "event_type": "air_quality",

                "location_id": location_id,
                "location_name": location.get(
                    "name"
                ),

                "sensor_id": sensor_id,

                "observed_at": observed_at,

                "ingested_at": datetime.now(
                    timezone.utc
                ).isoformat(),

                "data": measurement,
            }

            events.append(
                (
                    event_key,
                    event
                )
            )

    return events