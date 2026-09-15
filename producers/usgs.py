from datetime import datetime, timezone

from common.http_client import create_http_session


URL = (
    "https://earthquake.usgs.gov/"
    "earthquakes/feed/v1.0/"
    "summary/all_hour.geojson"
)

TOPIC = "earthquake"


def fetch_earthquakes():

    session = create_http_session()

    response = session.get(
        URL,
        timeout=30
    )

    response.raise_for_status()

    payload = response.json()

    events = []

    for feature in payload.get(
        "features",
        []
    ):

        earthquake_id = feature.get("id")

        properties = feature.get(
            "properties",
            {}
        )

        geometry = feature.get(
            "geometry",
            {}
        )

        coordinates = geometry.get(
            "coordinates",
            []
        )

        longitude = (
            coordinates[0]
            if len(coordinates) > 0
            else None
        )

        latitude = (
            coordinates[1]
            if len(coordinates) > 1
            else None
        )

        depth = (
            coordinates[2]
            if len(coordinates) > 2
            else None
        )

        event = {
            "source": "usgs",
            "event_type": "earthquake",

            "event_id": earthquake_id,

            "observed_at": properties.get(
                "time"
            ),

            "ingested_at": datetime.now(
                timezone.utc
            ).isoformat(),

            "latitude": latitude,
            "longitude": longitude,
            "depth_km": depth,

            "data": {
                "magnitude": properties.get("mag"),
                "place": properties.get("place"),
                "type": properties.get("type"),
                "status": properties.get("status"),
                "tsunami": properties.get("tsunami"),
            },
        }

        events.append(
            (
                earthquake_id,
                event
            )
        )

    return events