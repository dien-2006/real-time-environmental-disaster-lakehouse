"""Normalize source events without modifying Bronze objects."""

from datetime import datetime, timedelta, timezone
import math

UTC = timezone.utc


def number(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("boolean_numeric_value")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("non_finite_value")
    return result


def timestamp(value, offset=None):
    if isinstance(value, dict):
        value = value.get("utc")
    if isinstance(value, (int, float)):
        result = datetime.fromtimestamp(value / 1000, UTC)
    else:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            if offset is None:
                raise ValueError("timezone_missing")
            result = result.replace(tzinfo=timezone(timedelta(seconds=offset)))
    return result.astimezone(UTC).isoformat(timespec="microseconds")


def normalize(record):
    event = record["event"]
    if not isinstance(event, dict) or event.get("schema_version") != "1.0":
        raise ValueError("unsupported_event_schema")
    source, raw = event["source"], event["raw"]
    context = event.get("context") or {}
    metrics = []

    def metric(name, value, unit):
        value = number(value)
        if value is not None:
            if not unit:
                raise ValueError("measurement_unit_missing")
            metrics.append({"name": name, "value": value, "unit": unit})

    latitude = longitude = location_id = location_name = None
    ingested = timestamp(event["ingested_at"])
    updated = ingested
    dimensions = {}
    period_end = None
    if source == "usgs":
        props = raw["properties"]
        coords = raw["geometry"]["coordinates"]
        longitude, latitude = coords[:2]
        observed = timestamp(props["time"])
        if props.get("updated") is not None:
            updated = timestamp(props["updated"])
        location_name = props.get("place")
        metric(
            "magnitude",
            props.get("mag"),
            props.get("magType") or "unspecified_magnitude",
        )
        if len(coords) > 2:
            metric("depth", coords[2], "km")
        dimensions["status"] = props.get("status")
    elif source == "nasa_firms":
        clock = str(raw["acq_time"]).zfill(4)
        observed = timestamp(f"{raw['acq_date']}T{clock[:2]}:{clock[2:]}:00+00:00")
        latitude, longitude = raw["latitude"], raw["longitude"]
        metric("fire_radiative_power", raw.get("frp"), "MW")
        dimensions = {k: raw.get(k) for k in ("satellite", "instrument", "confidence")}
    elif source == "open_meteo":
        current = raw["current"]
        observed = timestamp(current["time"], raw.get("utc_offset_seconds"))
        latitude, longitude = raw["latitude"], raw["longitude"]
        location_id = location_name = context["location_name"]
        units = raw.get("current_units", {})
        for name, value in current.items():
            if name in ("time", "interval"):
                continue
            unit = units.get(name)
            if value is not None and unit == "km/h":
                value, unit = number(value) / 3.6, "m/s"
            elif value is not None and unit == "°F":
                value, unit = (number(value) - 32) * 5 / 9, "°C"
            metric(name, value, unit)
    elif source == "openaq":
        location, sensor = context["location"], context["sensor"]
        observed = timestamp(raw["period"]["datetimeFrom"])
        coords = raw.get("coordinates") or location.get("coordinates") or {}
        latitude, longitude = coords.get("latitude"), coords.get("longitude")
        location_id, location_name = str(location["id"]), location.get("name")
        parameter = raw.get("parameter") or sensor["parameter"]
        metric(parameter["name"], raw["value"], parameter["units"])
        dimensions["sensor_id"] = sensor["id"]
        if not metrics or sensor["id"] is None:
            raise ValueError("air_quality_measurement_missing")
        if raw["period"].get("datetimeTo") is not None:
            period_end = timestamp(raw["period"]["datetimeTo"])
            if period_end < observed:
                raise ValueError("measurement_period_reversed")
    else:
        raise ValueError("unsupported_source")
    latitude, longitude = number(latitude), number(longitude)
    if latitude is not None and not -90 <= latitude <= 90:
        raise ValueError("latitude_out_of_range")
    if longitude is not None and not -180 <= longitude <= 180:
        raise ValueError("longitude_out_of_range")
    if not event.get("event_id"):
        raise ValueError("event_id_missing")
    return {
        "schema_version": "2.0",
        "event_id": event["event_id"],
        "source": source,
        "event_type": event["event_type"],
        "observed_at": observed,
        "observed_date": observed[:10],
        "ingested_at": ingested,
        "period_end": period_end,
        "source_updated_at": updated,
        "payload_hash": event["payload_hash"],
        "location_id": location_id,
        "location_name": location_name,
        "latitude": latitude,
        "longitude": longitude,
        "metrics": metrics,
        "dimensions": dimensions,
        "kafka": record["kafka"],
    }


def build_silver(records):
    """Latest valid version per source/event; duplicate transport offsets removed."""
    latest, seen, rejects = {}, set(), []
    for record in records:
        try:
            if not isinstance(record, dict):
                raise ValueError("invalid_bronze_record")
            k = record["kafka"]
            transport = (k["topic"], k["partition"], k["offset"])
            if transport in seen:
                continue
            row = normalize(record)
            key = (row["source"], row["event_id"])
            rank = (row["source_updated_at"], row["ingested_at"], row["payload_hash"])
            if key not in latest or rank > latest[key][0]:
                latest[key] = (rank, row)
            seen.add(transport)
        except (
            ValueError,
            TypeError,
            KeyError,
            IndexError,
            AttributeError,
            OverflowError,
        ) as exc:
            rejects.append({"reason": type(exc).__name__, "record": record})
    return [latest[key][1] for key in sorted(latest)], rejects
