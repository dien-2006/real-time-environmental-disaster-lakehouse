"""Gold star schema, deterministic keys and executable relationship checks."""

from datetime import date
from common.events import digest

SOURCE_FACT = {
    "usgs": "fact_earthquake",
    "nasa_firms": "fact_fire_detection",
    "open_meteo": "fact_weather",
    "openaq": "fact_air_quality",
}
DIMENSIONS = {
    "dim_source": "source_key",
    "dim_date": "date_key",
    "dim_location": "location_key",
    "dim_sensor": "sensor_key",
    "dim_parameter": "parameter_key",
}


def key(kind, *values):
    return kind + ":" + digest(values)


def build_star_schema(rows):
    dims = {name: {} for name in DIMENSIONS}
    facts = {name: [] for name in SOURCE_FACT.values()}
    facts["fact_weather_measurement"] = []
    # Stable order also determines latest Type-1 dimension labels in each snapshot.
    rows = sorted(
        rows,
        key=lambda r: (
            r["source_updated_at"],
            r["ingested_at"],
            r["event_id"],
            r["payload_hash"],
        ),
    )
    for row in rows:
        source = row["source"]
        source_key = key("source", source)
        dims["dim_source"][source_key] = {
            "source_key": source_key,
            "source_code": source,
        }
        day = date.fromisoformat(row["observed_date"])
        date_key = int(day.strftime("%Y%m%d"))
        dims["dim_date"][date_key] = {
            "date_key": date_key,
            "date": day.isoformat(),
            "year": day.year,
            "quarter": (day.month - 1) // 3 + 1,
            "month": day.month,
            "day": day.day,
            "iso_weekday": day.isoweekday(),
        }
        # Source-scoped location version: never infer equivalence across providers.
        location_key = key(
            "location", source, row["location_id"], row["latitude"], row["longitude"]
        )
        dims["dim_location"][location_key] = {
            "location_key": location_key,
            "source_key": source_key,
            "source_location_id": row["location_id"],
            "location_name": row["location_name"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
        }
        fact_key = key("event", source, row["event_id"])
        base = {
            "event_key": fact_key,
            "event_id": row["event_id"],
            "source_key": source_key,
            "date_key": date_key,
            "location_key": location_key,
            "observed_at": row["observed_at"],
            "ingested_at": row["ingested_at"],
            "source_updated_at": row["source_updated_at"],
            "payload_hash": row["payload_hash"],
            "kafka": row["kafka"],
        }
        metrics = {}
        for m in row["metrics"]:
            pk = key("parameter", source, m["name"], m["unit"])
            dims["dim_parameter"][pk] = {
                "parameter_key": pk,
                "source_key": source_key,
                "parameter_name": m["name"],
                "unit": m["unit"],
            }
            if m["name"] in metrics:
                raise ValueError("Duplicate metric name in a Silver event")
            metrics[m["name"]] = (m["value"], pk)
        if source == "usgs":
            magnitude, magnitude_key = metrics.get("magnitude", (None, None))
            depth, depth_key = metrics.get("depth", (None, None))
            facts["fact_earthquake"].append(
                {
                    **base,
                    "magnitude": magnitude,
                    "magnitude_parameter_key": magnitude_key,
                    "depth_km": depth,
                    "depth_parameter_key": depth_key,
                    "status": row["dimensions"].get("status"),
                }
            )
        elif source == "nasa_firms":
            frp, frp_key = metrics.get("fire_radiative_power", (None, None))
            facts["fact_fire_detection"].append(
                {
                    **base,
                    "frp_mw": frp,
                    "frp_parameter_key": frp_key,
                    **{
                        n: row["dimensions"].get(n)
                        for n in ("satellite", "instrument", "confidence")
                    },
                }
            )
        elif source == "open_meteo":
            facts["fact_weather"].append(base)
            for value, pk in metrics.values():
                facts["fact_weather_measurement"].append(
                    {
                        "measurement_key": key("weather_value", fact_key, pk),
                        "event_key": fact_key,
                        "parameter_key": pk,
                        "value": value,
                    }
                )
        elif source == "openaq":
            if len(metrics) != 1:
                raise ValueError("Air quality fact requires exactly one measurement")
            value, pk = next(iter(metrics.values()))
            sensor_id = row["dimensions"]["sensor_id"]
            if sensor_id is None:
                raise ValueError("Sensor id missing")
            # Version by location and measured parameter so sensor moves do not rewrite older facts.
            sensor_key = key("sensor", source, sensor_id, location_key, pk)
            dims["dim_sensor"][sensor_key] = {
                "sensor_key": sensor_key,
                "source_key": source_key,
                "source_sensor_id": sensor_id,
                "location_key": location_key,
                "parameter_key": pk,
            }
            facts["fact_air_quality"].append(
                {
                    **base,
                    "sensor_key": sensor_key,
                    "parameter_key": pk,
                    "value": value,
                    "period_end": row.get("period_end"),
                }
            )
    tables = {
        name: sorted(values.values(), key=lambda r: str(r[DIMENSIONS[name]]))
        for name, values in dims.items()
    }
    tables.update(facts)
    validate_model(tables)
    return tables


def validate_model(tables):
    primary = {
        **DIMENSIONS,
        **{name: "event_key" for name in SOURCE_FACT.values()},
        "fact_weather_measurement": "measurement_key",
    }
    indexes = {}
    for table, pk in primary.items():
        values = [r[pk] for r in tables[table]]
        if None in values or len(values) != len(set(values)):
            raise ValueError(f"Invalid primary key: {table}.{pk}")
        indexes[table] = set(values)
    references = {
        "source_key": "dim_source",
        "date_key": "dim_date",
        "location_key": "dim_location",
        "sensor_key": "dim_sensor",
        "parameter_key": "dim_parameter",
        "magnitude_parameter_key": "dim_parameter",
        "depth_parameter_key": "dim_parameter",
        "frp_parameter_key": "dim_parameter",
    }
    for table, records in tables.items():
        for row in records:
            for column, target in references.items():
                if (
                    column in row
                    and row[column] is not None
                    and row[column] not in indexes[target]
                ):
                    raise ValueError(f"Broken foreign key: {table}.{column}")
            if table in SOURCE_FACT.values():
                for column in ("source_key", "date_key", "location_key"):
                    if row.get(column) is None:
                        raise ValueError(f"Missing fact dimension: {table}.{column}")
            if (
                table == "fact_weather_measurement"
                and row["event_key"] not in indexes["fact_weather"]
            ):
                raise ValueError("Weather measurement has no parent event")
