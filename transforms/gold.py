"""UTC daily event counts and descriptive measurement statistics."""


def build_gold(rows):
    counts, measurements = {}, {}
    for row in rows:
        key = (
            row["observed_date"],
            row["source"],
            row["event_type"],
            row["location_id"],
        )
        counts[key] = counts.get(key, 0) + 1
        for metric in row["metrics"]:
            # Keep sensors and units separate; do not average unlike measurements.
            mkey = (
                *key,
                row["dimensions"].get("sensor_id"),
                metric["name"],
                metric["unit"],
            )
            value = metric["value"]
            stat = measurements.setdefault(
                mkey, {"count": 0, "sum": 0.0, "min": value, "max": value}
            )
            stat["count"] += 1
            stat["sum"] += value
            stat["min"], stat["max"] = min(stat["min"], value), max(stat["max"], value)
    events = [
        dict(
            zip(("date", "source", "event_type", "location_id"), key), event_count=count
        )
        for key, count in sorted(counts.items(), key=lambda x: str(x[0]))
    ]
    stats = []
    for key, stat in sorted(measurements.items(), key=lambda x: str(x[0])):
        # Circular variables need circular statistics, not an arithmetic mean.
        mean = None if key[-1] == "°" else stat["sum"] / stat["count"]
        stats.append(
            dict(
                zip(
                    (
                        "date",
                        "source",
                        "event_type",
                        "location_id",
                        "sensor_id",
                        "metric",
                        "unit",
                    ),
                    key,
                ),
                sample_count=stat["count"],
                minimum=stat["min"],
                maximum=stat["max"],
                sample_mean=mean,
            )
        )
    return events, stats
