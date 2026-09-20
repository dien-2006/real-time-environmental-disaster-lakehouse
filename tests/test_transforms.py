import unittest
from copy import deepcopy
from common.events import envelope
from transforms.silver import normalize, build_silver
from transforms.gold import build_gold


def record(source, raw, context=None, offset=0, identity="one"):
    event = envelope(source, "test", identity, raw, context=context)
    event["ingested_at"] = "2026-09-16T12:00:00Z"
    return {
        "event": event,
        "kafka": {"topic": source, "partition": 0, "offset": offset},
    }


def quake(offset=0, mag=3, updated=2000):
    return record(
        "usgs",
        {
            "properties": {
                "time": 1000,
                "updated": updated,
                "mag": mag,
                "magType": "mw",
            },
            "geometry": {"coordinates": [105, 21, 10]},
        },
        offset=offset,
    )


class TransformTests(unittest.TestCase):
    def test_usgs_milliseconds_and_latest_revision(self):
        rows, rejects = build_silver(
            [
                quake(),
                quake(),
                quake(1, mag=4, updated=3000),
                quake(2, mag=2, updated=1000),
            ]
        )
        self.assertEqual(len(rows), 1)
        self.assertFalse(rejects)
        self.assertEqual(rows[0]["observed_at"], "1970-01-01T00:00:01.000000+00:00")
        self.assertEqual(rows[0]["metrics"][0]["value"], 4)

    def test_meteo_utc_day_and_units(self):
        r = record(
            "open_meteo",
            {
                "latitude": 21,
                "longitude": 105,
                "utc_offset_seconds": 25200,
                "current": {"time": "2026-09-16T01:00", "wind_speed_10m": 36},
                "current_units": {"wind_speed_10m": "km/h"},
            },
            {"location_name": "ha_noi"},
        )
        row = normalize(r)
        self.assertEqual(row["observed_date"], "2026-09-15")
        self.assertEqual(
            row["metrics"][0], {"name": "wind_speed_10m", "value": 10, "unit": "m/s"}
        )

    def test_nasa_padded_time(self):
        r = record(
            "nasa_firms",
            {
                "acq_date": "2026-09-16",
                "acq_time": "35",
                "latitude": "21",
                "longitude": "105",
                "frp": "2.5",
            },
        )
        row = normalize(r)
        self.assertEqual(row["observed_at"], "2026-09-16T00:35:00.000000+00:00")
        self.assertEqual(row["metrics"][0]["value"], 2.5)

    def test_openaq_sensor_and_unit_preserved(self):
        r = record(
            "openaq",
            {"period": {"datetimeFrom": {"utc": "2026-09-16T10:00Z"}}, "value": 15},
            {
                "location": {"id": 1},
                "sensor": {"id": 2, "parameter": {"name": "pm25", "units": "µg/m³"}},
            },
        )
        row = normalize(r)
        self.assertEqual(row["dimensions"]["sensor_id"], 2)
        self.assertEqual(row["metrics"][0]["unit"], "µg/m³")

    def test_bad_data_quarantined_without_changing_bronze(self):
        r = quake()
        r["event"]["raw"]["geometry"]["coordinates"][1] = 200
        original = deepcopy(r)
        rows, rejects = build_silver([r, {"event": None}, 123])
        self.assertFalse(rows)
        self.assertEqual(len(rejects), 3)
        self.assertEqual(r, original)

    def test_gold_does_not_double_count_replayed_events(self):
        rows, _ = build_silver([quake(), quake(1)])
        counts, stats = build_gold(rows)
        self.assertEqual(counts[0]["event_count"], 1)
        self.assertTrue(all(r["sample_count"] == 1 for r in stats))


if __name__ == "__main__":
    unittest.main()
