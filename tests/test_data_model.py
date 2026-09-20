import unittest
from copy import deepcopy
from transforms.model import build_star_schema, validate_model
from transforms.silver import build_silver
from test_transforms import record, quake


def fixtures():
    return [
        quake(),
        record(
            "nasa_firms",
            {
                "acq_date": "2026-09-16",
                "acq_time": "1000",
                "latitude": "21",
                "longitude": "105",
                "satellite": "N",
                "frp": "4",
            },
        ),
        record(
            "open_meteo",
            {
                "latitude": 21,
                "longitude": 105,
                "utc_offset_seconds": 0,
                "current": {
                    "time": "2026-09-16T10:00",
                    "temperature_2m": 30,
                    "wind_speed_10m": 36,
                },
                "current_units": {"temperature_2m": "°C", "wind_speed_10m": "km/h"},
            },
            {"location_name": "ha_noi"},
        ),
        record(
            "openaq",
            {
                "period": {
                    "datetimeFrom": {"utc": "2026-09-16T10:00Z"},
                    "datetimeTo": {"utc": "2026-09-16T11:00Z"},
                },
                "value": 20,
            },
            {
                "location": {
                    "id": 1,
                    "coordinates": {"latitude": 21, "longitude": 105},
                },
                "sensor": {"id": 2, "parameter": {"name": "pm25", "units": "µg/m³"}},
            },
        ),
    ]


class ModelTests(unittest.TestCase):
    def model(self):
        rows, rejects = build_silver(fixtures())
        self.assertFalse(rejects)
        return build_star_schema(rows)

    def test_grains_and_foreign_keys_for_all_sources(self):
        model = self.model()
        for name in (
            "fact_earthquake",
            "fact_fire_detection",
            "fact_weather",
            "fact_air_quality",
        ):
            self.assertEqual(len(model[name]), 1)
        self.assertEqual(len(model["fact_weather_measurement"]), 2)
        self.assertEqual(len(model["dim_location"]), 4)
        self.assertEqual(len(model["dim_source"]), 4)
        self.assertEqual(
            model["fact_air_quality"][0]["period_end"],
            "2026-09-16T11:00:00.000000+00:00",
        )
        validate_model(model)

    def test_deterministic_keys_when_input_order_changes(self):
        rows, _ = build_silver(fixtures())
        self.assertEqual(
            build_star_schema(rows), build_star_schema(list(reversed(rows)))
        )

    def test_reject_broken_fk(self):
        model = self.model()
        model["fact_air_quality"][0]["sensor_key"] = "missing"
        with self.assertRaises(ValueError):
            validate_model(model)

    def test_reject_duplicate_fact_pk(self):
        model = self.model()
        model["fact_earthquake"].append(deepcopy(model["fact_earthquake"][0]))
        with self.assertRaises(ValueError):
            validate_model(model)

    def test_sensor_location_changes_have_distinct_versions(self):
        first = fixtures()[-1]
        second = deepcopy(first)
        second["event"]["event_id"] += "-next-measurement"
        second["event"]["context"]["location"]["coordinates"]["latitude"] = 22
        second["kafka"]["offset"] = 1
        rows, _ = build_silver([first, second])
        model = build_star_schema(rows)
        self.assertEqual(len(model["dim_sensor"]), 2)
        self.assertEqual(len({f["sensor_key"] for f in model["fact_air_quality"]}), 2)


if __name__ == "__main__":
    unittest.main()
