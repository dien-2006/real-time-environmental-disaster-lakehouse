"""Star schema and aggregates built using Spark projections, joins and groupBy."""

from pyspark.sql import Window, functions as F
from pyspark.sql.types import StringType

from transforms.model import DIMENSIONS, SOURCE_FACT, key

# Small deterministic scalar UDF preserves the established v2 key algorithm.
# No dataset is collected to Python; sorting, joins and aggregations remain in Spark.
model_key = F.udf(key, StringType())


def keyed(kind, *columns):
    return model_key(F.lit(kind), *[F.col(c) for c in columns])


def build_tables(silver):
    base = (
        silver.withColumn("source_key", keyed("source", "source"))
        .withColumn("event_key", keyed("event", "source", "event_id"))
        .withColumn(
            "location_key",
            keyed("location", "source", "location_id", "latitude", "longitude"),
        )
        .withColumn("date_key", F.date_format("observed_date", "yyyyMMdd").cast("int"))
    )
    rank = Window.partitionBy("location_key").orderBy(
        F.col("source_updated_at").desc(),
        F.col("ingested_at").desc(),
        F.col("event_id").desc(),
        F.col("payload_hash").desc(),
    )
    tables = {
        "dim_source": base.select(
            "source_key", F.col("source").alias("source_code")
        ).distinct(),
        "dim_date": base.select(
            "date_key",
            F.col("observed_date").alias("date"),
            F.year("observed_date").alias("year"),
            F.quarter("observed_date").alias("quarter"),
            F.month("observed_date").alias("month"),
            F.dayofmonth("observed_date").alias("day"),
            (F.pmod(F.dayofweek("observed_date") + 5, F.lit(7)) + 1).alias(
                "iso_weekday"
            ),
        ).distinct(),
        "dim_location": base.withColumn("_rank", F.row_number().over(rank))
        .filter("_rank = 1")
        .select(
            "location_key",
            "source_key",
            F.col("location_id").alias("source_location_id"),
            "location_name",
            "latitude",
            "longitude",
        ),
    }
    metrics = base.select("*", F.explode("metrics").alias("metric")).withColumn(
        "parameter_key",
        model_key(F.lit("parameter"), "source", "metric.name", "metric.unit"),
    )
    tables["dim_parameter"] = metrics.select(
        "parameter_key",
        "source_key",
        F.col("metric.name").alias("parameter_name"),
        F.col("metric.unit").alias("unit"),
    ).distinct()
    fact_columns = [
        "event_key",
        "event_id",
        "source_key",
        "date_key",
        "location_key",
        "observed_at",
        "ingested_at",
        "source_updated_at",
        "payload_hash",
        "kafka",
    ]
    measurements = F.map_from_entries(
        F.transform("metrics", lambda m: F.struct(m["name"], m))
    )
    enriched = base.withColumn("_measurements", measurements)

    def measure(name):
        return F.element_at("_measurements", F.lit(name))

    def parameter(name):
        m = measure(name)
        return F.when(
            m.isNotNull(), model_key(F.lit("parameter"), "source", m["name"], m["unit"])
        )

    tables["fact_earthquake"] = enriched.filter("source = 'usgs'").select(
        *fact_columns,
        measure("magnitude")["value"].alias("magnitude"),
        parameter("magnitude").alias("magnitude_parameter_key"),
        measure("depth")["value"].alias("depth_km"),
        parameter("depth").alias("depth_parameter_key"),
        F.col("dimensions.status").alias("status"),
    )
    tables["fact_fire_detection"] = enriched.filter("source = 'nasa_firms'").select(
        *fact_columns,
        measure("fire_radiative_power")["value"].alias("frp_mw"),
        parameter("fire_radiative_power").alias("frp_parameter_key"),
        *[
            F.col(f"dimensions.{c}").alias(c)
            for c in ("satellite", "instrument", "confidence")
        ],
    )
    tables["fact_weather"] = base.filter("source = 'open_meteo'").select(*fact_columns)
    tables["fact_weather_measurement"] = metrics.filter("source = 'open_meteo'").select(
        model_key(F.lit("weather_value"), "event_key", "parameter_key").alias(
            "measurement_key"
        ),
        "event_key",
        "parameter_key",
        F.col("metric.value").alias("value"),
    )
    air = metrics.filter("source = 'openaq'").withColumn(
        "sensor_key",
        model_key(
            F.lit("sensor"),
            "source",
            "dimensions.sensor_id",
            "location_key",
            "parameter_key",
        ),
    )
    tables["dim_sensor"] = air.select(
        "sensor_key",
        "source_key",
        F.col("dimensions.sensor_id").alias("source_sensor_id"),
        "location_key",
        "parameter_key",
    ).distinct()
    tables["fact_air_quality"] = air.select(
        *fact_columns,
        "sensor_key",
        "parameter_key",
        F.col("metric.value").alias("value"),
        "period_end",
    )
    groups = ["observed_date", "source", "event_type", "location_id"]
    tables["daily_events"] = (
        base.groupBy(*groups)
        .agg(F.count("*").alias("event_count"))
        .withColumnRenamed("observed_date", "date")
    )
    tables["daily_metrics"] = (
        metrics.select(
            *groups,
            F.col("dimensions.sensor_id").alias("sensor_id"),
            F.col("metric.name").alias("metric"),
            F.col("metric.unit").alias("unit"),
            F.col("metric.value").alias("value"),
        )
        .groupBy(*groups, "sensor_id", "metric", "unit")
        .agg(
            F.count("*").alias("sample_count"),
            F.min("value").alias("minimum"),
            F.max("value").alias("maximum"),
            F.avg("value").alias("sample_mean"),
        )
        .withColumn("sample_mean", F.when(F.col("unit") != "°", F.col("sample_mean")))
        .withColumnRenamed("observed_date", "date")
    )
    return tables


def validate_tables(tables):
    primary_keys = {
        **DIMENSIONS,
        **{table: "event_key" for table in SOURCE_FACT.values()},
        "fact_weather_measurement": "measurement_key",
    }
    references = {
        "source_key": ("dim_source", "source_key"),
        "date_key": ("dim_date", "date_key"),
        "location_key": ("dim_location", "location_key"),
        "sensor_key": ("dim_sensor", "sensor_key"),
        "parameter_key": ("dim_parameter", "parameter_key"),
        "magnitude_parameter_key": ("dim_parameter", "parameter_key"),
        "depth_parameter_key": ("dim_parameter", "parameter_key"),
        "frp_parameter_key": ("dim_parameter", "parameter_key"),
    }
    for name, primary in primary_keys.items():
        frame = tables[name]
        if frame.filter(F.col(primary).isNull()).limit(1).count():
            raise ValueError(f"Missing primary key: {name}")
        if frame.groupBy(primary).count().filter("count > 1").limit(1).count():
            raise ValueError(f"Duplicate primary key: {name}")
        if name in SOURCE_FACT.values():
            missing_dimension = (
                F.col("source_key").isNull()
                | F.col("date_key").isNull()
                | F.col("location_key").isNull()
            )
            if frame.filter(missing_dimension).limit(1).count():
                raise ValueError(f"Missing fact dimension: {name}")
        for column, (target, target_key) in references.items():
            if column not in frame.columns or column == primary:
                continue
            values = (
                frame.filter(F.col(column).isNotNull())
                .select(F.col(column).alias("_key"))
                .distinct()
            )
            missing = values.join(
                tables[target].select(F.col(target_key).alias("_key")),
                "_key",
                "left_anti",
            )
            if missing.limit(1).count():
                raise ValueError(f"Broken foreign key: {name}.{column}")
    missing_weather = tables["fact_weather_measurement"].join(
        tables["fact_weather"].select("event_key"),
        "event_key",
        "left_anti",
    )
    if missing_weather.limit(1).count():
        raise ValueError("Weather measurement missing parent")
