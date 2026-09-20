"""Public entry points for the Spark Bronze → Silver → Gold pipeline."""

import json


def bronze_to_silver(storage):
    from transforms.spark.jobs import bronze_to_silver as run_silver
    from transforms.spark.session import spark_session

    with spark_session("environment-bronze-to-silver") as spark:
        return run_silver(spark, storage)


def silver_to_gold(storage, manifest_key):
    from transforms.spark.jobs import silver_to_gold as run_gold
    from transforms.spark.session import spark_session

    with spark_session("environment-silver-to-gold") as spark:
        return run_gold(spark, storage, manifest_key)


def main():
    from transforms.spark.jobs import bronze_to_silver, silver_to_gold
    from transforms.spark.session import spark_session
    from transforms.storage import LakeStorage

    storage = LakeStorage()
    with spark_session() as spark:
        manifest_key = bronze_to_silver(spark, storage)
        result = silver_to_gold(spark, storage, manifest_key)
    print(
        json.dumps(
            {
                "snapshot_id": result["snapshot_id"],
                "silver_count": result["silver_count"],
                "reject_count": result["reject_count"],
            }
        )
    )


if __name__ == "__main__":
    main()
