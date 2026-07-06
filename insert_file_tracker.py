from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse
import json

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Insert File Tracker")
        .config("spark.sql.catalog.nessie.ref", "backup")
        .getOrCreate()
    )

    # parse param
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", required=True)
    args = parser.parse_args()

    paths = json.loads(args.paths)

    # handle case chỉ có 1 string
    if isinstance(paths, str):
        paths = [paths]

    if not paths:
        print("No paths to insert")
        spark.stop()
        exit(0)

    print(f"Received paths: {paths}")

    # create dataframe
    # df = spark.createDataFrame([(p,) for p in paths], ["file_path"])

    # df = (
    #     df
    #     .withColumn("platform", F.split(F.col("file_path"), "/").getItem(-2))
    #     .withColumn("status", F.lit("pending"))
    #     .withColumn("created_at", F.current_timestamp())
    #     .withColumn("updated_at", F.current_timestamp())
    # )

    # # (
    # #     df.write
    # #     .format("iceberg")
    # #     .mode("append")
    # #     .saveAsTable("nessie.meta.file_tracker")
    # # )

    df = spark.table("nessie.meta.file_tracker")
    df.show(truncate=False)

    print(f"Inserted {len(paths)} records into file_tracker")

    spark.stop()