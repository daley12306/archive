from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Create Currency Dimension")
        .config("spark.sql.catalog.nessie.ref", "backup")
        .getOrCreate()
    )

    currencies = [
        # code, name
        ("VND", "Việt Nam Đồng"),
        ("USD", "Đô la Mỹ"),
    ]

    rows = [
        Row(
            currency_code=code,
            currency_name=name,
        )
        for (code, name) in currencies
    ]

    dim_currency_df = (
        spark.createDataFrame(rows)
        .withColumn("currency_key", F.expr("uuid()"))
        .select(
            "currency_key",
            "currency_code",
            "currency_name",
        )
    )

    dim_currency_df.show(truncate=False)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    (
        dim_currency_df.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("nessie.gold.dim_currency")
    )

    spark.stop()
