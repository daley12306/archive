from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Create Education Dimension")
        .config("spark.sql.catalog.nessie.ref", "backup")
        .getOrCreate()
    )

    education_levels = [
        ("0",      "Không yêu cầu", 0),
        ("1",      "Trung cấp/Nghề", 1),
        ("2",      "Cao đẳng", 2),
        ("3",      "Đại học", 3),
        ("4",      "Thạc sĩ", 4),
        ("5",      "Tiến sĩ", 5),
        ("other",  "Khác", -1),
    ]

    rows = [
        Row(
            education_code=code,
            education_name=name,
            education_rank=rank,
        )
        for (code, name, rank) in education_levels
    ]

    dim_education_df = (
        spark.createDataFrame(rows)
        .withColumn("education_key", F.expr("uuid()"))
        .select(
            "education_key",
            "education_code",
            "education_name",
            "education_rank",
        )
    )

    dim_education_df.show(truncate=False)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    (
        dim_education_df.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("nessie.gold.dim_education")
    )

    spark.stop()
