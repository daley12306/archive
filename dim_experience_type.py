from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Create Experience Type Dimension")
        .config("spark.sql.catalog.nessie.ref", "backup")
        .getOrCreate()
    )

    experience_types = [
        ("none",      "Không yêu cầu kinh nghiệm"),
        ("single",    "Số năm kinh nghiệm"),
        ("range",     "Khoảng kinh nghiệm"),
        ("at_least",  "Ít nhất X năm kinh nghiệm"),
        ("upto",      "Tối đa X năm kinh nghiệm"),
    ]

    rows = [
        Row(experience_type_code=code, experience_type_name=name)
        for code, name in experience_types
    ]

    dim_experience_type_df = (
        spark.createDataFrame(rows)
        .withColumn("experience_type_key", F.expr("uuid()"))
        .select("experience_type_key", "experience_type_code", "experience_type_name")
    )

    dim_experience_type_df.show(truncate=False)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    (
        dim_experience_type_df
        .write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("nessie.gold.dim_experience_type")
    )