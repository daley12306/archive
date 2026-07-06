from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Create Salary Type Dimension")
        # giống style các dim khác
        .config("spark.sql.catalog.nessie.ref", "backup")
        .getOrCreate()
    )

    salary_types = [
        # code,        name
        ("range",      "Khoảng lương (min - max)"),
        ("single",     "Mức lương cố định"),
        ("at_least",   "Từ mức này trở lên"),
        ("upto",       "Lên tới / tối đa mức này"),
        ("negotiable", "Lương thương lượng"),
    ]

    rows = [
        Row(
            salary_type_code=code,
            salary_type_name=name,
        )
        for (code, name) in salary_types
    ]

    dim_salary_type_df = (
        spark.createDataFrame(rows)
        .withColumn("salary_type_key", F.expr("uuid()"))
        .select(
            "salary_type_key",
            "salary_type_code",
            "salary_type_name",
        )
    )

    dim_salary_type_df.show(truncate=False)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    (
        dim_salary_type_df.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("nessie.gold.dim_salary_type")
    )

    spark.stop()
