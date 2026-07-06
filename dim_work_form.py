from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Create Work Form Dimension")
        # giống style của các dim khác
        .config("spark.sql.catalog.nessie.ref", "backup")
        .getOrCreate()
    )

    work_forms = [
        ("full_time",  "Toàn thời gian"),
        ("part_time",  "Bán thời gian / làm thêm"),
        ("internship", "Thực tập"),
        ("other",      "Hình thức khác"),
    ]

    rows = [
        Row(
            work_form_code=code,
            work_form_name=name,
        )
        for (code, name) in work_forms
    ]

    dim_work_form_df = (
        spark.createDataFrame(rows)
        .withColumn("work_form_key", F.expr("uuid()"))
        .select(
            "work_form_key",
            "work_form_code",
            "work_form_name",
        )
    )

    dim_work_form_df.show(truncate=False)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    (
        dim_work_form_df.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("nessie.gold.dim_work_form")
    )

    spark.stop()
