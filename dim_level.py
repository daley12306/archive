from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Create Level Dimension")
        # giữ style giống các dim khác
        .config("spark.sql.catalog.nessie.ref", "backup")
        .getOrCreate()
    )

    level_definitions = [
        ("intern",         "Thực tập sinh",                          0),
        ("fresher",        "Mới tốt nghiệp",                         1),
        ("staff",          "Nhân viên",                              2),
        ("lead_supervisor","Trưởng nhóm / Giám sát",                 3),
        ("manager",        "Quản lý",                                4),
        ("senior_manager", "Trưởng / Phó phòng",                     5),
        ("executive",      "Giám đốc / Cấp điều hành cao hơn",       6),
    ]

    rows = [
        Row(
            level_code=code,
            level_name=name,
            level_rank=rank,
        )
        for (code, name, rank) in level_definitions
    ]

    dim_level_df = (
        spark.createDataFrame(rows)
        .withColumn("level_key", F.expr("uuid()"))
        .select(
            "level_key",
            "level_code",
            "level_name",
            "level_rank",
        )
    )

    dim_level_df.show(truncate=False)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    (
        dim_level_df.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("nessie.gold.dim_level")
    )

    spark.stop()
