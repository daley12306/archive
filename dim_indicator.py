from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Create Indicator Dimension") \
            .config("spark.sql.catalog.nessie.ref", "backup") \
            .getOrCreate()

    indicators = [
        # code,                         name,                               
        ("LFPR",                        "Tỉ lệ tham gia lực lượng lao động"),
        ("UNEMPLOYMENT",                "Tỉ lệ thất nghiệp"),
        ("YOUTH_UNEMP",                 "Tỉ lệ thất nghiệp thanh niên"),
        ("EMP_POP",                     "Tỉ lệ có việc làm trên dân số"),
        ("INFORMAL_EMP",                "Tỉ lệ lao động phi chính thức"),
        ("AVG_HOURS",                   "Số giờ làm việc trung bình mỗi tuần"),
        ("AVG_INCOME",                  "Thu nhập trung bình hàng tháng"),
        ("MIN_WAGE",                    "Mức lương tối thiểu hàng tháng"),
        ("CPI",                         "Chỉ số giá các mặt hàng quan trọng"),
        ("GDP_NOMINAL",                 "GDP danh nghĩa theo quý"),
        ("GDP_REAL",                    "GDP thực tế theo quý"),
        ("INFLATION_BY_INDUSTRY",       "Lạm phát theo ngành hàng"),
        ("FDI",                         "Tình hình FDI"),
        ("GDP_GROWTH_RATE",             "Tốc độ tăng trưởng GDP theo ngành"),
        ("GDP_REAL_GROWTH_RATE",        "Tốc độ tăng trưởng GPD thực tế"),
        ("FDI_DISBURSED_VALUE",         "FDI giải ngân"),
        ("FDI_DISBURSED_YOY",           "FDI giải ngân (YoY)"),
        ("FDI_REGISTERED_VALUE",        "FDI đăng ký"),
        ("FDI_REGISTERED_YOY",          "FDI đăng ký (YoY)"),
        ("FDI_NEW_PLUS_CAP_VALUE",      "FDI cấp mới + tăng vốn"),
        ("FDI_NEW_PLUS_CAP_YOY",        "FDI cấp mới + tăng vốn (YoY)"),
        ("FDI_SHARE_PURCHASE_VALUE",    "FDI mua cổ phần"),
        ("FDI_SHARE_PURCHASE_YOY",      "FDI mua cổ phần (YoY)"),
    ]

    rows = [Row(indicator_code=c, indicator_name=n)
            for (c, n) in indicators]

    dim_indicator_df = spark.createDataFrame(rows).withColumn("indicator_key", F.expr("uuid()"))
    dim_indicator_df = dim_indicator_df.select("indicator_key", "indicator_code", "indicator_name")

    dim_indicator_df.show(20)

    # Write to Iceberg table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    dim_indicator_df.write.format("iceberg").mode("overwrite").saveAsTable(f"nessie.gold.dim_indicator")
