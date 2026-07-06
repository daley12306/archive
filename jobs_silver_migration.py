import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

# Giữ đúng branch + warehouse như job silver hiện tại
# NESSIE_BRANCH = "etl/bronze-to-silver-jobs-data"
NESSIE_BRANCH = "backup"
SILVER_TABLE = "nessie.silver.jobs"


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("jobs_silver_migration")
        .config("spark.sql.catalog.nessie.ref", NESSIE_BRANCH)
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse")
        .getOrCreate()
    )
    return spark


def main():
    spark = build_spark_session()

    # 1) Đọc bảng silver hiện tại
    try:
        df = spark.read.table(SILVER_TABLE)
    except AnalysisException:
        print(f"[ERROR] Không tìm thấy bảng {SILVER_TABLE} – hãy chắc chắn bảng đã được tạo trước.")
        spark.stop()
        sys.exit(1)

    print(f"[INFO] Số bản ghi ban đầu trong {SILVER_TABLE}: {df.count()}")

    # 2) Drop các hàng có title_clean null hoặc rỗng
    df = df.filter(
        F.col("title_clean").isNotNull() & (F.col("title_clean") != "")
    )

    print(f"[INFO] Sau khi drop title_clean null/rỗng: {df.count()}")

    # 3) Salary: với các job có salary_type = 'negotiable' => currency = 'VND'
    df = df.withColumn(
        "currency",
        F.when(F.col("salary_type") == "negotiable", F.lit("VND"))
         .otherwise(F.col("currency"))
    )

    # 4) Experience:
    #    - min_years, max_years null => 0.0
    #    - nếu min_years = 0 và max_years = 0 => experience_type = 'khong_yeu_cau'
    df = (
        df
        .withColumn(
            "min_years",
            F.when(F.col("min_years") == 'NaN', F.lit(0.0)).otherwise(F.col("min_years"))
        )
        .withColumn(
            "max_years",
            F.when(F.col("max_years") == 'NaN', F.lit(0.0)).otherwise(F.col("max_years"))
        )
    )

    # 5) Education: education_standard null => '0'
    df = df.withColumn(
        "education_standard",
        F.coalesce(F.col("education_standard"), F.lit("0"))
    )

    # 6) Chuẩn bị dedupe các job giống hệt nhau nhưng khác link tracking
    #    → cùng mọi thứ (platform, title_clean, location, company, salary, exp, sector, category, skills…)
    #
    #    Ta tạo một job_key dựa trên gần như toàn bộ cột nội dung (trừ link).
    #    Với skills_all (ArrayType) thì sort rồi join để không phụ thuộc thứ tự phần tử.
    #    Sau đó chọn 1 bản ghi / job_key (ví dụ link nhỏ nhất theo thứ tự chữ cái).

    # Nếu bảng của bạn chưa có cột này thì bỏ ra khỏi danh sách hoặc thêm try/except
    job_key_cols = [
        "platform",
        "title_clean",
        "location_clean",
        "company_clean",
        "min_salary",
        "max_salary",
        "currency",
        "salary_type",
        "min_years",
        "max_years",
        "experience_type",
        "education_standard",
        "work_form_standard",
        "quantity_normalized",
        "expired_date_norm",
        "industry_sector",
        "category_name_final",
    ]

    # Nếu bảng không có skills_all, bạn có thể bỏ đoạn này và không dùng trong job_key
    if "skills_all" in df.columns:
        df = df.withColumn(
            "skills_str_for_key",
            F.concat_ws("||", F.sort_array("skills_all"))
        )
        extra_for_key = [F.col("skills_str_for_key")]
    else:
        extra_for_key = []

    # Tạo job_key (hash) từ các cột nội dung
    job_key_exprs = [F.col(c).cast("string") for c in job_key_cols] + extra_for_key

    df = df.withColumn(
        "job_key",
        F.sha2(F.concat_ws("||", *job_key_exprs), 256)
    )

    # Window để chọn 1 bản ghi đại diện cho mỗi job_key
    # Ở đây chọn bản ghi có link nhỏ nhất theo thứ tự chữ cái (ổn định, deterministic)
    w_job = Window.partitionBy("job_key").orderBy(F.col("link").asc_nulls_last())

    df = (
        df
        .withColumn("rn_job", F.row_number().over(w_job))
        .filter(F.col("rn_job") == 1)
        .drop("rn_job", "job_key", "skills_str_for_key")
    )

    print(f"[INFO] Sau khi dedupe theo job_key: {df.count()}")

    # 7) Ghi đè lại bảng silver
    (
        df.write
          .format("iceberg")
          .mode("overwrite")
          .saveAsTable(SILVER_TABLE)
    )

    print(f"[INFO] Migration hoàn tất. Bảng {SILVER_TABLE} đã được overwrite với dữ liệu đã làm sạch.")

    spark.stop()


if __name__ == "__main__":
    main()
