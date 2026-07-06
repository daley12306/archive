from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, quarter, year, month, day
from datetime import datetime

GOLD_NAMESPACE = "nessie.gold"

VN_PROVINCES = [
    "An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu",
    "Bắc Ninh", "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước",
    "Bình Thuận", "Cà Mau", "Cần Thơ", "Cao Bằng", "Đà Nẵng",
    "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp",
    "Gia Lai", "Hà Giang", "Hà Nam", "Hà Nội", "Hà Tĩnh",
    "Hải Dương", "Hải Phòng", "Hậu Giang", "Hòa Bình", "Hưng Yên",
    "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng",
    "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An",
    "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình",
    "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng",
    "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên", "Thanh Hóa",
    "Thừa Thiên Huế", "Tiền Giang", "Hồ Chí Minh", "Trà Vinh",
    "Tuyên Quang", "Vĩnh Long", "Vĩnh Phúc", "Yên Bái", "Khác"
]

VSIC_SECTORS = [
    "NÔNG NGHIỆP, LÂM NGHIỆP VÀ THỦY SẢN",
    "KHAI KHOÁNG",
    "CÔNG NGHIỆP CHẾ BIẾN, CHẾ TẠO",
    "SẢN XUẤT VÀ PHÂN PHỐI ĐIỆN, KHÍ ĐỐT, NƯỚC NÓNG, HƠI NƯỚC VÀ ĐIỀU HOÀ KHÔNG KHÍ",
    "CUNG CẤP NƯỚC; HOẠT ĐỘNG QUẢN LÝ VÀ XỬ LÝ RÁC THẢI, NƯỚC THẢI",
    "XÂY DỰNG",
    "BÁN BUÔN VÀ BÁN LẺ",
    "VẬN TẢI, KHO BÃI",
    "DỊCH VỤ LƯU TRÚ VÀ ĂN UỐNG",
    "HOẠT ĐỘNG XUẤT BẢN, PHÁT SÓNG, SẢN XUẤT VÀ PHÂN PHỐI NỘI DUNG",
    "HOẠT ĐỘNG VIỄN THÔNG; LẬP TRÌNH MÁY TÍNH, TƯ VẤN, CƠ SỞ HẠ TẦNG MÁY TÍNH VÀ CÁC DỊCH VỤ THÔNG TIN KHÁC",
    "HOẠT ĐỘNG TÀI CHÍNH, NGÂN HÀNG VÀ BẢO HIỂM",
    "HOẠT ĐỘNG KINH DOANH BẤT ĐỘNG SẢN",
    "HOẠT ĐỘNG CHUYÊN MÔN, KHOA HỌC VÀ CÔNG NGHỆ",
    "HOẠT ĐỘNG HÀNH CHÍNH VÀ DỊCH VỤ HỖ TRỢ",
    "HOẠT ĐỘNG CỦA ĐẢNG CỘNG SẢN, TỔ CHỨC CHÍNH TRỊ - XÃ HỘI, QUẢN LÝ NHÀ NƯỚC, AN NINH QUỐC PHÒNG; BẢO ĐẢM XÃ HỘI BẮT BUỘC",
    "GIÁO DỤC VÀ ĐÀO TẠO",
    "Y TẾ VÀ HOẠT ĐỘNG TRỢ GIÚP XÃ HỘI",
    "NGHỆ THUẬT, THỂ THAO VÀ GIẢI TRÍ",
    "HOẠT ĐỘNG DỊCH VỤ KHÁC",
    "HOẠT ĐỘNG LÀM THUÊ CÁC CÔNG VIỆC TRONG CÁC HỘ GIA ĐÌNH, SẢN XUẤT SẢN PHẨM VẬT CHẤT VÀ DỊCH VỤ TỰ TIÊU DÙNG CỦA HỘ GIA ĐÌNH",
    "HOẠT ĐỘNG CỦA CÁC TỔ CHỨC VÀ CƠ QUAN QUỐC TẾ"
]

# -----------------------------------------------------------------------------
# Mappings for level and education
# -----------------------------------------------------------------------------
LEVEL_MAPPING = {
    "executive":       (1, "Giám đốc và cấp cao hơn"),
    "senior_manager":  (2, "Trưởng/phó phòng"),
    "manager":         (3, "Quản lý"),
    "lead_supervisor": (4, "Trưởng nhóm / Giám sát"),
    "staff":           (5, "Chuyên viên / Nhân viên"),
    "intern":          (6, "Thực tập sinh"),
    "fresher":         (7, "Mới tốt nghiệp"),
}

EDUCATION_MAPPING = {
    "0":     (1, "Trung học / Không yêu cầu"),
    "1":     (2, "Trung cấp / Nghề"),
    "2":     (3, "Cao đẳng"),
    "3":     (4, "Đại học / Cử nhân"),
    "4":     (5, "Thạc sĩ"),
    "5":     (6, "Tiến sĩ"),
    "other": (7, "Khác"),
}

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------
def create_namespace_and_tables(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.dim_time (
            time_key INT, full_date DATE, day INT, day_name STRING,
            month INT, month_name STRING, quarter INT, year INT
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.dim_job (
            job_key STRING, title_clean STRING, description STRING,
            requirement STRING, link STRING
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.dim_location (
            location_key INT, province_name STRING
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.dim_company (
            company_key STRING, company_name STRING
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.dim_category (
            category_key STRING, category_name STRING
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.dim_level (
            level_key INT, level_code STRING, level_name_vn STRING
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.dim_education (
            education_key INT, education_code STRING, education_name_vn STRING
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.dim_skill (
            skill_key INT, skill_name STRING, skill_type STRING
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.dim_skill_alias (
            raw_skill STRING, skill_key INT
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.skill_embeddings (
            skill_key INT, skill_name STRING, skill_type STRING, embedding ARRAY<FLOAT>
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.fact_jobs (
            job_key STRING, processed_time_key INT, expired_time_key INT,
            location_key INT, company_key STRING, category_key STRING,
            level_key INT, education_key INT, platform STRING,
            work_form_standard STRING, salary_type STRING, currency STRING,
            experience_type STRING, min_salary BIGINT, max_salary BIGINT,
            min_years DOUBLE, max_years DOUBLE, quantity_normalized DOUBLE
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_NAMESPACE}.fact_job_skills (
            job_key STRING, skill_key INT, skill_type STRING
        ) USING iceberg
    """)

def populate_dim_time(spark):
    current_year = datetime.now().year
    start_date = "1990-01-01"
    end_date = f"{current_year + 10}-12-31"

    date_range = spark.sql(f"""
        SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date
    """)

    dim_time = date_range.withColumn("time_key", date_format("full_date", "yyyyMMdd").cast("int")) \
                        .withColumn("day", day("full_date")) \
                        .withColumn("day_name", date_format("full_date", "EEEE")) \
                        .withColumn("month", month("full_date")) \
                        .withColumn("month_name", date_format("full_date", "MMMM")) \
                        .withColumn("quarter", quarter("full_date")) \
                        .withColumn("year", year("full_date"))

    dim_time.write.mode("overwrite").saveAsTable(f"{GOLD_NAMESPACE}.dim_time")
    print(f"Inserted rows into dim_time from {start_date} to {end_date}")

def populate_static_dimensions(spark):
    # dim_location
    if spark.sql(f"SELECT COUNT(*) FROM {GOLD_NAMESPACE}.dim_location").collect()[0][0] == 0:
        rows = [(i, prov) for i, prov in enumerate(VN_PROVINCES, start=1)]
        spark.createDataFrame(rows, ["location_key", "province_name"]) \
             .write.mode("append").saveAsTable(f"{GOLD_NAMESPACE}.dim_location")
        print("Inserted dim_location rows.")
    else:
        print("dim_location already populated, skipping.")

    # dim_category
    if spark.sql(f"SELECT COUNT(*) FROM {GOLD_NAMESPACE}.dim_category").collect()[0][0] == 0:
        from pyspark.sql.functions import sha2, lit
        df_cat = spark.createDataFrame([(s,) for s in VSIC_SECTORS], ["category_name"])
        df_cat = df_cat.withColumn("category_key", sha2("category_name", 256))
        df_cat.select("category_key", "category_name") \
              .write.mode("append").saveAsTable(f"{GOLD_NAMESPACE}.dim_category")
        print("Inserted dim_category rows.")
    else:
        print("dim_category already populated, skipping.")

    # dim_level
    spark.sql(f"DELETE FROM {GOLD_NAMESPACE}.dim_level WHERE 1=1")
    rows = [
        (key_num, level_code, name)
        for level_code, (key_num, name) in LEVEL_MAPPING.items()
    ]
    spark.createDataFrame(rows, ["level_key", "level_code", "level_name_vn"]) \
         .write.mode("append").saveAsTable(f"{GOLD_NAMESPACE}.dim_level")
    print("Inserted dim_level rows.")

    # dim_education
    spark.sql(f"DELETE FROM {GOLD_NAMESPACE}.dim_education WHERE 1=1")
    rows = [
        (key_num, edu_code, name)
        for edu_code, (key_num, name) in EDUCATION_MAPPING.items()
    ]
    spark.createDataFrame(rows, ["education_key", "education_code", "education_name_vn"]) \
         .write.mode("append").saveAsTable(f"{GOLD_NAMESPACE}.dim_education")
    print("Inserted dim_education rows.")

def main():
    spark = SparkSession.builder \
        .appName("init_gold_tables") \
        .config("spark.sql.catalog.nessie.ref", "demo") \
        .getOrCreate()
    create_namespace_and_tables(spark)
    populate_dim_time(spark)
    populate_static_dimensions(spark)
    print("Gold layer initialization completed. Ready for daily incremental pipeline.")

if __name__ == "__main__":
    main()