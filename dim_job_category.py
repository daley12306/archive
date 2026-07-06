from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

CATEGORY_NAMES = [
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
    "HOẠT ĐỘNG CỦA CÁC TỔ CHỨC VÀ CƠ QUAN QUỐC TẾ",
]

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Create Job Category Dimension")
        .config("spark.sql.catalog.nessie.ref", "backup")
        .getOrCreate()
    )
    
    rows = [Row(category_name=name) for name in CATEGORY_NAMES]

    df = (
        spark.createDataFrame(rows)
        .withColumn("category_key", F.expr("uuid()"))
        .select("category_key", "category_name")
        .orderBy("category_name")
    )

    df.show(truncate=False)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    (
        df.write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable("nessie.gold.dim_job_category")
    )

    spark.stop()