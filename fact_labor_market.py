from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Create Labor Market Fact Table") \
            .config("spark.sql.catalog.nessie.ref", "backup") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.sql.iceberg.vectorization.enabled", "false") \
            .getOrCreate()
    
    """
    Load dimension tables and fact source data
    """
    dim_time = spark.read.format("iceberg").load("nessie.gold.dim_time")
    dim_demographic = spark.read.format("iceberg").load("nessie.gold.dim_demographic")
    dim_indicator = spark.read.format("iceberg").load("nessie.gold.dim_indicator")

    """
    indicator_code	indicator_name
    LFPR	Tỉ lệ tham gia lực lượng lao động
    UNEMPLOYMENT	Tỉ lệ thất nghiệp
    YOUTH_UNEMP	Tỉ lệ thất nghiệp thanh niên
    EMP_POP	Tỉ lệ có việc làm trên dân số
    INFORMAL_EMP	Tỉ lệ lao động phi chính thức
    AVG_HOURS	Số giờ làm việc trung bình mỗi tuần
    AVG_INCOME	Thu nhập trung bình hàng tháng
    MIN_WAGE	Mức lương tối thiểu hàng tháng
    """

    lfpr = spark.read.format("iceberg").load("nessie.silver.luc_luong_tham_gia_lao_dong")
    unemployment = spark.read.format("iceberg").load("nessie.silver.ty_le_that_nghiep")
    youth_unemp = spark.read.format("iceberg").load("nessie.silver.ty_le_that_nghiep_thanh_nien")
    emp_pop = spark.read.format("iceberg").load("nessie.silver.ty_le_viec_lam_dan_so")
    informal_emp = spark.read.format("iceberg").load("nessie.silver.ty_le_viec_lam_phi_chinh_thuc")
    avg_hours = spark.read.format("iceberg").load("nessie.silver.thoi_gian_lam_viec_trung_binh")
    avg_income = spark.read.format("iceberg").load("nessie.silver.thu_nhap_trung_binh_thang")
    min_wage = spark.read.format("iceberg").load("nessie.silver.thu_nhap_toi_thieu_thang")

    """ 
    Fill indicator_code and normalize dimensions for union
    """
    lfpr = lfpr.withColumn("indicator_code", F.lit("LFPR"))
    unemployment = unemployment.withColumn("indicator_code", F.lit("UNEMPLOYMENT"))
    youth_unemp = youth_unemp.withColumn("indicator_code", F.lit("YOUTH_UNEMP"))
    emp_pop = emp_pop.withColumn("indicator_code", F.lit("EMP_POP"))
    informal_emp = informal_emp.withColumn("indicator_code", F.lit("INFORMAL_EMP"))
    avg_hours = avg_hours.withColumn("indicator_code", F.lit("AVG_HOURS"))
    avg_income = avg_income.withColumn("indicator_code", F.lit("AVG_INCOME"))
    min_wage = min_wage.withColumn("indicator_code", F.lit("MIN_WAGE"))

    informal_emp = informal_emp.withColumn("age_group", F.lit("Total"))
    avg_hours = avg_hours.withColumn("age_group", F.lit("Total"))
    avg_income = avg_income.withColumn("age_group", F.lit("Total"))
    min_wage = (min_wage.withColumn("age_group", F.lit("Total"))
                .withColumn("sex", F.lit("Total")))
    
    # Default first day of year for time_key
    lfpr = lfpr.withColumn("time_key", F.col("year") * 10000 + F.lit(101))
    unemployment = unemployment.withColumn("time_key", F.col("year") * 10000 + F.lit(101))
    youth_unemp = youth_unemp.withColumn("time_key", F.col("year") * 10000 + F.lit(101))
    emp_pop = emp_pop.withColumn("time_key", F.col("year") * 10000 + F.lit(101))
    informal_emp = informal_emp.withColumn("time_key", F.col("year") * 10000 + F.lit(101))
    avg_hours = avg_hours.withColumn("time_key", F.col("year") * 10000 + F.lit(101))
    avg_income = avg_income.withColumn("time_key", F.col("year") * 10000 + F.lit(101))
    min_wage = min_wage.withColumn("time_key", F.col("year") * 10000 + F.lit(101))
    
    """
    Union Total fact dataframes
    Join with dimension tables to get surrogate keys
    Select and rename columns as needed
    """
    fact_df = (lfpr.unionByName(unemployment)
                .unionByName(youth_unemp)
                .unionByName(emp_pop)
                .unionByName(informal_emp)
                .unionByName(avg_hours)
                .unionByName(avg_income)
                .unionByName(min_wage))
    
    fact_df = fact_df.join(dim_time, "time_key", "left") \
                     .join(dim_demographic, ["sex", "age_group"], "left") \
                     .join(dim_indicator, "indicator_code", "left") \
                     .select("time_key", "demographic_key", "indicator_key", "value")
    
    fact_df = fact_df.withColumn("fact_labor_id", F.expr("uuid()")) \
                     .select("fact_labor_id", "time_key", "demographic_key", "indicator_key", "value")
    
    fact_df.show(20)

    # Write to Iceberg table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    fact_df.write.format("iceberg").mode("overwrite").saveAsTable(f"nessie.gold.fact_labor_market")