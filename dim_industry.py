from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Create Industry Dimension") \
            .config("spark.sql.catalog.nessie.ref", "backup") \
            .getOrCreate()
    
    cpi_df = spark.read.format("iceberg").load("nessie.silver.chi_so_gia_cac_mat_hang_quan_trong")
    gdp_nominal_df = spark.read.format("iceberg").load("nessie.silver.gdp_danh_nghia_theo_quy")
    gdp_real_df = spark.read.format("iceberg").load("nessie.silver.gdp_thuc_te_theo_quy")
    inflation_by_industry_df = spark.read.format("iceberg").load("nessie.silver.lam_phat_theo_nganh_hang")
    gdp_growth_rate_df = spark.read.format("iceberg").load("nessie.silver.toc_do_tang_truong_gdp_theo_nganh")
    gdp_real_growth_rate_df = spark.read.format("iceberg").load("nessie.silver.toc_do_tang_truong_gdp_thuc_te")

    industries = (cpi_df.select("industry")
                  .union(gdp_nominal_df.select("industry"))
                  .union(gdp_real_df.select("industry"))
                  .union(inflation_by_industry_df.select("industry"))
                  .union(gdp_growth_rate_df.select("industry"))
                  .union(gdp_real_growth_rate_df.select("industry"))
                  .distinct())
    
    industries = industries.union(spark.createDataFrame([("Tất cả",)], ["industry"]))
    
    dim_industry_df = (industries.withColumn("industry_key", F.expr("uuid()"))
                       .select("industry_key", "industry"))
    dim_industry_df = dim_industry_df.sort("industry")
    dim_industry_df.show(40, truncate=False)
    
    # Write to Iceberg table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    dim_industry_df.write.format("iceberg").mode("overwrite").saveAsTable(f"nessie.gold.dim_industry")
