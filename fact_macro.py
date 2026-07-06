from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Create Macro Fact Table") \
            .config("spark.sql.catalog.nessie.ref", "backup") \
            .config("spark.sql.parquet.enableVectorizedReader", "false") \
            .config("spark.sql.iceberg.vectorization.enabled", "false") \
            .getOrCreate()

    """
    Load dimension tables and fact source data
    """
    dim_time = spark.read.format("iceberg").load("nessie.gold.dim_time")
    dim_industry = spark.read.format("iceberg").load("nessie.gold.dim_industry")
    dim_indicator = spark.read.format("iceberg").load("nessie.gold.dim_indicator")

    """
    indicator_code	indicator_name
    CPI	Chỉ số giá các mặt hàng quan trọng
    GDP_NOMINAL	GDP danh nghĩa theo quý
    GDP_REAL	GDP thực tế theo quý
    INFLATION_BY_INDUSTRY	Lạm phát theo ngành hàng        
    GDP_GROWTH_RATE	Tốc độ tăng trưởng GDP theo ngành
    GDP_REAL_GROWTH_RATE	Tốc độ tăng trưởng GPD thực tế  
    FDI_DISBURSED_VALUE	FDI giải ngân
    FDI_DISBURSED_YOY	FDI giải ngân (YoY)
    FDI_REGISTERED_VALUE	FDI đăng ký
    FDI_REGISTERED_YOY	FDI đăng ký (YoY)
    FDI_NEW_PLUS_CAP_VALUE	FDI cấp mới + tăng vốn
    FDI_NEW_PLUS_CAP_YOY	FDI cấp mới + tăng vốn (YoY)
    FDI_SHARE_PURCHASE_VALUE	FDI mua cổ phần
    FDI_SHARE_PURCHASE_YOY	FDI mua cổ phần (YoY)   
    """

    cpi = spark.read.format("iceberg").load("nessie.silver.chi_so_gia_cac_mat_hang_quan_trong") 
    gdp_nominal = spark.read.format("iceberg").load("nessie.silver.gdp_danh_nghia_theo_quy")        
    gdp_real = spark.read.format("iceberg").load("nessie.silver.gdp_thuc_te_theo_quy")
    inflation_by_industry = spark.read.format("iceberg").load("nessie.silver.lam_phat_theo_nganh_hang") 
    gdp_growth_rate = spark.read.format("iceberg").load("nessie.silver.toc_do_tang_truong_gdp_theo_nganh") 
    gdp_real_growth_rate = spark.read.format("iceberg").load("nessie.silver.toc_do_tang_truong_gdp_thuc_te")
    fdi = spark.read.format("iceberg").load("nessie.silver.tinh_hinh_fdi")

    """
    Fill indicator_code and normalize dimensions for union
    """
    cpi = cpi.withColumn("time_key", F.col("year") * 10000 + F.col("month") * 100 + F.lit(1))
    inflation_by_industry = inflation_by_industry.withColumn("time_key", F.col("year") * 10000 + F.col("month") * 100 + F.lit(1))
    fdi = fdi.withColumn("time_key", F.col("year") * 10000 + F.col("month") * 100 + F.lit(1))
    
    # Convert quarter to month (take first month of the quarter)
    def expand_quarter_to_months(df: F.DataFrame) -> F.DataFrame:
        start_month = F.col("quarter") * 3 - 2
        months = F.array(start_month, start_month + 1, start_month + 2)
        return (
                df.withColumn("month", F.explode(months))
                .withColumn("time_key", F.col("year") * 10000 + F.col("month") * 100 + F.lit(1))
        )

    gdp_nominal = expand_quarter_to_months(gdp_nominal)
    gdp_real = expand_quarter_to_months(gdp_real)
    gdp_growth_rate = expand_quarter_to_months(gdp_growth_rate)
    gdp_real_growth_rate = expand_quarter_to_months(gdp_real_growth_rate)

    cpi = cpi.withColumn("indicator_code", F.lit("CPI"))
    gdp_nominal = gdp_nominal.withColumn("indicator_code", F.lit("GDP_NOMINAL"))
    gdp_real = gdp_real.withColumn("indicator_code", F.lit("GDP_REAL"))
    inflation_by_industry = inflation_by_industry.withColumn("indicator_code", F.lit("INFLATION_BY_INDUSTRY"))
    gdp_growth_rate = gdp_growth_rate.withColumn("indicator_code", F.lit("GDP_GROWTH_RATE"))
    gdp_real_growth_rate = gdp_real_growth_rate.withColumn("indicator_code", F.lit("GDP_REAL_GROWTH_RATE"))

    # Fill missing industry with "Tất cả"
    fdi = fdi.withColumn("industry", F.lit("Tất cả"))

    """
    Divide FDI into multiple indicators
    """
    fdi_indicators = {
        'FDI_DISBURSED_VALUE': 'fdi_disbursed_value',
        'FDI_DISBURSED_YOY': 'fdi_disbursed_yoy',
        'FDI_REGISTERED_VALUE': 'fdi_registered_value',
        'FDI_REGISTERED_YOY': 'fdi_registered_yoy',
        'FDI_NEW_PLUS_CAP_VALUE': 'fdi_new_plus_capital_value',
        'FDI_NEW_PLUS_CAP_YOY': 'fdi_new_plus_capital_yoy',
        'FDI_SHARE_PURCHASE_VALUE': 'fdi_share_purchase_value',
        'FDI_SHARE_PURCHASE_YOY': 'fdi_share_purchase_yoy'
    }

    fdi_dfs = []
    for indicator_code, column_name in fdi_indicators.items():
        fdi_df = fdi.select("time_key", "industry", F.col(column_name).alias("value")) \
                    .withColumn("indicator_code", F.lit(indicator_code))
        fdi_dfs.append(fdi_df)


    cpi = cpi.select("time_key", "industry", "indicator_code", "value")

    """
    Union all macroeconomic indicators into a single fact table
    """
    fact_df = (cpi.unionByName(gdp_nominal.select(cpi.columns))
               .unionByName(gdp_real.select(cpi.columns))
               .unionByName(inflation_by_industry.select(cpi.columns))
               .unionByName(gdp_growth_rate.select(cpi.columns))
               .unionByName(gdp_real_growth_rate.select(cpi.columns))
              )
    
    for fdi_df in fdi_dfs:
        fact_df = fact_df.unionByName(fdi_df.select(cpi.columns))
    
    fact_df = fact_df.join(dim_time, "time_key", "left") \
                     .join(dim_industry, "industry", "left") \
                     .join(dim_indicator, "indicator_code", "left") \
                     .select("time_key", "industry_key", "indicator_key", "value")
    
    fact_df = fact_df.withColumn("fact_macro_id", F.expr("uuid()")) \
                     .select("fact_macro_id", "time_key", "industry_key", "indicator_key", "value")
    
    fact_df.show(20)
    
    # Write to Iceberg table        
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    fact_df.write.format("iceberg").mode("overwrite").saveAsTable(f"nessie.gold.fact_macro")