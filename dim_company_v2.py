from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Create Company Dimension")
        .config("spark.sql.catalog.nessie.ref", "test")
        .getOrCreate()
    )

    print("Append new companies to dim_company...")

    # Check dim_company is existed or not, if not create it
    spark.sql("""CREATE TABLE IF NOT EXISTS nessie.gold.dim_company (
        company_key STRING,
        company_name STRING
    ) USING iceberg""")
    
    df_jobs = spark.read.table("nessie.silver.jobs")
    df_company = spark.read.table("nessie.gold.dim_company")

    # Left anti join để lấy các công ty mới chưa có trong dim_company
    df_jobs = df_jobs.join(df_company, df_jobs["company_clean"] == df_company["company_name"], "left_anti")

    df_new = (
        df_jobs
        .select(F.col("company_clean").alias("company_name"))
        .filter(F.col("company_name").isNotNull() & (F.col("company_name") != ""))
        .dropDuplicates(["company_name"])
        .withColumn("company_key", F.sha2(F.col("company_name"), 256))
        .select("company_key", "company_name")
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    df_new.write.format("iceberg").mode("append").saveAsTable("nessie.gold.dim_company")