from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession.builder \
            .appName("Create Demographic Dimension") \
            .config("spark.sql.catalog.nessie.ref", "backup") \
            .getOrCreate()
    
    unemployment_df = spark.read.format("iceberg").load("nessie.silver.ty_le_that_nghiep")
    sex_list = [row.sex for row in unemployment_df.select("sex").distinct().collect()]
    age_list = [row.age_group for row in unemployment_df.select("age_group").distinct().collect()]
    
    # Tạo cartesian product trên driver
    data = [(sex, age) for sex in sex_list for age in age_list]
    
    demographic_df = spark.createDataFrame(data, ["sex", "age_group"])
    demographic_df = demographic_df.sort("sex", "age_group")
    demographic_df = demographic_df.withColumn("demographic_key", F.expr("uuid()"))
    demographic_df = demographic_df.select("demographic_key", "sex", "age_group")
    demographic_df.show(20)

    # Write to Iceberg table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    demographic_df.write.format("iceberg").mode("overwrite").saveAsTable(f"nessie.gold.dim_demographic")
