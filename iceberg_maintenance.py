from pyspark.sql import SparkSession
from datetime import datetime, timedelta

TABLES_TO_MAINTAIN = [
    "nessie.silver.jobs",
    "nessie.gold.fact_jobs",
    "nessie.gold.fact_job_skills",
    "nessie.gold.dim_skill",
    "nessie.gold.dim_skill_alias",
    "nessie.gold.dim_job",
    "nessie.gold.dim_company",
]

# Giữ lại snapshot trong 7 ngày gần nhất
RETAIN_CUTOFF = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")

def main():
    spark = SparkSession.builder \
        .appName("iceberg_maintenance") \
        .config("spark.sql.catalog.nessie.ref", "demo") \
        .getOrCreate()

    for table in TABLES_TO_MAINTAIN:
        print(f"\n{'='*50}")
        print(f"Maintaining: {table}")

        try:
            count = spark.sql(f"SELECT COUNT(*) FROM {table}").collect()[0][0]
            print(f"  Row count: {count:,}")
        except Exception as e:
            print(f"  Table not found or error, skipping: {e}")
            continue

        # Bước 1: Đổi sang Merge-on-Read để UPDATE/DELETE nhẹ hơn
        try:
            spark.sql(f"""
                ALTER TABLE {table}
                SET TBLPROPERTIES (
                    'write.update.mode' = 'merge-on-read',
                    'write.delete.mode' = 'merge-on-read',
                    'write.merge.mode'  = 'merge-on-read'
                )
            """)
            print(f"  MOR mode set.")
        except Exception as e:
            print(f"  MOR set failed (may already be set): {e}")

        # Bước 2: Expire snapshots cũ
        try:
            result = spark.sql(f"""
                CALL nessie.system.expire_snapshots(
                    table      => '{table}',
                    older_than => TIMESTAMP '{RETAIN_CUTOFF}',
                    retain_last => 3
                )
            """)
            row = result.collect()[0]
            print(f"  Expired: {row['deleted_data_files_count']} data files, "
                  f"{row['deleted_manifest_files_count']} manifest files")
        except Exception as e:
            print(f"  expire_snapshots failed: {e}")

        # Bước 3: Compact small files
        try:
            result = spark.sql(f"""
                CALL nessie.system.rewrite_data_files(
                    table   => '{table}',
                    options => map('target-file-size-bytes', '134217728')
                )
            """)
            row = result.collect()[0]
            print(f"  Compacted: {row['rewritten_data_files_count']} → "
                  f"{row['added_data_files_count']} files")
        except Exception as e:
            print(f"  rewrite_data_files failed: {e}")

    print("\nMaintenance complete.")
    spark.stop()

if __name__ == "__main__":
    main()