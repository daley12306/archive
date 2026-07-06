"""
Fix 1 corrupted parquet file using Spark
Test with: careerviet_20260211_010331.parquet
"""

from pyspark.sql import SparkSession
import sys

def fix_parquet():
    spark = SparkSession.builder \
        .appName("fix_single_parquet") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    # Use S3 paths for Spark (works in container)
    # Original corrupted file
    corrupted_path = "s3a://warehouse/bronze/careerviet/careerviet_20260211_010331.parquet"
    
    # Backup path
    backup_path = "s3a://warehouse/bronze/.backup/careerviet_20260211_010331.parquet"
    
    # Fixed path
    fixed_path = "s3a://warehouse/bronze/careerviet/careerviet_20260211_010331_FIXED.parquet"
    
    print("="*80)
    print("FIX SINGLE CORRUPTED PARQUET FILE")
    print("="*80)
    print(f"\n1. Reading corrupted file: {corrupted_path}")
    
    try:
        # Read corrupted parquet (Spark is tolerant)
        df = spark.read.parquet(corrupted_path)
        print(f"   ✓ Successfully read!")
        row_count = df.count()
        col_count = len(df.columns)
        print(f"   Rows: {row_count:,}")
        print(f"   Columns: {col_count}")
        print(f"   Schema: {', '.join(df.schema.fieldNames())}")
        
    except Exception as e:
        print(f"   ✗ Failed to read: {str(e)[:100]}")
        spark.stop()
        return
    
    print(f"\n2. Creating backup: {backup_path}")
    try:
        df.coalesce(1).write.mode("overwrite").parquet(backup_path)
        print(f"   ✓ Backup created!")
    except Exception as e:
        print(f"   ✗ Backup failed: {str(e)[:100]}")
    
    print(f"\n3. Writing fixed file: {fixed_path}")
    try:
        df.coalesce(1).write.mode("overwrite").parquet(fixed_path)
        print(f"   ✓ Fixed file created!")
    except Exception as e:
        print(f"   ✗ Failed to write fixed: {str(e)[:100]}")
        spark.stop()
        return
    
    print(f"\n4. Verifying fixed file...")
    try:
        df_verify = spark.read.parquet(fixed_path)
        print(f"   ✓ Fixed file is readable!")
        print(f"   Rows: {df_verify.count():,}")
        
        # Compare row counts
        original_rows = df.count()
        fixed_rows = df_verify.count()
        if original_rows == fixed_rows:
            print(f"   ✓ All {original_rows:,} rows preserved!")
        else:
            print(f"   ⚠ Row count mismatch: {original_rows} → {fixed_rows}")
    except Exception as e:
        print(f"   ✗ Verification failed: {str(e)[:100]}")
    
    print("\n" + "="*80)
    print("RESULT: Check if fixed file can be read properly")
    print("="*80 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    fix_parquet()
