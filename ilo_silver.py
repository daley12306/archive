from pyspark.sql import SparkSession
import json
import pandas as pd
import fsspec
from pathlib import Path

def normalize(df):
    # Drop unnecessary columns
    cols_to_drop = [
        'ref_area.label',
        'source.label',
        'indicator.label',
        'obs_status.label',
        'note_classif.label',
        'note_indicator.label',
        'note_source.label',
    ]
    df = df.drop(columns=cols_to_drop, errors='ignore')

    # Rename columns
    rename_map = {
        'sex.label': 'sex',
        'classif2.label': 'currency',
        'time': 'year',
        'obs_value': 'value'
    }
    df = df.rename(columns=rename_map)

    # Identify and rename classif1.label based on its content
    if 'classif1.label' in df.columns:
        if df['classif1.label'].str.contains('Economic').any():
            df = df.rename(columns={'classif1.label': 'economic_activity'})
        elif df['classif1.label'].str.contains('Age').any():
            df = df.rename(columns={'classif1.label': 'age_group'})
        elif df['classif1.label'].str.contains('Currency').any():
            df = df.rename(columns={'classif1.label': 'currency'})

    # Handle Economic activity instead of age_group
    if 'economic_activity' in df.columns:
        df = df[df['economic_activity'].str.contains('Aggregate')]
        df['economic_activity'] = df['economic_activity'].apply(lambda x: x.split(' ')[-1].strip())
        df = df[df['economic_activity'] == 'Total']
        df = df.drop(columns=['economic_activity'])

    # Filter age_group to keep only 'Aggregate' entries and extract age group
    if 'age_group' in df.columns:
        df = df[df['age_group'].str.contains('Aggregate')]
        df['age_group'] = df['age_group'].apply(lambda x: x.split(' ')[-1].strip())
    
    # Filter currency to keep only 'Local' entries
    if 'currency' in df.columns:
        df = df[df['currency'].str.contains('Local')]
        df = df.drop(columns=['currency'])

    # Cast 
    df['year'] = df['year'].astype(int)
    df['value'] = df['value'].astype(float)

    # Drop NA values in value column
    df = df.dropna(subset=['value'])

    # Reset index
    df = df.reset_index(drop=True)

    return df

def write_iceberg(spark, df, filename):
    df_spark = spark.createDataFrame(df)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    df_spark.write.format("iceberg").mode("overwrite").saveAsTable(f"nessie.silver.{filename}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ILO Silver").config("spark.sql.catalog.nessie.ref", "backup").getOrCreate()
    fs = fsspec.filesystem(
        "s3",
        key="admin",
        secret="password",
        client_kwargs={"endpoint_url": "http://minio:9000"}
    )

    fs.invalidate_cache()

    for file in fs.glob("warehouse/bronze/ilo/*.json"):
        print(f"\nLoading file: {file}")
        filename = Path(file).stem

        with fs.open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        df = pd.DataFrame(data)
        df = normalize(df)
        write_iceberg(spark, df, filename)
        print(df.head(5))

    spark.stop()