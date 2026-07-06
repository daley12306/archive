import pandas as pd
from pyspark.sql import SparkSession
import fsspec
from pathlib import Path

def to_long(df, path):
    df = df.rename(columns={df.columns[0]: 'industry'})
    if df.columns[-1].startswith("Unnamed"):
        df = df.iloc[:, :-1]
    if 'lam_phat' in path:
        df = df.drop(index=[0,1]).reset_index(drop=True)
    elif 'gdp' in path:
        df = df.drop(index=[0]).reset_index(drop=True)
    df = df.dropna(subset=["industry"])
    df = df.melt(id_vars="industry", var_name="period", value_name="value")
    df['value'] = (df['value']
               .astype(str)
               .str.replace(',', '', regex=False)
               .astype(float))
    df["industry"] = df["industry"].astype(str).str.strip().str.strip('"')
    tmp = df["period"].str.split(" ", n=1, expand=True)
    df['year'] = tmp[1].astype(int)
    if(tmp[0].str.contains('T').all()):
        df['month'] = tmp[0].str.replace('T', '', regex=False).astype(int)
        df["period"] = pd.PeriodIndex(year=df["year"], month=df["month"], freq="M")
        df["quarter"] = df["period"].dt.quarter
        df = df[['industry', 'quarter', 'month', 'year', 'value']].sort_values(by=['year', 'month']).reset_index(drop=True)
    else:
        df['quarter'] = tmp[0].str.replace('Q', '', regex=False).astype(int)
        df = df[['industry', 'quarter', 'year', 'value']].sort_values(by=['year', 'quarter']).reset_index(drop=True)
    return df

def fdi_to_long(df):
    df.columns = ['_'.join(col).strip().replace("\n", " ") for col in df.columns.values]
    rename_map = {
        df.columns[0]: "period",
        df.columns[1]: "fdi_disbursed_value",
        df.columns[2]: "fdi_disbursed_yoy",
        df.columns[3]: "fdi_registered_value",
        df.columns[4]: "fdi_registered_yoy",
        df.columns[5]: "fdi_new_plus_capital_value",
        df.columns[6]: "fdi_new_plus_capital_yoy",
        df.columns[7]: "fdi_share_purchase_value",
        df.columns[8]: "fdi_share_purchase_yoy"
    }
    df = df.rename(columns=rename_map)
    df = df.dropna(subset=["period"]).reset_index(drop=True)
    for col in df.columns:
        if col != "period":
            df[col] = (df[col]
                       .astype(str)
                       .str.replace(",", "", regex=False)
                       .str.replace('"', '', regex=False)
                       .replace("-", None)
                       .astype(float))
    tmp = df["period"].str.split(" ", n=1, expand=True)
    df['year'] = tmp[1].astype(int)
    df['month'] = tmp[0].str.replace('T', '', regex=False).astype(int)
    df["period"] = pd.PeriodIndex(year=df["year"], month=df["month"], freq="M")
    df["quarter"] = df["period"].dt.quarter
    df = df[['quarter', 'month', 'year', 'fdi_disbursed_value', 'fdi_disbursed_yoy',
             'fdi_registered_value', 'fdi_registered_yoy', 'fdi_new_plus_capital_value',
             'fdi_new_plus_capital_yoy', 'fdi_share_purchase_value', 'fdi_share_purchase_yoy']]
    return df

def write_iceberg(spark, df, filename):
    df_spark = spark.createDataFrame(df)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    df_spark.write.format("iceberg").mode("overwrite").saveAsTable(f"nessie.silver.{filename}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("VBMA Silver").config("spark.sql.catalog.nessie.ref", "backup").getOrCreate()
    fs = fsspec.filesystem(
        "s3",
        key="admin",
        secret="password",
        client_kwargs={"endpoint_url": "http://minio:9000"}
    )

    fs.invalidate_cache()

    for file in fs.glob("warehouse/bronze/vbma/*.csv"):
        with fs.open(file) as f:
            print(file)
            filename = Path(file).stem
            if 'fdi' in file:
                df = pd.read_csv(f, encoding='utf-16', sep="\t", header=[0,1])
                df = fdi_to_long(df)
                write_iceberg(spark, df, filename)
            else:
                df = pd.read_csv(f, encoding='utf-16', sep="\t")
                df = to_long(df, file)
                write_iceberg(spark, df, filename)
            print(df)