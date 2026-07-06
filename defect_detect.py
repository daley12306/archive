from minio import Minio
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
import io                          # FIX 1: thêm import io còn thiếu
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Kết nối ─────────────────────────────────────────────────────────────────
client = Minio(
    "minio:9000",
    access_key="admin",
    secret_key="password",
    secure=False
)

BUCKET = "warehouse"
PREFIX = "bronze/"


def list_parquet_files(bucket: str, prefix: str) -> list[str]:
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    keys = [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]
    print(f"Tìm thấy {len(keys)} file parquet")
    return keys


def check_parquet_file(key: str) -> dict:
    result = {"key": key, "status": "ok", "error": None,
              "num_rows": None, "num_cols": None, "size_bytes": None, "schema": None}
    try:
        stat = client.stat_object(BUCKET, key)
        result["size_bytes"] = stat.size

        if stat.size == 0:
            result["status"] = "empty"
            result["error"] = "File 0 byte"
            return result

        response = client.get_object(BUCKET, key)
        file_bytes = response.read()
        response.close()
        response.release_conn()

        buf = BytesIO(file_bytes)
        pf = pq.ParquetFile(buf)
        result["num_rows"] = pf.metadata.num_rows
        result["num_cols"] = pf.metadata.num_columns
        result["schema"] = pf.schema_arrow.to_string()
        pf.read_row_group(0)

    except pa.lib.ArrowInvalid as e:
        result["status"] = "corrupt"
        result["error"] = f"ArrowInvalid: {e}"
    except Exception as e:
        result["status"] = "corrupt"
        result["error"] = str(e)

    return result


def scan_all_files(keys: list[str], max_workers: int = 8) -> pd.DataFrame:
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(check_parquet_file, k): k for k in keys}
        for i, future in enumerate(as_completed(futures), 1):
            res = future.result()
            results.append(res)
            status_icon = "✅" if res["status"] == "ok" else "❌"
            print(f"[{i}/{len(keys)}] {status_icon} {res['key']} — {res['status']}")
    return pd.DataFrame(results)


def find_schema_mismatches(df: pd.DataFrame) -> pd.DataFrame:
    ok_files = df[df["status"] == "ok"].copy()
    if ok_files.empty:
        return ok_files
    majority_schema = ok_files["schema"].value_counts().index[0]
    mismatch = ok_files[ok_files["schema"] != majority_schema].copy()
    mismatch["status"] = "schema_mismatch"
    mismatch["error"] = "Schema khác với majority"
    return mismatch


def save_csv_to_minio(df: pd.DataFrame, filename: str):
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    size = buf.getbuffer().nbytes
    client.put_object(
        BUCKET, f"reports/{filename}",
        data=buf, length=size,
        content_type="text/csv"
    )
    print(f"Đã lưu lên MinIO: s3://{BUCKET}/reports/{filename}")


# ── Main ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    keys = list_parquet_files(BUCKET, PREFIX)
    report_df = scan_all_files(keys)

    mismatch_df = find_schema_mismatches(report_df)
    if not mismatch_df.empty:
        report_df.loc[mismatch_df.index, "status"] = "schema_mismatch"
        report_df.loc[mismatch_df.index, "error"] = "Schema khác majority"

    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    print(report_df["status"].value_counts().to_string())

    bad_files = report_df[report_df["status"] != "ok"]
    print(f"\nTổng file lỗi: {len(bad_files)} / {len(report_df)}")

    if not bad_files.empty:
        print("\nDanh sách file lỗi:")
        print(bad_files[["key", "status", "error", "size_bytes"]].to_string(index=False))


    # save_csv_to_minio(report_df, "parquet_scan_report.csv")
    # save_csv_to_minio(bad_files, "parquet_bad_files.csv")
    # print(f"Đã lưu report lên MinIO: s3://{BUCKET}/reports/parquet_scan_report.csv")
    # print(f"Đã lưu file lỗi lên MinIO: s3://{BUCKET}/reports/parquet_bad_files.csv")