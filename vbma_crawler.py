import requests
from minio import Minio
from minio.error import S3Error
from io import BytesIO

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_REGION = "us-east-1"

def create_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
        region=MINIO_REGION,
    )

def save_to_minio(client, bucket_name, object_name, buf: BytesIO):
    buf.seek(0)
    size = buf.getbuffer().nbytes
    try:
        client.make_bucket(bucket_name, location=MINIO_REGION)
    except S3Error as e:
        if e.code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
    client.put_object(bucket_name, object_name, buf, size, content_type="application/octet-stream")

def getFile(url):
    client = create_minio_client()
    response = requests.get(url, verify=False)
    filename = url.split('?')[0].rstrip('/').split('/')[-1]
    if response.status_code == 200:
        buf = BytesIO(response.content)
        save_to_minio(client, "warehouse", f'bronze/vbma/{filename}', buf)
        print("CSV file downloaded successfully.")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

lst = [
    'https://vbma.org.vn/csv/markets/tables/vi/lam_phat_theo_nganh_hang.csv',
    'https://vbma.org.vn/csv/markets/tables/vi/gdp_danh_nghia_theo_quy.csv',
    'https://vbma.org.vn/csv/markets/tables/vi/gdp_thuc_te_theo_quy.csv',
    'https://vbma.org.vn/csv/markets/tables/vi/tinh_hinh_fdi.csv',
    'https://vbma.org.vn/csv/markets/charts/vi/chi_so_gia_cac_mat_hang_quan_trong.csv?t=1762139967',
    'https://vbma.org.vn/csv/markets/charts/vi/toc_do_tang_truong_gdp_theo_nganh.csv?t=1762703953',
    'https://vbma.org.vn/csv/markets/tables/vi/toc_do_tang_truong_gdp_thuc_te.csv'
]

if __name__ == "__main__":
    for url in lst:
        getFile(url)