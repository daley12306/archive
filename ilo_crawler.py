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

def getFile(url, filename):
    client = create_minio_client()
    response = requests.get(url)
    if response.status_code == 200:
        # with open(f'./data/ilo/{filename}.json', 'wb') as file:
        #     file.write(response.content)
        buf = BytesIO(response.content)
        save_to_minio(client, "warehouse", f'bronze/ilo/{filename}.json', buf)
        print("JSON file downloaded successfully.")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

files = [
    ['https://rplumber.ilo.org/data/indicator/?id=EMP_DWAP_SEX_AGE_RT_A&ref_area=VNM&type=label&format=.json', 'ty_le_viec_lam_dan_so'], # Tỷ lệ việc làm / dân số
    ['https://rplumber.ilo.org/data/indicator/?id=EAP_DWAP_SEX_AGE_RT_A&ref_area=VNM&type=label&format=.json', 'luc_luong_tham_gia_lao_dong'], # Lực lượng tham gia lao động
    ['https://rplumber.ilo.org/data/indicator/?id=UNE_DEAP_SEX_AGE_RT_A&ref_area=VNM&type=label&format=.json', 'ty_le_that_nghiep'], # Tỷ lệ thất nghiệp
    ['https://rplumber.ilo.org/data/indicator/?id=EAR_4MMN_CUR_NB_A&ref_area=VNM&type=label&format=.json', 'thu_nhap_toi_thieu_thang'], # Thu nhập tối thiểu tháng
    ['https://rplumber.ilo.org/data/indicator/?id=HOW_TEMP_SEX_ECO_NB_A&ref_area=VNM&type=label&format=.json', 'thoi_gian_lam_viec_trung_binh'], # Thời gian làm việc trung bình
    ['https://rplumber.ilo.org/data/indicator/?id=EAR_4MTH_SEX_ECO_CUR_NB_A&ref_area=VNM&type=label&format=.json', 'thu_nhap_trung_binh_thang'], # Thu nhập trung bình tháng
    ['https://rplumber.ilo.org/data/indicator/?id=EAR_4MTH_SEX_ECO_CUR_NB_A&ref_area=VNM&type=label&format=.json', 'ty_le_viec_lam_phi_chinh_thuc'], # Tỷ lệ việc làm phi chính thức
    ['https://rplumber.ilo.org/data/indicator/?id=UNE_DEAP_SEX_AGE_RT_A&ref_area=VNM&type=label&format=.json', 'ty_le_that_nghiep_thanh_nien'], # Tỷ lệ thất nghiệp thanh niên
    ['https://rplumber.ilo.org/data/indicator/?id=EIP_NEET_SEX_RT_A&ref_area=VNM&type=label&format=.json', 'ty_le_neet'], # Tỷ lệ NEET
]

if __name__ == "__main__":
    for url, filename in files:
        getFile(url, filename)