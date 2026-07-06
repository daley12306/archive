from minio import Minio
from minio.error import S3Error
from io import BytesIO
import requests
import time
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

def get_categories():
    categories_url = 'https://ms.vietnamworks.com/meta/v1.2/job-functions'
    response = requests.get(categories_url)
    response_data = response.json()['data']
    categories = []
    for data in response_data:
        category_name = data['attributes']['nameVi']
        category_id = data['id']
        categories.append((category_name, category_id))
    return categories

def try_request(func, max_retries=3):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"Attempt {i + 1} failed: {e}")
            time.sleep(5)
    return None

def get_jobs(page=0, category_id=1):
    api = 'https://ms.vietnamworks.com/job-search/v1.0/search'

    today = datetime.now().strftime('%Y-%m-%d')
    payload = {
            "userId": 0,
            "query": "",
            "filter": [
                {
                    "field": "jobFunction",
                    "value": f'[{{"parentId":{category_id},"childrenIds":[-1]}}]'
                },
                {
                    "field": "approvedOn",
                    "value": today
                }
            ],
            "ranges": [],
            "order": [
                {
                    "field": "approvedOn",
                    "value": "desc"
                }
            ],
            
            # "hitsPerPage": 100,
            "page": page,
            "summaryVersion": ""
        }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    response = requests.post(api, json=payload, headers=headers)
    return response.json()['data']

def get_degrees():
    degree_api = 'https://ms.vietnamworks.com/meta/v1.0/highest-degrees'
    degree_response = requests.get(degree_api)
    degree_data = degree_response.json()['data']['relationships']['data']
    return {item['id']: item['attributes']['nameVi'] for item in degree_data}

def get_work_types():
    work_types_api = 'https://ms.vietnamworks.com/meta/v1.0/type-workings'
    work_types_response = requests.get(work_types_api)
    work_types_data = work_types_response.json()['data']['relationships']['data']
    return {item['id']: item['attributes']['nameVi'] for item in work_types_data}

def get_jobs_info(data, degree, work_types, category_name):
    jobs = []

    for job in data:
        title = job.get('jobTitle')
        salary = job.get('prettySalary')
        
        # location - luôn trả về list
        location = [location.get('address') for location in job.get('workingLocations', [])] if job.get('workingLocations') else []
        
        experience = job.get('yearsOfExperience')
        expired_date = job.get('expiredOn')
        company = job.get('companyName')
        
        # job description + requirement - handle None
        job_desc = job.get('jobDescription') or ''
        job_req = job.get('jobRequirement') or ''
        job_description = 'Mô tả công việc' + '\n' + job_desc + '\n' + 'Yêu cầu công việc' + '\n' + job_req
        
        level = job.get('jobLevelVI')
        education = degree.get(job.get('highestDegreeId'))
        quantity = job.get('numberOfRecruits')
        work_form = work_types.get(job.get('typeWorkingId'))

        # skills - luôn trả về list
        skills = [skill.get('skillName') for skill in job.get('skills', [])] if job.get('skills') else []
        
        link = job.get('jobUrl')
        category = category_name
        
        # industry - handle safely
        industry = None
        if job.get('industriesV3') and len(job.get('industriesV3')) > 0:
            industry = job.get('industriesV3')[0].get('industryV3NameVI')

        jobs.append([
            title, salary, location, experience, expired_date, company,
            job_description, level, education, quantity, work_form, skills, link, category, industry
        ])

    return pd.DataFrame(jobs, columns=[
        "title", "salary", "location", "experience", "expired_date", "company",
        "job_description", "level", "education", "quantity", "work_form", "skills", "link", "category", "industry"
    ])

if __name__ == "__main__":
    categories = get_categories()
    degree = get_degrees()  
    work_types = get_work_types()
    jobs_df = pd.DataFrame()
    for category_name, category_id in categories:
        jobs_list = get_jobs(page=0, category_id=category_id)
        jobs_df = pd.concat([jobs_df, get_jobs_info(jobs_list, degree, work_types, category_name)], ignore_index=True)
        time.sleep(2)

    # Define schema and convert columns
    jobs_df = jobs_df.astype({
        "title": "string",
        "salary": "string",
        "location": "object",
        "experience": "string",
        "expired_date": "string",
        "company": "string",
        "job_description": "string",
        "level": "string",
        "education": "string",
        "quantity": "Int64",
        "work_form": "string",
        "skills": "object",
        "link": "string",
        "category": "string",
        "industry": "string",
    })

    if(jobs_df.empty):
        print("No job data crawled.")
        exit(0)

    buf = BytesIO()
    jobs_df.to_parquet(buf, index=False)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"bronze/vietnamworks/vietnamworks_{ts}.parquet"

    client = create_minio_client()
    save_to_minio(client, "warehouse", object_name, buf)
    print(f"Uploaded to MinIO: s3://warehouse/{object_name}")
    print(f"s3://warehouse/{object_name}")