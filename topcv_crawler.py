from minio import Minio
from minio.error import S3Error
from io import BytesIO
import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from datetime import datetime
import tempfile
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari/537.36"}
REQ_TIMEOUT = 15
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
    """
    Upload parquet file to MinIO with robust error handling
    Strategy: Write to temp file first → verify → upload → cleanup
    This prevents incomplete uploads and corrupted parquet files
    """
    buf.seek(0)
    size = buf.getbuffer().nbytes
    
    # Create bucket if not exists
    try:
        client.make_bucket(bucket_name, location=MINIO_REGION)
    except S3Error as e:
        if e.code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
    
    # Write to temporary file first for safety
    temp_file = None
    try:
        # Write buffer to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
            temp_file = tmp.name
            tmp.write(buf.getvalue())
            tmp.flush()
        
        # Verify file was written correctly
        if not os.path.exists(temp_file):
            raise RuntimeError("Failed to create temp file")
        
        file_size = os.path.getsize(temp_file)
        if file_size != size:
            raise RuntimeError(f"Temp file size mismatch: {file_size} vs {size}")
        
        # Upload to MinIO with multipart for large files
        with open(temp_file, 'rb') as f:
            client.put_object(
                bucket_name,
                object_name,
                f,
                length=file_size,
                content_type="application/octet-stream",
                part_size=5*1024*1024  # 5MB parts for multipart upload
            )
        
        print(f"✓ Successfully uploaded to MinIO: {object_name}")
        
    finally:
        # Always cleanup temp file
        if temp_file and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except Exception as e:
                print(f"Warning: Could not delete temp file {temp_file}: {e}")

def get_category():
    base_url = "https://www.topcv.vn"
    r = requests.get(base_url, headers=UA, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    nav_links = soup.find_all("div", class_="navigation-left__item-sub-menu-wrapper navigation-left__item-sub-menu-right")
    if not nav_links:
        raise RuntimeError("Menu not found on TopCV")
    links = nav_links[0].find_all("a", class_="sub-menu__item")
    categories = [
        (' '.join(link.get('title', '').split()[2:]),
         link['href'] if link.get('href', '').startswith('https') else base_url + link.get('href', ''))
        for link in links if 'viec-lam' in link.get('href', '')
    ]
    return categories

def get_job(link):
    r = requests.get(link, headers=UA, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    job_cards = soup.find_all("div", class_="job-item-search-result") or soup.find_all("div", class_="job-item-default")
    if not job_cards:
        return [], True
    job_cards = [card for card in job_cards if card.find("label", class_="address")]
    has_today = any(card.find("label", class_="address").get_text(strip=True) == "Đăng hôm nay" for card in job_cards)
    stop_flag = not has_today
    job_links = []
    for card in job_cards:
        a = card.find("a")
        if not a or not a.get("href"):
            continue
        href = a["href"]
        if "brand" in href:
            continue
        job_links.append(href)
    return job_links, stop_flag

def get_job_info(link, category):
    r = requests.get(link, headers=UA, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    title = soup.find("h1", class_="job-detail__info--title").get_text(strip=True)
    job_detail = soup.find_all("div", class_="job-detail__info--section-content-value")
    salary = job_detail[0].get_text(strip=True) if len(job_detail) > 0 else None
    location = job_detail[1].get_text(strip=True) if len(job_detail) > 1 else None
    experience = job_detail[2].get_text(strip=True) if len(job_detail) > 2 else None
    expired = soup.find("div", class_="job-detail__info--deadline")
    expired_date = expired.get_text(strip=True) if expired else None
    company_tag = soup.find("a", class_="name")
    company = company_tag.get_text(strip=True) if company_tag else None
    industry = soup.find("div", class_="company-field")
    industry = industry.find("div", class_="company-value").get_text(strip=True) if industry else None
    desc_tag = soup.find("div", class_="job-description")
    job_description = desc_tag.get_text(strip=True) if desc_tag else None
    general = soup.find_all("div", class_="box-general-group-info-value")
    level = general[0].get_text(strip=True) if len(general) > 0 else None
    education = general[1].get_text(strip=True) if len(general) > 1 else None
    quantity = general[2].get_text(strip=True) if len(general) > 2 else None
    work_form = general[3].get_text(strip=True) if len(general) > 3 else None
    skills_blk = soup.find("div", class_="box-category")
    skills = None
    if skills_blk:
        tags = skills_blk.find_all("span", class_="box-category-tag")
        skills = [t.get_text(strip=True) for t in tags] if tags else None
    return [title, salary, location, experience, expired_date, company, industry, job_description, level, education, quantity, work_form, skills, link, category]

def try_request(func, max_retries=5, sleep_s=3):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"[retry {i+1}/{max_retries}] {e}")
            time.sleep(sleep_s)
    print("[skip] Max retries exceeded, skipping this item")
    return None
    

if __name__ == "__main__":
    all_jobs_link = {}
    categories = try_request(get_category)

    for category_name, category_url in categories:
        page = 1
        link_list = []
        while True:
            page_url = f"{category_url}?sort=up_top&page={page}"
            result = try_request(lambda: get_job(page_url))
            
            if result is None:
                print(f"[SKIP] Category '{category_name}' page {page} failed after retries")
                break
            
            job_links, stop_flag = result
            
            if job_links:
                link_list.extend(job_links)
                page += 1
                time.sleep(0.6)
            if stop_flag:
                break
            
        link_list = list(dict.fromkeys(link_list))
        all_jobs_link[category_name] = link_list
        print(f'Found {len(link_list)} job links in category: {category_name}')

    jobs = []
    for category, job_links in all_jobs_link.items():
        for job_link in job_links:
            print(f'Crawling {job_link}')
            row = try_request(lambda: get_job_info(job_link, category))
            if row:
                jobs.append(row)
            else:
                print(f"Failed to crawl {job_link} after all retries")

    cols = ["title","salary","location","experience","expired_date","company","industry","job_description","level","education","quantity","work_form","skills","link","category"]
    df = pd.DataFrame(jobs, columns=cols)
    if df.empty:
        raise SystemExit("No jobs collected; exit 1")

    buf = BytesIO()
    df.to_parquet(buf, index=False)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"bronze/topcv/topcv_{ts}.parquet"

    client = create_minio_client()
    save_to_minio(client, "warehouse", object_name, buf)
    print(f"Uploaded to MinIO: s3://warehouse/{object_name}")
    print(f"s3://warehouse/{object_name}")