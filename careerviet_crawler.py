from minio import Minio
from minio.error import S3Error
from io import BytesIO
import os
import time
import requests
from datetime import datetime, date
from bs4 import BeautifulSoup
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
}

BASE_URL = "https://careerviet.vn/tim-viec-lam.html"


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

def try_request(func, max_retries=6, delay=5):
    for i in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"[Retry {i + 1}/{max_retries}] Lỗi: {e}")
            time.sleep(delay)
    return None


def get_all_categories(soup):
    categories = {}
    title_sections = soup.find_all('div', class_='title-h3')

    for title_section in title_sections:
        h2_tag = title_section.find('h2')
        if not h2_tag:
            continue

        category_name = h2_tag.get_text(strip=True).replace('Tìm việc làm ', '')
        categories[category_name] = []

        next_ul = title_section.find_next_sibling('ul', class_='list-jobs')
        if not next_ul:
            continue

        for li in next_ul.find_all('li'):
            link_tag = li.find('a')
            if link_tag and link_tag.get('href'):
                categories[category_name].append(link_tag.get('href'))

    return categories


def get_job_links(link):
    response = requests.get(link, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')

    job_cards = soup.find_all('div', class_='job-item')
    job_links_and_dates = []

    if not job_cards:
        raise Exception("Không tìm thấy thẻ job-item nào!")

    for card in job_cards:
        job_link = card.find('a', class_='job_link')['href']
        time_divs = card.find_all('time')
        post_date = date.today()

        if len(time_divs) > 0:
            try:
                post_date = datetime.strptime(time_divs[-1].get_text(strip=True), '%d-%m-%Y').date()
            except Exception:
                pass
        job_links_and_dates.append((job_link, post_date))
    return job_links_and_dates

def get_text_or_none(elem):
    return elem.get_text(strip=True) if elem else None


def get_location(soup):
    place_detail = soup.find('div', class_='info-place-detail')
    if place_detail:
        return get_text_or_none(place_detail.find('span'))
    map_div = soup.find('div', class_='map')
    if map_div and map_div.find('a'):
        return map_div.find('a').text.strip()
    return None


def get_title_company(soup):
    title = None
    company_name = None
    
    title = soup.find('h1', class_='title').get_text(strip=True) if soup.find('h1', class_='title') else None
    if title is None:
        caption_div = soup.find('div', class_='caption')
        title_div = caption_div.find('div', class_='title') if caption_div else None
        title = title_div.get_text(strip=True) if title_div else None
    if title is None:
        head_left_div = soup.find('div', class_='head-left')
        title_div = head_left_div.find('div', class_='title') if head_left_div else None
        title = title_div.get_text(strip=True) if title_div else None
    company_name = soup.find('a', class_='company').get_text(strip=True) if soup.find('a', class_='company') else None
    if company_name is None:
        company_name = soup.find('a', class_='employer job-company-name').get_text(strip=True) if soup.find('a', class_='employer job-company-name') else None
    return { 'title': title, 'company': company_name }


def get_general_info(soup):
    job_info = {
        'salary': None,
        'experience': None,
        'level': None,
        'expired_date': None,
        'work_form': None,
        'industry': None,   
    }

    general_info = soup.find_all('div', class_='detail-box has-background')
    if len(general_info) > 1:
        li_elements = []
        for info in general_info:
            li_elements.extend(info.find_all('li'))
        for li in li_elements:
            strong_tag = li.find('strong')
            if strong_tag:
                field_name = strong_tag.get_text(strip=True)
                p_tag = li.find('p')
                if p_tag:
                    value = p_tag.get_text(separator=' ', strip=True)
                    value = ' '.join(value.split())

                    if 'Lương' in field_name:
                        job_info['salary'] = value
                    elif 'Kinh nghiệm' in field_name:
                        job_info['experience'] = value
                    elif 'Cấp bậc' in field_name:
                        job_info['level'] = value
                    elif 'Hết hạn nộp' in field_name:
                        job_info['expired_date'] = value
                    elif 'Hình thức' in field_name:
                        job_info['work_form'] = value
                    elif 'Ngành nghề' in field_name:
                        job_info['industry'] = [item.strip() for item in value.split(',')]

    elif soup.find('ul', class_='info'):
        info_career = soup.find('ul', class_='info')
        if info_career:
            li_elements = info_career.find_all('li')
            for li in li_elements:
                b_tag = li.find('b')
                if b_tag:
                    field_name = b_tag.get_text(strip=True)
                    value_div = li.find('div', class_='value')
                    if value_div:
                        value = value_div.get_text(separator=' ', strip=True)
                        value = ' '.join(value.split())

                        if 'Lương' in field_name:
                            job_info['salary'] = value
                        elif 'Kinh nghiệm' in field_name:
                            job_info['experience'] = value
                        elif 'Cấp bậc' in field_name:
                            job_info['level'] = value
                        elif 'Hết hạn nộp' in field_name:
                            job_info['expired_date'] = value
                        elif 'Hình thức' in field_name:
                            job_info['work_form'] = ', '.join([v.strip() for v in value.split(',')])
                        elif 'Ngành nghề' in field_name:
                            job_info['industry'] = [item.strip() for item in value.split(',')]

    elif soup.find('div', class_='table'):
        table = soup.find('div', class_='table')
        if table:
            rows = table.find_all('tr')
            for row in rows:
                name_td = row.find('td', class_='name')
                content_td = row.find('td', class_='content')
                if name_td and content_td:
                    field_name = name_td.get_text(strip=True)
                    value = content_td.get_text(separator=' ', strip=True)
                    value = ' '.join(value.split())

                    if 'Lương' in field_name:
                        job_info['salary'] = value
                    elif 'Kinh nghiệm' in field_name:
                        job_info['experience'] = value
                    elif 'Cấp bậc' in field_name:
                        job_info['level'] = value
                    elif 'Hết hạn nộp' in field_name:
                        job_info['expired_date'] = value
                    elif 'Hình thức' in field_name:
                        job_info['work_form'] = value
                    elif 'Ngành nghề' in field_name:
                        job_info['industry'] = [item.strip() for item in value.split(',')]

    detail_rows = soup.find_all('div', class_='detail-row')
    for detail_row in detail_rows:
        title_elem = detail_row.find('h3', class_='detail-title')
        if title_elem and 'Thông tin khác' in title_elem.get_text(strip=True):
            for class_name in ['content', 'content_fck']:
                content_div = detail_row.find('div', class_=class_name)
                if content_div:
                    li_elements = content_div.find_all('li')
                    for li in li_elements:
                        text = li.get_text(strip=True)
                        if job_info['work_form'] is None and 'Hình thức' in text:
                            wf = text.split('Hình thức')[-1].strip()
                            job_info['work_form'] = ' '.join(wf.split())
            break

    return job_info


def get_job_description(soup):
    sections = []
    for title in soup.find_all(['h2', 'h3'], class_='detail-title'):
        t_text = title.text.strip()
        if any(k in t_text for k in ['Mô tả', 'Yêu Cầu', 'Công việc']):
            sections.append(f"\n{t_text}:")
            parent = title.find_parent()
            if parent:
                for elem in parent.find_all(['p', 'li']):
                    text = elem.get_text(strip=True)
                    if text:
                        sections.append(text)
    return '\n'.join(sections) if sections else None


def get_education(soup):
    for li in soup.find_all('li'):
        text = li.get_text(strip=True)
        if 'Bằng cấp:' in text:
            return text.split('Bằng cấp:')[1].strip()
    return None


def get_skills(soup):
    section = soup.find('div', class_='job-tags')
    if section:
        return [a.get_text(strip=True) for a in section.find_all('a')]
    return []


def get_job_info(link, category):
    resp = requests.get(link, headers=HEADERS)
    soup = BeautifulSoup(resp.text, 'html.parser')

    title_company = get_title_company(soup)
    general = get_general_info(soup)

    return {
        'title': title_company['title'],
        'company': title_company['company'],
        'salary': general['salary'],
        'experience': general['experience'],
        'expired_date': general['expired_date'],
        'level': general['level'],
        'work_form': general['work_form'],
        'industry': general['industry'],
        'education': get_education(soup),
        'job_description': get_job_description(soup),
        'skills': get_skills(soup),
        'location': get_location(soup),
        'category': category,
        'link': link
    }

def crawl_career_viet():
    home_resp = requests.get(BASE_URL, headers=HEADERS)
    soup = BeautifulSoup(home_resp.text, 'html.parser')
    categories = get_all_categories(soup)

    all_jobs = {}
    for category, urls in categories.items():
        all_jobs[category] = []
        for base_url in urls:
            i = 1
            while True:
                page_url = base_url.replace('-vi.html', f'-trang-{i}-vi.html')
                page_url = 'https://careerviet.vn/' + page_url if not page_url.startswith('http') else page_url 
                job_links = try_request(lambda: get_job_links(page_url))
                if not job_links:
                    break
                stop = False
                for link, pdate in job_links:
                    if pdate == date.today():
                        all_jobs[category].append(link)
                    else:
                        stop = True
                        break
                if stop:
                    break
                i += 1
                time.sleep(1)

    jobs = []
    for cat, links in all_jobs.items():
        print(f"\nĐang xử lý {len(links)} job trong {cat}")
        for l in links:
            l = 'https://careerviet.vn' + l if not l.startswith('http') else l
            job = try_request(lambda: get_job_info(l, cat))
            if job:
                jobs.append(job)

    df = pd.DataFrame(jobs)
    # df.to_csv("data/csv/carrerviet.csv", index=False)
    # df.to_parquet("data/parquet/carrerviet.parquet")
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"bronze/careerviet/careerviet_{ts}.parquet"

    client = create_minio_client()
    save_to_minio(client, "warehouse", object_name, buf)
    print(f"Uploaded to MinIO: s3://warehouse/{object_name}")
    print(f"s3://warehouse/{object_name}")

if __name__ == "__main__":
    crawl_career_viet()
