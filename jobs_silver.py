
import os
import re
import unicodedata
from datetime import datetime
from functools import reduce
from collections import Counter
from itertools import chain
from pathlib import Path

import unidecode
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.utils import AnalysisException

from py4j.java_gateway import java_import

from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"

BRONZE_BASE = "s3a://warehouse/bronze"
PLATFORMS = ["careerviet", "topcv", "vietnamworks"]

NER_MODEL_DIR = "/opt/models/ner-model"

EMBED_MODEL_NAME = "/opt/models/bge-m3"

SILVER_TABLE = "nessie.silver.jobs"

def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("jobs_bronze_to_silver")
        .config("spark.sql.catalog.nessie.ref", "backup")
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    return spark

def normalize_common(text: str) -> str | None:
    if text is None:
        return None
    cleaned = re.sub(r"\s+", " ", text, flags=re.UNICODE).strip().lower()
    cleaned = re.sub(r"\(\s+", "(", cleaned)
    cleaned = re.sub(r"\s+\)", ")", cleaned)
    cleaned = re.sub(r"([^\s(])\(", r"\1 (", cleaned)
    return cleaned or None

normalize_common_udf = udf(normalize_common, StringType())

SALARY_CONFIG = {
    "currency_alias": {
        "tr": ("VND", 1_000_000),
        "triệu": ("VND", 1_000_000),
        "trieu": ("VND", 1_000_000),
        "k": ("VND", 1_000),
        "vnd": ("VND", 1),
        "đ": ("VND", 1),
        "₫": ("VND", 1),
        "usd": ("USD", 1),
        "$": ("USD", 1),
    },
    "patterns": [
        {
            "id": "range_dash",
            "kind": "range",
            "regex": r"([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?\s*[-–]\s*([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?",
        },
        {
            "id": "range_to",
            "kind": "range",
            "regex": r"từ\s*([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?\s*(?:đến|to)\s*([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?",
        },
        {
            "id": "upto",
            "kind": "upto",
            "regex": r"(lên đến|upto|up to|tối đa)\s*([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?",
        },
        {
            "id": "at_least",
            "kind": "at_least",
            "regex": r"(từ|ít nhất|tối thiểu|>=)\s*([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?",
        },
        {
            "id": "single",
            "kind": "single",
            "regex": r"([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)",
        },
        {
            "id": "negotiable",
            "kind": "negotiable",
            "regex": r"(thoả thuận|thương lượng|cạnh tranh|negotiable|competitive)",
        },
    ],
}

_salary_schema = StructType([
    StructField("min_salary", LongType(), True),
    StructField("max_salary", LongType(), True),
    StructField("currency", StringType(), True),
    StructField("salary_type", StringType(), True),
    StructField("parse_status", StringType(), True),
    StructField("pattern_id", StringType(), True),
])

def _parse_money_number(token: str | None, *, allow_decimal: bool) -> float | None:
    if not token:
        return None
    s = token.strip()
    if not s:
        return None

    # chỉ giữ digits và . ,
    s = re.sub(r"[^0-9\.,]", "", s)
    if not s:
        return None

    if not allow_decimal:
        # coi . , là thousand separators
        s2 = s.replace(",", "").replace(".", "")
        return float(s2) if s2.isdigit() else None

    # allow_decimal=True: parse thập phân kiểu VN/EN
    if "." in s and "," in s:
        # dấu xuất hiện sau cùng là decimal
        last_dot = s.rfind(".")
        last_comma = s.rfind(",")
        dec_sep = "." if last_dot > last_comma else ","
        thou_sep = "," if dec_sep == "." else "."
        s = s.replace(thou_sep, "")
        s = s.replace(dec_sep, ".")
    else:
        s = s.replace(",", ".")

    try:
        return float(s)
    except ValueError:
        return None

def _apply_currency(value: float | None, unit: str | None, config: dict):
    if value is None:
        return None, None
    if unit:
        unit_norm = unit.lower()
        currency = config.get(unit_norm, ("VND", 1))
    else:
        currency = ("VND", 1)

    final = int(round(value * currency[1]))
    return final, currency[0]

def _allow_decimal_for_unit(unit: str | None) -> bool:
    u = (unit or "").lower()
    return u in ("tr", "triệu", "trieu", "usd", "$")

def parse_salary(text: str | None):
    if text is None:
        return (None, None, None, None, "missing", None)
    value = text.strip().lower()
    if not value:
        return (None, None, None, None, "missing", None)

    config = SALARY_CONFIG["currency_alias"]
    for pattern in SALARY_CONFIG["patterns"]:
        match = re.search(pattern["regex"], value)
        if not match:
            continue

        kind = pattern["kind"]

        if kind == "negotiable":
            return (None, None, None, "negotiable", "tag", pattern["id"])

        if kind == "range":
            min_raw, unit_min, max_raw, unit_max = match.group(1), match.group(2), match.group(3), match.group(4)

            unit_for_min = unit_min or unit_max
            unit_for_max = unit_max or unit_min

            min_val = _parse_money_number(min_raw, allow_decimal=_allow_decimal_for_unit(unit_for_min))
            max_val = _parse_money_number(max_raw, allow_decimal=_allow_decimal_for_unit(unit_for_max))

            min_final, currency = _apply_currency(min_val, unit_for_min, config)
            max_final, _ = _apply_currency(max_val, unit_for_max, config)

            # fail-safe
            if min_final is not None and max_final is not None and min_final > max_final:
                min_final, max_final = max_final, min_final

            status = "parsed" if min_final is not None and max_final is not None else "partial"
            return (min_final, max_final, currency, "range", status, pattern["id"])

        if kind == "upto":
            value_raw, unit = match.group(2), match.group(3)
            max_val = _parse_money_number(value_raw, allow_decimal=_allow_decimal_for_unit(unit))
            max_final, currency = _apply_currency(max_val, unit, config)
            return (None, max_final, currency, "upto", "parsed" if max_final is not None else "partial", pattern["id"])

        if kind == "at_least":
            value_raw, unit = match.group(2), match.group(3)
            min_val = _parse_money_number(value_raw, allow_decimal=_allow_decimal_for_unit(unit))
            min_final, currency = _apply_currency(min_val, unit, config)
            return (min_final, None, currency, "at_least", "parsed" if min_final is not None else "partial", pattern["id"])

        if kind == "single":
            value_raw, unit = match.group(1), match.group(2)
            val = _parse_money_number(value_raw, allow_decimal=_allow_decimal_for_unit(unit))
            final, currency = _apply_currency(val, unit, config)
            return (final, final, currency, "single", "parsed" if final is not None else "partial", pattern["id"])

    # fallback giữ nguyên logic nhưng dùng parse mới
    digits = re.findall(r"[\d.,]+", value)
    parsed = []
    for d in digits:
        v = _parse_money_number(d, allow_decimal=True)
        if v is not None:
            parsed.append(v)

    if len(parsed) == 1:
        final, currency = _apply_currency(parsed[0], None, config)
        return (final, final, currency, "single", "assumed_vnd", "fallback_single")

    if len(parsed) >= 2:
        min_final, currency = _apply_currency(min(parsed), None, config)
        max_final, _ = _apply_currency(max(parsed), None, config)
        if min_final is not None and max_final is not None and min_final > max_final:
            min_final, max_final = max_final, min_final
        return (min_final, max_final, currency, "range", "assumed_vnd", "fallback_range")

    return (None, None, None, None, "unparsed", "no_match")

parse_salary_udf = udf(parse_salary, _salary_schema)

_experience_schema = StructType([
    StructField("min_years", DoubleType(), True),
    StructField("max_years", DoubleType(), True),
    StructField("experience_type", StringType(), True),
    StructField("parse_status", StringType(), True),
])

_NO_EXP_KEYWORDS = ("không yêu cầu", "không cần kinh nghiệm", "no experience", "fresh", "mới tốt nghiệp")
_EXP_RANGE_PATTERN = re.compile(
    r"(?:từ|from)?\s*(\d+(?:[\.,]\d+)?)\s*(?:\+)?\s*(?:năm|nam|years?|yrs?)?\s*(?:-|–|to|đến)\s*(\d+(?:[\.,]\d+)?)\s*(?:\+)?\s*(?:năm|nam|years?|yrs?)",
    re.IGNORECASE,
)
_AT_LEAST_PATTERN = re.compile(
    r"(?:ít nhất|tối thiểu|>=|>\s*=?|from|at least|minimum|trên)\s*(\d+(?:[\.,]\d+)?)\s*(?:\+)?\s*(?:năm|nam|years?|yrs?)",
    re.IGNORECASE,
)
_MAX_ONLY_PATTERN = re.compile(
    r"(?:tối đa|<=|<\s*=?|dưới|up to)\s*(\d+(?:[\.,]\d+)?)\s*(?:năm|nam|years?|yrs?)",
    re.IGNORECASE,
)
_SINGLE_PATTERN = re.compile(
    r"(\d+(?:[\.,]\d+)?)\s*(?:\+)?\s*(?:năm|nam|years?|yrs?)",
    re.IGNORECASE,
)

def _parse_year_number(value: str | None) -> float | None:
    if not value:
        return None
    normalized = value.replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None

def parse_experience(text: str | None):
    if text is None:
        return (None, None, None, "missing")
    raw = text.strip()
    if not raw:
        return (None, None, None, "missing")
    lowered = raw.lower()

    if any(keyword in lowered for keyword in _NO_EXP_KEYWORDS):
        return (0.0, 0.0, "none", "tag")

    range_match = _EXP_RANGE_PATTERN.search(lowered)
    if range_match:
        min_years = _parse_year_number(range_match.group(1))
        max_years = _parse_year_number(range_match.group(2))
        status = "parsed" if min_years is not None and max_years is not None else "partial"
        return (min_years, max_years, "range", status)

    at_least_match = _AT_LEAST_PATTERN.search(lowered)
    if at_least_match:
        min_years = _parse_year_number(at_least_match.group(1))
        return (min_years, None, "at_least", "parsed" if min_years is not None else "partial")

    max_only_match = _MAX_ONLY_PATTERN.search(lowered)
    if max_only_match:
        max_years = _parse_year_number(max_only_match.group(1))
        return (None, max_years, "upto", "parsed" if max_years is not None else "partial")

    single_match = _SINGLE_PATTERN.search(lowered)
    if single_match:
        value = _parse_year_number(single_match.group(1))
        return (value, value, "single", "parsed" if value is not None else "partial")

    digits = re.findall(r"\d+(?:[\.,]\d+)?", lowered)
    if len(digits) == 1:
        val = _parse_year_number(digits[0])
        return (val, val, "single", "assumed_years" if val is not None else "partial")
    if len(digits) >= 2:
        min_years = _parse_year_number(digits[0])
        max_years = _parse_year_number(digits[1])
        status = "assumed_years" if None not in (min_years, max_years) else "partial"
        return (min_years, max_years, "range", status)

    return (None, None, None, "unparsed")

parse_experience_udf = udf(parse_experience, _experience_schema)

_DMY_PATTERN = re.compile(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})")

def _extract_date_fragment(text: str) -> str:
    lowered = text.lower()
    if "hạn nộp hồ sơ" in lowered:
        return text.split(":")[-1].strip()
    return text

def parse_expired_date(text: str | None):
    if text is None:
        return None
    fragment = _extract_date_fragment(text.strip())
    try:
        return datetime.fromisoformat(fragment.replace("Z", "+00:00")).date()
    except ValueError:
        match = _DMY_PATTERN.search(fragment)
        if not match:
            return None
        day, month, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day)).date()
        except ValueError:
            return None

parse_expired_date_udf = udf(parse_expired_date, DateType())

def _normalize_for_matching(text: str | None) -> str | None:
    if text is None:
        return None
    ascii_text = unicodedata.normalize("NFD", text)
    ascii_stripped = "".join(
        ch for ch in ascii_text
        if unicodedata.category(ch) != "Mn"
    )
    ascii_stripped = ascii_stripped.replace("đ", "d").replace("Đ", "D")
    normalized = re.sub(r"\s+", " ", ascii_stripped).strip().lower()
    return normalized or None

_LEVEL_RULES = [
    ("executive", (
        "tổng giám đốc",
        "giám đốc và cấp cao hơn",
        "giám đốc",
        "phó giám đốc",
    )),
    ("senior_manager", (
        "trưởng/phó phòng",
        "trưởng phòng",
    )),
    ("manager", (
        "quản lý",
    )),
    ("lead_supervisor", (
        "trưởng nhóm",
        "trưởng nhóm / giám sát",
        "quản lý / giám sát",
    )),
    ("staff", (
        "nhân viên",
    )),
    ("intern", (
        "thực tập sinh",
        "thực tập sinh/sinh viên",
        "sinh viên/ thực tập sinh",
    )),
    ("fresher", (
        "mới tốt nghiệp",
    )),
]

_LEVEL_RULES_NORMALIZED = [
    (group, tuple(filter(None, (_normalize_for_matching(keyword) for keyword in keywords))))
    for group, keywords in _LEVEL_RULES
]

_level_schema = StructType([
    StructField("level_group", StringType(), True),
    StructField("level_parse_status", StringType(), True),
    StructField("level_keyword", StringType(), True),
])

def categorize_level(text: str | None):
    cleaned = normalize_common(text)
    if not cleaned:
        return (None, "missing", None)
    normalized = _normalize_for_matching(cleaned)
    if not normalized:
        return (None, "missing", None)
    for group, keywords in _LEVEL_RULES_NORMALIZED:
        for keyword in keywords:
            if keyword and keyword in normalized:
                return (group, "matched", keyword)
    return (None, "unmapped", None)

categorize_level_udf = udf(categorize_level, _level_schema)

_EDUCATION_RULES = [
    ("0", ("trung học", "bất kỳ")),
    ("5", ("tiến sĩ",)),
    ("4", ("thạc sĩ", "sau đại học")),
    ("3", ("cử nhân", "đại học")),
    ("2", ("cao đẳng",)),
    ("1", ("trung cấp", "nghề")),
    ("other", ("khác",)),
]

_EDUCATION_RULES_NORMALIZED = [
    (group, tuple(filter(None, (_normalize_for_matching(keyword) for keyword in keywords))))
    for group, keywords in _EDUCATION_RULES
]

_education_schema = StructType([
    StructField("education_group", StringType(), True),
    StructField("education_parse_status", StringType(), True),
    StructField("education_keyword", StringType(), True),
])

def categorize_education(text: str | None):
    cleaned = normalize_common(text)
    if not cleaned:
        return (None, "missing", None)
    normalized = _normalize_for_matching(cleaned)
    if not normalized:
        return (None, "missing", None)
    for group, keywords in _EDUCATION_RULES_NORMALIZED:
        for keyword in keywords:
            if keyword and keyword in normalized:
                return (group, "matched", keyword)
    return (None, "unmapped", None)

categorize_education_udf = udf(categorize_education, _education_schema)

_PART_TIME_KEYWORDS = ("ban thoi gian", "viec lam online")
_INTERNSHIP_KEYWORDS = ("thuc tap",)
_OTHER_WORKFORMS = ("khac",)
_FULL_TIME_FALLBACK = "full_time"

def categorize_work_form(text: str | None) -> str | None:
    cleaned = normalize_common(text)
    if not cleaned:
        return None
    normalized = _normalize_for_matching(cleaned)
    if not normalized:
        return None

    if any(keyword in normalized for keyword in _INTERNSHIP_KEYWORDS):
        return "internship"
    if any(keyword in normalized for keyword in _PART_TIME_KEYWORDS):
        return "part_time"
    if any(keyword in normalized for keyword in _OTHER_WORKFORMS):
        return "other"
    return _FULL_TIME_FALLBACK

categorize_work_form_udf = udf(categorize_work_form, StringType())

_QUANTITY_PATTERN = re.compile(r"(\d+(?:[.,]\d+)?)")

def normalize_quantity(value: str | None) -> float | None:
    if value is None:
        return 1.0
    match = _QUANTITY_PATTERN.search(value.lower())
    if not match:
        return 1.0
    number = match.group(1).replace(",", ".")
    try:
        return float(number)
    except ValueError:
        return 1.0

normalize_quantity_udf = udf(normalize_quantity, DoubleType())

_INDUSTRY_SECTOR_RULES = [
    (
        "NÔNG NGHIỆP, LÂM NGHIỆP VÀ THỦY SẢN",
        (
            "nong nghiep",
            "nong lam ngu nghiep",
            "nong nghiep/lam nghiep/nuoi trong thuy san",
            "lam nghiep",
            "thuy san",
            "nuoi trong thuy san",
            "thu y",
        ),
    ),
    (
        "KHAI KHOÁNG",
        (
            "khai khoang",
            "khoang san",
            "dau khi",          
        ),
    ),
    (
        "CÔNG NGHIỆP CHẾ BIẾN, CHẾ TẠO",
        (
            "cong nghiep che bien",
            "che bien",
            "che tao",
            "san xuat",
            "co khi",
            "o to",
            "xe may",
            "may moc",
            "thiet bi cong nghiep",
            "det may",
            "may mac",
            "giay dep",
            "da giay",
            "nhua",
            "cao su",
            "hoa chat",
            "hoa sinh",
            "thuc pham",
            "do uong",
            "hang tieu dung (san xuat)",
            "thiet bi y te (san xuat)",
            "dong goi",
            "bao bi",
            "in an",  
            "dien/dien tu",  
            "dien/ dien tu", 
            "dien / dien tu / dien lanh / dien cong nghiep",
            "dien tu / dien lanh",
            "bao tri / sua chua",
            "bao tri/sua chua",
            "dien tu",       
            "dien lanh",     
            "dien cong nghiep",
            "noi that/go",
            "do go",
            "tu dong hoa",
        ),
    ),
    (
        "SẢN XUẤT VÀ PHÂN PHỐI ĐIỆN, KHÍ ĐỐT, NƯỚC NÓNG, HƠI NƯỚC VÀ ĐIỀU HOÀ KHÔNG KHÍ",
        (
            "san xuat va phan phoi dien",
            "dien/khidot/nuoc nong",
            "san xuat dien",
            "dien luc",
            "khidot",
            "gas",
        ),
    ),
    (
        "CUNG CẤP NƯỚC; HOẠT ĐỘNG QUẢN LÝ VÀ XỬ LÝ RÁC THẢI, NƯỚC THẢI",
        (
            "xu ly rac thai",
            "rac thai",
            "nuoc thai",
            "xu ly nuoc thai",
            "cap nuoc",
            "dich vu moi truong",
            "quan ly moi truong",
            "moi truong/chat thai",
            "moi truong",
            "an toan lao dong",
        ),
    ),
    (
        "XÂY DỰNG",
        (
            "xay dung",
            "ha tang",
            "ky thuat xay dung",
            "co so ha tang",
            "vat lieu xay dung",
        ),
    ),
    (
        "BÁN BUÔN VÀ BÁN LẺ",
        (
            "ban buon",
            "ban le",
            "ban si",
            "ban le - hang tieu dung - fmcg",
            "fmcg",
            "sieuthi",
            "trung tam thuong mai",
            "hang tieu dung",
            "thoi trang",
            "trang suc",
            "ban le thoi trang",
            "ban le dien may",
            "ban le dien tu",
            "hang gia dung / cham soc ca nhan",
            "thuong mai tong hop",
            "ban hang / kinh doanh",   
            "ban hang ky thuat"
        ),
    ),
    (
        "VẬN TẢI, KHO BÃI",
        (
            "van tai",
            "logistics",
            "giao nhan",
            "giao nhan - kho van",
            "van chuyen",
            "kho bai",
            "hau can/giao nhan",        
            "xuat nhap khau",      
            "nhap khau/xuat khau", 
            "chuoi cung ung", 
            "thu mua / vat tu",
            "thu mua/vat tu",     
        ),
    ),
    (
        "DỊCH VỤ LƯU TRÚ VÀ ĂN UỐNG",
        (
            "nha hang",
            "khach san",
            "luu tru",
            "dich vu luu tru/nha hang/khach san/du lich",
            "luu tru va an uong",
            "f&b",
            "du lich",
        ),
    ),
    (
        "HOẠT ĐỘNG XUẤT BẢN, PHÁT SÓNG, SẢN XUẤT VÀ PHÂN PHỐI NỘI DUNG",
        (
            "xuat ban",
            "phat song",
            "san xuat noi dung",
            "phim anh",
            "truyen hinh",
            "bao chi",
            "in an/xuat ban",
            "truyen thong",
        ),
    ),
    (
        "HOẠT ĐỘNG VIỄN THÔNG; LẬP TRÌNH MÁY TÍNH, TƯ VẤN, CƠ SỞ HẠ TẦNG MÁY TÍNH VÀ CÁC DỊCH VỤ THÔNG TIN KHÁC",
        (
            "vien thong",
            "buuchinh vien thong",
            "it - phan cung",
            "it - phan mem",
            "cntt - phan mem",
            "cntt - phan cung / mang",
            "cntt - phan cung",
            "cntt - phan mem, cntt - phan cung / mang",
            "he thong cntt & thiet bi",
            "phan mem cntt/dich vu phan mem",
            "internet / online",
            "thuong mai dien tu",  
            "tiep thi / marketing",    
            "tiep thi truc tuyen",
            "lap trinh",
            "ha tang may tinh",
            "dich vu thong tin",
        ),
    ),
    (
        "HOẠT ĐỘNG TÀI CHÍNH, NGÂN HÀNG VÀ BẢO HIỂM",
        (
            "ngan hang",
            "tai chinh",
            "tai chinh / dau tu",
            "bao hiem",
            "chung khoan",
            "quy dau tu",
            "fintech",
            "ke toan / kiem toan",  
            "ke toan/ kiem toan",   
            "ke toan",              
            "kiem toan",            
            "thong ke",             
        ),
    ),
    (
        "HOẠT ĐỘNG KINH DOANH BẤT ĐỘNG SẢN",
        (
            "bat dong san",
            "bds",
            "cho thue",
            "kinh doanh bat dong san",
        ),
    ),
    (
        "HOẠT ĐỘNG CHUYÊN MÔN, KHOA HỌC VÀ CÔNG NGHỆ",
        (
            "tu van",
            "consulting",
            "nghien cuu",
            "r&d",
            "khoa hoc",
            "cong nghe",
            "kien truc/thiet ke noi that",
            "thiet ke/kien truc",
            "kien truc",
            "chuyen mon khoa hoc cong nghe",
            "luat / phap ly",
            "luat/dich vu phap ly",
            "luat",
            "bien phien dich",
            "quan ly chat luong (qa/qc)",
            "qa/qc",
            "hoa hoc"
        ),
    ),
    (
        "HOẠT ĐỘNG HÀNH CHÍNH VÀ DỊCH VỤ HỖ TRỢ",
        (
            "cung cap nhan luc",
            "dich vu ho tro",
            "hanh chinh",
            "thue ngoai",
            "outsourcing",
            "call center",
            "nhan su",
            "an ninh / bao ve",
            "bao ve",
        ),
    ),
    (
        "HOẠT ĐỘNG CỦA ĐẢNG CỘNG SẢN, TỔ CHỨC CHÍNH TRỊ - XÃ HỘI, QUẢN LÝ NHÀ NƯỚC, AN NINH QUỐC PHÒNG; BẢO ĐẢM XÃ HỘI BẮT BUỘC",
        (
            "chinh phu",
            "co quan nha nuoc",
            "quan ly nha nuoc",
            "an ninh quoc phong",
        ),
    ),
    (
        "GIÁO DỤC VÀ ĐÀO TẠO",
        (
            "giao duc",
            "giao duc / dao tao",
            "training",
            "dao tao",
            "truong hoc",
            "truong dai hoc",
        ),
    ),
    (
        "Y TẾ VÀ HOẠT ĐỘNG TRỢ GIÚP XÃ HỘI",
        (
            "y te",
            "cham soc suc khoe",
            "dich vu y te/cham soc suc khoe",
            "benh vien",
            "phong kham",
            "dieu duong",
            "dieu duong - cham soc",
            "hoat dong tro giup xa hoi",
            "nguoi khuyet tat",
            "duoc pham",      
            "duoc pham/ hoa my pham",  
            "lam dep (my pham) & cham soc ca nhan",  
        ),
    ),
    (
        "NGHỆ THUẬT, THỂ THAO VÀ GIẢI TRÍ",
        (
            "nghe thuat",
            "giai tri",
            "the thao",
            "event",
            "to chuc su kien",
            "showbiz",
        ),
    ),
    (
        "HOẠT ĐỘNG DỊCH VỤ KHÁC",
        (
            "dich vu khac",
            "nganh khac",
            "other",
            "khac",     
            "lao dong pho thong",
            "moi tot nghiep / thuc tap",
            "quan ly dieu hanh",
        ),
    ),
    (
        "HOẠT ĐỘNG CỦA CÁC TỔ CHỨC VÀ CƠ QUAN QUỐC TẾ",
        (
            "ngo",
            "to chuc phi chinh phu",
            "co quan quoc te",
            "tổ chức và cơ quan quốc tế",
        ),
    ),
]

_INDUSTRY_SECTOR_RULES_NORMALIZED = [
    (sector, tuple(filter(None, (_normalize_for_matching(k) for k in keywords))))
    for sector, keywords in _INDUSTRY_SECTOR_RULES
]

_industry_sector_schema = StructType([
    StructField("industry_sector", StringType(), True),
    StructField("industry_sector_status", StringType(), True),
    StructField("industry_sector_keyword", StringType(), True),
])

def categorize_industry_sector(text: str | None):
    cleaned = normalize_common(text)
    if not cleaned:
        return (None, "missing", None)
    normalized = _normalize_for_matching(cleaned)
    if not normalized:
        return (None, "missing", None)

    for sector, keywords in _INDUSTRY_SECTOR_RULES_NORMALIZED:
        for keyword in keywords:
            if keyword and keyword in normalized:
                return (sector, "matched", keyword)

    return (None, "unmapped", None)

categorize_industry_sector_udf = udf(categorize_industry_sector, _industry_sector_schema)


def _prepare_source(df: DataFrame, platform: str) -> DataFrame:
    base_columns = (
        "title",
        "salary",
        "location",
        "experience",
        "expired_date",
        "company",
        "job_description",
        "level",
        "education",
        "quantity",
        "work_form",
        "skills",
        "category",
        "industry",
        "link",
    )

    existing_cols = [c for c in base_columns if c in df.columns]
    if existing_cols:
        df = df.select(*existing_cols)

    for col in base_columns:
        if col not in df.columns:
            if col == "skills":
                df = df.withColumn(col, F.array().cast(ArrayType(StringType())))
            else:
                df = df.withColumn(col, F.lit(None).cast(StringType()))

    if isinstance(df.schema["location"].dataType, ArrayType):
        df = df.withColumn("location", F.array_join(F.col("location"), ", "))

    if isinstance(df.schema["industry"].dataType, ArrayType):
        df = df.withColumn("industry", F.array_join(F.col("industry"), ", "))

    from pyspark.sql.types import ArrayType as SparkArrayType

    if isinstance(df.schema["skills"].dataType, SparkArrayType):
        df = df.withColumn("skills", F.col("skills").cast(ArrayType(StringType())))
    else:
        df = df.withColumn(
            "skills",
            F.when(
                F.col("skills").isNull(),
                F.array().cast(ArrayType(StringType()))
            ).otherwise(
                F.array(F.col("skills").cast(StringType()))
            )
        )

    return (
        df
        .withColumn("platform", F.lit(platform))
        .withColumn("title", F.col("title").cast(StringType()))
        .withColumn("salary", F.col("salary").cast(StringType()))
        .withColumn("location", F.col("location").cast(StringType()))
        .withColumn("experience", F.col("experience").cast(StringType()))
        .withColumn("expired_date", F.col("expired_date").cast(StringType()))
        .withColumn("company", F.col("company").cast(StringType()))
        .withColumn("job_description", F.col("job_description").cast(StringType()))
        .withColumn("level", F.col("level").cast(StringType()))
        .withColumn("education", F.col("education").cast(StringType()))
        .withColumn("quantity", F.col("quantity").cast(StringType()))
        .withColumn("work_form", F.col("work_form").cast(StringType()))
        # .withColumn("skills", F.col("skills").cast(ArrayType(StringType())))
        .withColumn("category", F.col("category").cast(StringType()))
        .withColumn("industry", F.col("industry").cast(StringType()))
        .withColumn("link", F.col("link").cast(StringType()))
    )

def _glob_paths(spark: SparkSession, pattern: str) -> list[str]:
    sc = spark.sparkContext
    jvm = sc._jvm
    java_import(jvm, "org.apache.hadoop.fs.*")
    path = jvm.org.apache.hadoop.fs.Path(pattern)
    fs = path.getFileSystem(sc._jsc.hadoopConfiguration())
    stats = fs.globStatus(path) or []
    return [s.getPath().toString() for s in stats]

def get_batch_files(spark: SparkSession, size: int):
    rows = (
        spark.table("nessie.meta.file_tracker")
        .filter(F.col("status") == "pending")
        .limit(size)
        .select("file_path", "platform")
        .collect()
    )
    return [row.asDict() for row in rows]

def update_status_by_file_path(spark: SparkSession, file_path: str, new_status: str):
    safe_path = file_path.replace("'", "''")
    safe_status = new_status.replace("'", "''")
    try:
        spark.sql(f"""
            UPDATE nessie.meta.file_tracker
            SET status = '{safe_status}', updated_at = current_timestamp()
            WHERE file_path = '{safe_path}' and status = 'pending'
        """)
    except Exception as e:
        print(f"Failed to update status for {file_path}: {e}")

def load_batch(spark: SparkSession, files) -> DataFrame:
    dfs = []

    for row in files:
        file_path = row["file_path"]
        platform = row["platform"]

        try:
            df_raw = spark.read.parquet(file_path)
            df_raw = df_raw.withColumn(
                    "quantity",
                    F.col("quantity").cast(StringType()) if "quantity" in df_raw.columns else F.lit("1")
                )  # unify type sớm
            df_prepared = _prepare_source(df_raw, platform)
            dfs.append(df_prepared)
        except Exception as e:
            print(f"Failed to read {file_path}: {e}")
            update_status_by_file_path(spark, file_path, "failed")
            continue

    if not dfs:
        raise RuntimeError("Không tìm thấy hoặc đọc được bất kỳ file nào trong batch.")

    df_union = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        dfs
    )

    # Dedupe trong batch theo link (đã crawl trùng trong cùng ngày)
    df_union = df_union.withColumn("link", F.col("link").cast(StringType()))
    link_window = Window.partitionBy("link").orderBy(
        F.col("expired_date").desc_nulls_last(),
        F.col("title").asc_nulls_last(),
    )

    df_union = (
        df_union
        .withColumn("rn_link_batch", F.row_number().over(link_window))
        .filter(
            (F.col("link").isNull()) |
            (F.col("rn_link_batch") == 1)
        )
        .drop("rn_link_batch")
    )
    return df_union

# def load_today_bronze(spark: SparkSession, date_str: str) -> DataFrame:
#     dfs = []

#     for platform in PLATFORMS:
#         pattern = f"{BRONZE_BASE}/{platform}/{platform}_{date_str}_*.parquet"
#         for p in _glob_paths(spark, pattern):
#             try:
#                 df_raw = spark.read.parquet(p)
#                 df_raw = df_raw.withColumn("quantity", F.col("quantity").cast(StringType()))  # unify type sớm
#                 df_prepared = _prepare_source(df_raw, platform)
#                 dfs.append(df_prepared)
#             except Exception:
#                 continue

#     if not dfs:
#         raise RuntimeError(f"Không tìm thấy parquet hôm nay ({date_str}) cho bất kỳ platform nào.")

#     df_union = reduce(
#         lambda left, right: left.unionByName(right, allowMissingColumns=True),
#         dfs
#     )

#     # Dedupe trong batch theo link (đã crawl trùng trong cùng ngày)
#     df_union = df_union.withColumn("link", F.col("link").cast(StringType()))
#     link_window = Window.partitionBy("link").orderBy(
#         F.col("expired_date").desc_nulls_last(),
#         F.col("title").asc_nulls_last(),
#     )

#     df_union = (
#         df_union
#         .withColumn("rn_link_batch", F.row_number().over(link_window))
#         .filter(
#             (F.col("link").isNull()) |
#             (F.col("rn_link_batch") == 1)
#         )
#         .drop("rn_link_batch")
#     )
#     return df_union

def standardize_jobs(df_union: DataFrame) -> DataFrame:
    df_std = (
        df_union
        .withColumn("title_clean", normalize_common_udf(F.col("title")))
        .withColumn("location_clean", normalize_common_udf(F.col("location")))
        .withColumn("company_clean", normalize_common_udf(F.col("company")))

        .withColumn("salary_struct", parse_salary_udf(F.col("salary")))
        .withColumn("min_salary", F.col("salary_struct.min_salary"))
        .withColumn("max_salary", F.col("salary_struct.max_salary"))
        .withColumn("currency", F.col("salary_struct.currency"))
        .withColumn("salary_type", F.col("salary_struct.salary_type"))
        .withColumn("salary_parse_status", F.col("salary_struct.parse_status"))
        .withColumn("salary_pattern_id", F.col("salary_struct.pattern_id"))
        .withColumn(
            "currency",
            F.when(F.col("salary_type") == "negotiable", F.lit("VND"))
             .otherwise(F.col("currency"))
        )

        .withColumn("experience_struct", parse_experience_udf(F.col("experience")))
        .withColumn("min_years", F.col("experience_struct.min_years"))
        .withColumn("max_years", F.col("experience_struct.max_years"))
        .withColumn("experience_type", F.col("experience_struct.experience_type"))
        .withColumn("experience_parse_status", F.col("experience_struct.parse_status"))
        .withColumn(
            "min_years",
            F.when(F.col("min_years").isNull(), F.lit(0.0)).otherwise(F.col("min_years"))
        )
        .withColumn(
            "max_years",
            F.when(F.col("max_years").isNull(), F.lit(0.0)).otherwise(F.col("max_years"))
        )
        .withColumn(
            "experience_type",
            F.when(
                (F.col("min_years") == 0.0) & (F.col("max_years") == 0.0) & F.col("experience_type").isNull(),
                F.lit("none")
            ).otherwise(F.col("experience_type"))
        )

        .withColumn("expired_date_norm", parse_expired_date_udf(F.col("expired_date")))

        .withColumn("level_struct", categorize_level_udf(F.col("level")))
        .withColumn("level_standard", F.col("level_struct.level_group"))
        .withColumn("level_parse_status", F.col("level_struct.level_parse_status"))
        .withColumn("level_keyword", F.col("level_struct.level_keyword"))

        .withColumn("education_struct", categorize_education_udf(F.col("education")))
        .withColumn("education_standard", F.col("education_struct.education_group"))
        .withColumn("education_parse_status", F.col("education_struct.education_parse_status"))
        .withColumn("education_keyword", F.col("education_struct.education_keyword"))
        .withColumn(
            "education_standard",
            F.coalesce(F.col("education_standard"), F.lit("0"))
        )

        .withColumn("work_form", F.col("work_form").cast(StringType()))
        .withColumn("work_form_standard", categorize_work_form_udf(F.col("work_form")))

        .withColumn("quantity_normalized", normalize_quantity_udf(F.col("quantity")))

        .withColumn("industry_clean", normalize_common_udf(F.col("industry")))
        .withColumn(
            "industry_sector_struct",
            categorize_industry_sector_udf(F.col("industry"))
        )
        .withColumn("industry_sector", F.col("industry_sector_struct.industry_sector"))
        .withColumn("industry_sector_status", F.col("industry_sector_struct.industry_sector_status"))
        .withColumn("industry_sector_keyword", F.col("industry_sector_struct.industry_sector_keyword"))

        .drop("salary_struct", "experience_struct", "level_struct", "education_struct", "industry_sector_struct")
    )

    df_std = df_std.filter(
        F.col("title_clean").isNotNull() & (F.col("title_clean") != "")
    )

    return df_std


def load_ner_pipeline():
    tokenizer = AutoTokenizer.from_pretrained(
        NER_MODEL_DIR,
        local_files_only=True
    )
    model = AutoModelForTokenClassification.from_pretrained(
        NER_MODEL_DIR,
        local_files_only=True
    )
    ner_pipe = pipeline(
        "token-classification",
        model=model,
        tokenizer=tokenizer,
        aggregation_strategy="simple"
    )
    return ner_pipe


def extract_qualification(text: str) -> str:
    if not isinstance(text, str):
        return ""

    start_keywords = [
        "YÊU CẦU ỨNG VIÊN",
        "Yêu cầu ứng viên",
        "Yêu cầu công việc",
        "Yêu Cầu Công Việc",
    ]
    end_keywords = [
        "Thu nhập",
        "Quyền lợi",
        "Phúc lợi",
        "Địa điểm làm việc",
        "Thông tin khác",
        "Các phúc lợi dành cho bạn",
    ]

    start_pos = -1
    for kw in start_keywords:
        match = re.search(kw, text, re.IGNORECASE)
        if match:
            start_pos = match.end()
            break

    if start_pos == -1:
        return ""

    all_end_pos = []
    for kw in end_keywords:
        match = re.search(kw, text[start_pos:], re.IGNORECASE)
        if match:
            all_end_pos.append(start_pos + match.start())

    end_pos = min(all_end_pos) if all_end_pos else len(text)
    return text[start_pos:end_pos].strip()

def insert_space_lower_upper(text: str) -> str:
    if not text:
        return text
    chars = list(text)
    out = [chars[0]]
    for i in range(1, len(chars)):
        prev_ch = chars[i - 1]
        ch = chars[i]
        if prev_ch.islower() and ch.isupper():
            out.append(" ")
        out.append(ch)
    return "".join(out)

def preprocess_requirement(text: str) -> str:
    if not isinstance(text, str):
        return ""

    text = re.sub(r"</?p>|</?strong>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"^[\-\–]\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"[➢▸ü·\r\t•●▪◦\*\+]+", " ", text)
    text = re.sub(r"\n+", " ", text)
    text = re.sub(r"\u2026+", ".", text)
    text = re.sub(r"\.{2,}", ".", text)
    text = re.sub(r"[()]", " ", text)
    text = insert_space_lower_upper(text)
    text = re.sub(r"([\.!?;:])", r" \1 ", text)
    text = re.sub(r"\b([A-Za-z]|[IVXLCM]+|\d+)\s*\.", " ", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()

VOWELS = set("aeiouy")

def is_reasonable_skill_token(s: str) -> bool:
    if not s:
        return False

    s_stripped = s.strip()
    if not s_stripped:
        return False

    s_norm = unidecode.unidecode(s_stripped)
    s_lower = s_norm.lower()

    if not re.search(r"[a-z]", s_lower):
        return False

    if not any(ch in VOWELS for ch in s_lower):
        return False

    words = s_lower.split()

    if len(words) == 1:
        if any(c.isdigit() for c in s_norm):
            return True
        if s_stripped.isupper():
            return True
        if s_stripped[0].isupper() and s_stripped[1:].islower():
            return len(s_lower) >= 3
        if len(s_lower) < 5:
            return False
        consonant_count = len(re.findall(r"[bcdfghjklmnpqrstvwxyz]", s_lower))
        if consonant_count < 2:
            return False
        return True

    total_len = len(s_lower.replace(" ", ""))
    if total_len < 4:
        return False
    if not any(len(w) >= 4 for w in words):
        return False

    return True

def normalize_and_split_skill_span(span: str):
    if not isinstance(span, str):
        return []
    s = span.strip()
    s = s.strip("()[]\"' ")
    s = s.replace("–", "-").replace("—", "-")
    s = s.replace("…", ".")
    s = insert_space_lower_upper(s)
    s = re.sub(r'^[\s\.,;:!?\-]+', '', s)
    s = re.sub(r'[\s\.,;:!?\-]+$', '', s)

    tokens = s.split()
    if len(tokens) > 1 and len(tokens[-1]) == 1 and tokens[-1].isupper():
        tokens = tokens[:-1]
    s = " ".join(tokens)
    s = re.sub(r'\s{2,}', ' ', s)
    raw_parts = re.split(r"[,/;]", s)

    out = []
    for p in raw_parts:
        p = p.strip()
        if not p:
            continue
        if is_reasonable_skill_token(p):
            out.append(p)
    return out

def extract_skills_from_text(text: str, ner_pipe):
    if not isinstance(text, str) or not text.strip():
        return []

    entities = ner_pipe(text)
    skill_ents = []
    for ent in entities:
        label = ent.get("entity_group") or ent.get("entity") or ""
        if "SKILL" in label.upper() and "start" in ent and "end" in ent:
            skill_ents.append(ent)

    if not skill_ents:
        return []

    skill_ents = sorted(skill_ents, key=lambda e: e["start"])
    merged_spans = []
    cur_start = skill_ents[0]["start"]
    cur_end = skill_ents[0]["end"]

    for ent in skill_ents[1:]:
        s = int(ent["start"])
        e = int(ent["end"])
        gap_text = text[cur_end:s]
        if re.fullmatch(r"[\s,;/\-]*", gap_text or ""):
            cur_end = e
        else:
            merged_spans.append(text[cur_start:cur_end])
            cur_start, cur_end = s, e

    merged_spans.append(text[cur_start:cur_end])

    skills = []
    seen = set()
    for raw_span in merged_spans:
        parts = normalize_and_split_skill_span(raw_span)
        for p in parts:
            key = p.lower()
            if key not in seen:
                seen.add(key)
                skills.append(p)

    return skills

def build_skill_counter(df: pd.DataFrame, col_name: str = "skills_ner_raw") -> Counter:
    all_lists = df[col_name].dropna().tolist()
    all_skills = list(chain.from_iterable(all_lists))
    all_skills_norm = [
        unidecode.unidecode(str(s)).strip().lower()
        for s in all_skills
        if isinstance(s, str) and s.strip()
    ]
    return Counter(all_skills_norm)

def clean_skill_list(skills, counter, min_count_single_lower: int = 2):
    if not isinstance(skills, (list, tuple)):
        return []

    cleaned = []
    seen = set()

    for s in skills:
        if not isinstance(s, str):
            continue
        s_stripped = s.strip()
        if not s_stripped:
            continue

        s_norm = unidecode.unidecode(s_stripped).strip().lower()
        if not s_norm:
            continue

        words = s_norm.split()

        if len(words) > 1:
            u = unidecode.unidecode(s_stripped)
            if u.endswith(("A", "E", "I", "O", "U", "Y")) and not s_stripped.isupper():
                continue
            key = s_norm
        else:
            if any(c.isdigit() for c in s_norm):
                key = s_norm
            elif s_stripped.isupper():
                key = s_norm
            elif s_stripped[0].isupper() and s_stripped[1:].islower() and len(s_stripped) >= 3:
                key = s_norm
            else:
                if counter[s_norm] < min_count_single_lower:
                    continue
                key = s_norm

        if key in seen:
            continue
        seen.add(key)
        cleaned.append(s_stripped)

    return cleaned

def merge_skill_lists(original, ner_list):
    merged = []
    seen = set()

    if isinstance(original, list):
        original_list = original
    elif isinstance(original, str):
        original_list = re.split(r"[;,/|]", original)
    else:
        original_list = []

    for s in original_list:
        if not isinstance(s, str):
            continue
        s_clean = s.strip()
        if not s_clean:
            continue
        key = unidecode.unidecode(s_clean).strip().lower()
        if key not in seen:
            seen.add(key)
            merged.append(s_clean)

    if isinstance(ner_list, list):
        for s in ner_list:
            if not isinstance(s, str):
                continue
            s_clean = s.strip()
            if not s_clean:
                continue
            key = unidecode.unidecode(s_clean).strip().lower()
            if key not in seen:
                seen.add(key)
                merged.append(s_clean)

    return merged


CATEGORY_NAMES = [
    "NÔNG NGHIỆP, LÂM NGHIỆP VÀ THỦY SẢN",
    "KHAI KHOÁNG",
    "CÔNG NGHIỆP CHẾ BIẾN, CHẾ TẠO",
    "SẢN XUẤT VÀ PHÂN PHỐI ĐIỆN, KHÍ ĐỐT, NƯỚC NÓNG, HƠI NƯỚC VÀ ĐIỀU HOÀ KHÔNG KHÍ",
    "CUNG CẤP NƯỚC; HOẠT ĐỘNG QUẢN LÝ VÀ XỬ LÝ RÁC THẢI, NƯỚC THẢI",
    "XÂY DỰNG",
    "BÁN BUÔN VÀ BÁN LẺ",
    "VẬN TẢI, KHO BÃI",
    "DỊCH VỤ LƯU TRÚ VÀ ĂN UỐNG",
    "HOẠT ĐỘNG XUẤT BẢN, PHÁT SÓNG, SẢN XUẤT VÀ PHÂN PHỐI NỘI DUNG",
    "HOẠT ĐỘNG VIỄN THÔNG; LẬP TRÌNH MÁY TÍNH, TƯ VẤN, CƠ SỞ HẠ TẦNG MÁY TÍNH VÀ CÁC DỊCH VỤ THÔNG TIN KHÁC",
    "HOẠT ĐỘNG TÀI CHÍNH, NGÂN HÀNG VÀ BẢO HIỂM",
    "HOẠT ĐỘNG KINH DOANH BẤT ĐỘNG SẢN",
    "HOẠT ĐỘNG CHUYÊN MÔN, KHOA HỌC VÀ CÔNG NGHỆ",
    "HOẠT ĐỘNG HÀNH CHÍNH VÀ DỊCH VỤ HỖ TRỢ",
    "HOẠT ĐỘNG CỦA ĐẢNG CỘNG SẢN, TỔ CHỨC CHÍNH TRỊ - XÃ HỘI, QUẢN LÝ NHÀ NƯỚC, AN NINH QUỐC PHÒNG; BẢO ĐẢM XÃ HỘI BẮT BUỘC",
    "GIÁO DỤC VÀ ĐÀO TẠO",
    "Y TẾ VÀ HOẠT ĐỘNG TRỢ GIÚP XÃ HỘI",
    "NGHỆ THUẬT, THỂ THAO VÀ GIẢI TRÍ",
    "HOẠT ĐỘNG DỊCH VỤ KHÁC",
    "HOẠT ĐỘNG LÀM THUÊ CÁC CÔNG VIỆC TRONG CÁC HỘ GIA ĐÌNH, SẢN XUẤT SẢN PHẨM VẬT CHẤT VÀ DỊCH VỤ TỰ TIÊU DÙNG CỦA HỘ GIA ĐÌNH",
    "HOẠT ĐỘNG CỦA CÁC TỔ CHỨC VÀ CƠ QUAN QUỐC TẾ",
]

CATEGORY_VERBOSE = {
    "NÔNG NGHIỆP, LÂM NGHIỆP VÀ THỦY SẢN": """
        Nông nghiệp, trồng trọt, chăn nuôi, thủy sản, nuôi trồng, đánh bắt, trang trại,
        thú y, kỹ thuật nông nghiệp, giống cây trồng, thức ăn chăn nuôi.
    """,
    "KHAI KHOÁNG": """
        Khai khoáng, khai thác mỏ, khoáng sản, than, đá, quặng, dầu khí, giàn khoan,
        an toàn mỏ, địa chất, kỹ sư mỏ, vận hành thiết bị khai thác.
    """,
    "CÔNG NGHIỆP CHẾ BIẾN, CHẾ TẠO": """
        Sản xuất, nhà máy, dây chuyền, QA/QC, cơ khí, điện-điện tử, lắp ráp, bảo trì,
        CNC, khuôn mẫu, dệt may, da giày, thực phẩm, đồ uống, đóng gói, bao bì.
    """,
    "SẢN XUẤT VÀ PHÂN PHỐI ĐIỆN, KHÍ ĐỐT, NƯỚC NÓNG, HƠI NƯỚC VÀ ĐIỀU HOÀ KHÔNG KHÍ": """
        Điện lực, nhà máy điện, trạm biến áp, lưới điện, vận hành điện,
        khí đốt, gas, hơi nước, nồi hơi, HVAC, điều hoà không khí công nghiệp.
    """,
    "CUNG CẤP NƯỚC; HOẠT ĐỘNG QUẢN LÝ VÀ XỬ LÝ RÁC THẢI, NƯỚC THẢI": """
        Cấp nước, nhà máy nước, xử lý nước, xử lý nước thải, môi trường, rác thải,
        vận hành hệ thống xử lý, hoá chất xử lý nước, quan trắc môi trường, HSE.
    """,
    "XÂY DỰNG": """
        Xây dựng, công trình, thi công, giám sát công trình, QS, kiến trúc, kết cấu, MEP,
        dự toán, an toàn công trường, vật liệu xây dựng.
    """,
    "BÁN BUÔN VÀ BÁN LẺ": """
        Bán buôn, bán lẻ, cửa hàng, siêu thị, sales, tư vấn bán hàng, thu ngân,
        quản lý cửa hàng, trưng bày, POS, POSM, FMCG.
    """,
    "VẬN TẢI, KHO BÃI": """
        Logistics, vận tải, kho bãi, xuất nhập khẩu, giao nhận, điều phối,
        shipping, forwarder, hải quan, chứng từ, supply chain.
    """,
    "DỊCH VỤ LƯU TRÚ VÀ ĂN UỐNG": """
        Nhà hàng, khách sạn, resort, F&B, bếp, phục vụ, lễ tân, buồng phòng,
        barista, bartender, đặt phòng, vận hành khách sạn.
    """,
    "HOẠT ĐỘNG XUẤT BẢN, PHÁT SÓNG, SẢN XUẤT VÀ PHÂN PHỐI NỘI DUNG": """
        Xuất bản, báo chí, truyền thông, phát sóng, biên tập, phóng viên,
        sản xuất nội dung, quay dựng, hậu kỳ, studio, truyền hình, radio.
    """,
    "HOẠT ĐỘNG VIỄN THÔNG; LẬP TRÌNH MÁY TÍNH, TƯ VẤN, CƠ SỞ HẠ TẦNG MÁY TÍNH VÀ CÁC DỊCH VỤ THÔNG TIN KHÁC": """
        CNTT, phần mềm, lập trình, developer, backend, frontend, fullstack, QA, tester,
        data, AI, hạ tầng, system, network, cloud, viễn thông, IT support, helpdesk.
    """,
    "HOẠT ĐỘNG TÀI CHÍNH, NGÂN HÀNG VÀ BẢO HIỂM": """
        Ngân hàng, tài chính, kế toán, kiểm toán, tín dụng, giao dịch viên,
        bảo hiểm, thẩm định, đầu tư, chứng khoán.
    """,
    "HOẠT ĐỘNG KINH DOANH BẤT ĐỘNG SẢN": """
        Bất động sản, môi giới, tư vấn mua bán, cho thuê, dự án, pháp lý BĐS,
        sales dự án, chăm sóc khách hàng BĐS.
    """,
    "HOẠT ĐỘNG CHUYÊN MÔN, KHOA HỌC VÀ CÔNG NGHỆ": """
        Tư vấn, consulting, nghiên cứu, R&D, phòng thí nghiệm, kiểm định,
        đo lường, luật, sở hữu trí tuệ, dịch vụ kỹ thuật chuyên môn.
    """,
    "HOẠT ĐỘNG HÀNH CHÍNH VÀ DỊCH VỤ HỖ TRỢ": """
        Hành chính, nhân sự, tuyển dụng, C&B, văn phòng, trợ lý, lễ tân,
        call center, outsourcing, bảo vệ, vệ sinh, hỗ trợ vận hành.
    """,
    "HOẠT ĐỘNG CỦA ĐẢNG CỘNG SẢN, TỔ CHỨC CHÍNH TRỊ - XÃ HỘI, QUẢN LÝ NHÀ NƯỚC, AN NINH QUỐC PHÒNG; BẢO ĐẢM XÃ HỘI BẮT BUỘC": """
        Cơ quan nhà nước, hành chính công, quản lý nhà nước, an ninh quốc phòng,
        BHXH, dịch vụ công.
    """,
    "GIÁO DỤC VÀ ĐÀO TẠO": """
        Giáo dục, đào tạo, giáo viên, giảng viên, trợ giảng,
        trung tâm đào tạo, e-learning, huấn luyện.
    """,
    "Y TẾ VÀ HOẠT ĐỘNG TRỢ GIÚP XÃ HỘI": """
        Y tế, bệnh viện, phòng khám, bác sĩ, điều dưỡng, dược sĩ, trình dược viên,
        dược phẩm, thiết bị y tế, xét nghiệm, chăm sóc sức khỏe, công tác xã hội.
    """,
    "NGHỆ THUẬT, THỂ THAO VÀ GIẢI TRÍ": """
        Nghệ thuật, giải trí, thể thao, sự kiện, biểu diễn,
        thiết kế, sáng tạo, producer, đạo diễn.
    """,
    "HOẠT ĐỘNG DỊCH VỤ KHÁC": """
        Dịch vụ khác, sửa chữa, chăm sóc cá nhân, làm đẹp, salon, spa,
        dịch vụ cộng đồng, nghề nghiệp đặc thù khác.
    """,
    "HOẠT ĐỘNG LÀM THUÊ CÁC CÔNG VIỆC TRONG CÁC HỘ GIA ĐÌNH, SẢN XUẤT SẢN PHẨM VẬT CHẤT VÀ DỊCH VỤ TỰ TIÊU DÙNG CỦA HỘ GIA ĐÌNH": """
        Giúp việc, tạp vụ gia đình, chăm sóc người già, chăm sóc trẻ em,
        lao động phục vụ trong hộ gia đình.
    """,
    "HOẠT ĐỘNG CỦA CÁC TỔ CHỨC VÀ CƠ QUAN QUỐC TẾ": """
        Tổ chức quốc tế, NGO, INGO, dự án quốc tế, UN, viện trợ,
        điều phối dự án, M&E, grant.
    """,
}

def build_job_text(row):
    title = row.get("title", "") or ""
    skills = row.get("skills_all", []) or []
    category_raw = row.get("category", "") or ""           # category cũ (raw từ source)
    industry_sector = row.get("industry_sector", "") or "" # bạn đã có từ standardize_jobs

    skills_str = ", ".join(map(str, skills))

    # trick tăng trọng số cho category cũ + industry_sector
    parts = [
        f"Tieu de: {title}. {title}.",
    ]

    if category_raw:
        parts.append(f"Category_cu: {category_raw}. {category_raw}. {category_raw}.")  # weight mạnh
    if industry_sector:
        parts.append(f"Nganh_chuan_rule: {industry_sector}. {industry_sector}.")       # weight vừa

    if skills_str:
        parts.append(f"Ky nang: {skills_str}")

    return " ".join(parts).strip()


def load_category_embedding_model():
    model_eb = SentenceTransformer(EMBED_MODEL_NAME)
    category_texts = [CATEGORY_VERBOSE[name] for name in CATEGORY_NAMES]
    category_embs = model_eb.encode(
        category_texts,
        convert_to_numpy=True,
        normalize_embeddings=True,
        batch_size=32,
        show_progress_bar=False,
    )
    return model_eb, category_embs

def enrich_with_ner_and_category(df_std: DataFrame, spark: SparkSession) -> DataFrame:
    df_std.select("skills").show(20, truncate=False)
    
    df_pd = df_std.select(
        "title", "job_description", "skills", "category",
        "platform",
        "title_clean", "location_clean", "company_clean",
        "min_salary", "max_salary", "currency", "salary_type",
        "min_years", "max_years", "experience_type",
        "education_standard", "work_form_standard",
        "quantity_normalized", "expired_date_norm",
        "industry_sector",
        "link",
    ).toPandas()

    if "job_description" not in df_pd.columns:
        return df_std

    ner_pipe = load_ner_pipeline()

    df_pd["qualification_raw"] = df_pd["job_description"].apply(extract_qualification)
    df_pd["qualification"] = df_pd["qualification_raw"].apply(preprocess_requirement)
    df_pd["skills_ner_raw"] = df_pd["qualification"].apply(lambda x: extract_skills_from_text(x, ner_pipe))
    skill_counter = build_skill_counter(df_pd, col_name="skills_ner_raw")
    df_pd["skills_ner"] = df_pd["skills_ner_raw"].apply(
        lambda lst: clean_skill_list(lst, skill_counter, min_count_single_lower=2)
    )
    df_pd["skills_all"] = df_pd.apply(
        lambda row: merge_skill_lists(row.get("skills"), row.get("skills_ner")),
        axis=1
    )

    model_eb, category_embs = load_category_embedding_model()

    df_pd["job_text"] = df_pd.apply(lambda r: build_job_text(r.to_dict()), axis=1)
    job_embs = model_eb.encode(
        df_pd["job_text"].tolist(),
        convert_to_numpy=True,
        normalize_embeddings=True,
        batch_size=32,
        show_progress_bar=True,
    )

    sim_matrix = cosine_similarity(job_embs, category_embs)
    best_idx = sim_matrix.argmax(axis=1)
    best_scores = sim_matrix.max(axis=1)

    df_pd["category_name_emb"] = [CATEGORY_NAMES[i] for i in best_idx]
    df_pd["category_score_emb"] = best_scores

    THRESHOLD = 0.55
    FALLBACK = "HOẠT ĐỘNG DỊCH VỤ KHÁC"

    df_pd["category_name_final"] = np.where(
        df_pd["category_score_emb"] >= THRESHOLD,
        df_pd["category_name_emb"],
        FALLBACK,
    )

    df_enriched = (
        spark.createDataFrame(df_pd)
        .withColumn("expired_date_norm", F.col("expired_date_norm").cast(DateType()))
        .withColumn("skills_all", F.col("skills_all").cast(ArrayType(StringType())))
    )
    return df_enriched

def normalize_location(jobs_df: DataFrame, spark: SparkSession) -> DataFrame:
    """ Chuẩn hóa location, đưa về tên tỉnh thành chuẩn của Việt Nam nếu có thể"""
    
    vietnam_provinces = [
        "An Giang",
        "Bà Rịa - Vũng Tàu",
        "Bắc Giang",
        "Bắc Kạn",
        "Bạc Liêu",
        "Bắc Ninh",
        "Bến Tre",
        "Bình Định",
        "Bình Dương",
        "Bình Phước",
        "Bình Thuận",
        "Cà Mau",
        "Cần Thơ",
        "Cao Bằng",
        "Đà Nẵng",
        "Đắk Lắk",
        "Đắk Nông",
        "Điện Biên",
        "Đồng Nai",
        "Đồng Tháp",
        "Gia Lai",
        "Hà Giang",
        "Hà Nam",
        "Hà Nội",
        "Hà Tĩnh",
        "Hải Dương",
        "Hải Phòng",
        "Hậu Giang",
        "Hòa Bình",
        "Hưng Yên",
        "Khánh Hòa",
        "Kiên Giang",
        "Kon Tum",
        "Lai Châu",
        "Lâm Đồng",
        "Lạng Sơn",
        "Lào Cai",
        "Long An",
        "Nam Định",
        "Nghệ An",
        "Ninh Bình",
        "Ninh Thuận",
        "Phú Thọ",
        "Phú Yên",
        "Quảng Bình",
        "Quảng Nam",
        "Quảng Ngãi",
        "Quảng Ninh",
        "Quảng Trị",
        "Sóc Trăng",
        "Sơn La",
        "Tây Ninh",
        "Thái Bình",
        "Thái Nguyên",
        "Thanh Hóa",
        "Thừa Thiên Huế",
        "Tiền Giang",
        "Hồ Chí Minh",
        "Trà Vinh",
        "Tuyên Quang",
        "Vĩnh Long",
        "Vĩnh Phúc",
        "Yên Bái"
    ]

    unidecode_udf = F.udf(lambda x: unidecode.unidecode(x) if x else None, StringType())

    # Tạo DataFrame chứa tên tỉnh thành đã chuẩn hóa và phiên bản unidecode của nó
    vietnam_provinces_df = spark.createDataFrame(vietnam_provinces, "string").toDF("province")

    vietnam_provinces_df = (
        vietnam_provinces_df
        .withColumn("province_clean", unidecode_udf(F.col("province")))
        .withColumn("province_clean", F.regexp_replace(F.col("province_clean"), r'[^\w\s]', ''))
        .withColumn("province_clean", F.regexp_replace(F.col("province_clean"), r'\s+', ' '))
        .withColumn("province_clean", F.trim(F.col("province_clean")))
        .withColumn("province_clean", F.lower(F.col("province_clean")))
    )

    # Chuẩn hóa cột location_clean trong jobs_df theo cùng một cách
    jobs_norm_df = (jobs_df.withColumn("location_clean", unidecode_udf(F.col("location_clean")))
        .withColumn("location_clean", F.regexp_replace(F.col("location_clean"), r'[^\w\s]', ''))
        .withColumn("location_clean", F.regexp_replace(F.col("location_clean"), r'\s+', ' '))
        .withColumn("location_clean", F.trim(F.col("location_clean")))
        .withColumn("location_clean", F.lower(F.col("location_clean")))
    )

    # Tạo view tạm thời để join với bảng tỉnh thành
    vietnam_provinces_df.createOrReplaceTempView("provinces")
    jobs_norm_df.createOrReplaceTempView("jobs")

    # Join để gán tên tỉnh thành chuẩn vào jobs_df
    jobs_norm_df = spark.sql("""
        WITH joined AS (
            SELECT 
                j.*,
                p.province,
                ROW_NUMBER() OVER (
                    PARTITION BY j.platform, j.title_clean, j.location_clean
                    ORDER BY LENGTH(p.province_clean) DESC
                ) as rn
            FROM jobs j
            LEFT JOIN provinces p 
                ON j.location_clean LIKE CONCAT('%', p.province_clean, '%')
        )
        SELECT 
            platform,
            title_clean,
            COALESCE(province, "Khác") as location_clean,
            company_clean,
            min_salary,
            max_salary,
            currency,
            salary_type,
            min_years,
            max_years,
            experience_type,
            education_standard,
            work_form_standard,
            quantity_normalized,
            expired_date_norm,
            industry_sector,
            skills_all,
            category_name_final,
            link
        FROM joined
        WHERE rn = 1
    """)

    return jobs_norm_df

def upsert_into_silver(df_new: DataFrame, spark: SparkSession):
    # Đảm bảo df_new có đủ các cột này
    SILVER_COLS = [
        "platform",
        "title_clean",
        "location_clean",
        "company_clean",
        "min_salary",
        "max_salary",
        "currency",
        "salary_type",
        "min_years",
        "max_years",
        "experience_type",
        "education_standard",
        "work_form_standard",
        "quantity_normalized",
        "expired_date_norm",
        "industry_sector",
        "skills_all",
        "category_name_final",
        "link",
    ]

    df_new = df_new.select(*SILVER_COLS)

    try:
        df_existing = spark.read.table(SILVER_TABLE)
        df_existing = df_existing.select(*SILVER_COLS)
        has_existing = True
    except AnalysisException:
        has_existing = False

    if has_existing:
        df_combined = df_existing.unionByName(df_new, allowMissingColumns=False)
    else:
        df_combined = df_new

    df_combined = df_combined.withColumn("link", F.col("link").cast(StringType()))

    cdc_window = Window.partitionBy("link").orderBy(
        F.col("expired_date_norm").desc_nulls_last(),
        F.col("platform").asc_nulls_last(),  # tie-breaker
    )

    df_dedup = (
        df_combined
        .withColumn("rn_cdc", F.row_number().over(cdc_window))
        .filter(
            (F.col("link").isNull()) |  # job không có link thì giữ nguyên
            (F.col("rn_cdc") == 1)
        )
        .drop("rn_cdc")
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    (
        df_dedup
        .write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(SILVER_TABLE)
    )

    dedup_key_cols = [
        "platform",
        "title_clean",
        "location_clean",
        "company_clean",
        "min_salary",
        "max_salary",
        "currency",
        "salary_type",
        "min_years",
        "max_years",
        "experience_type",
        "education_standard",
        "work_form_standard",
        "quantity_normalized",
        "expired_date_norm",
        "industry_sector",
        "category_name_final",
    ]
    df_dedup_keyed = df_dedup.withColumn(
        "job_key",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in dedup_key_cols]
            ),
            256,
        )
    )

    job_window = Window.partitionBy("job_key").orderBy(
        F.col("expired_date_norm").desc_nulls_last(),
        F.col("link").asc_nulls_last()
    )

    df_dedup_final = (
        df_dedup_keyed
        .withColumn("rn_job", F.row_number().over(job_window))
        .filter(F.col("rn_job") == 1)
        .drop("rn_job", "job_key")
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    (
        df_dedup_final
        .write
        .format("iceberg")
        .mode("overwrite")
        .saveAsTable(SILVER_TABLE)
    )

def main():
    today_str = datetime.today().strftime("%Y%m%d") or os.getenv("JOB_DATE")
    spark = build_spark_session()
    # df_union = load_today_bronze(spark, today_str)
    files = get_batch_files(spark, 10)
    df_union = load_batch(spark, files)
    df_standardized_new = standardize_jobs(df_union)
    df_enriched_new = enrich_with_ner_and_category(df_standardized_new, spark)
    df_enriched_new = normalize_location(df_enriched_new, spark)
    upsert_into_silver(df_enriched_new, spark)
    for f in files:
        update_status_by_file_path(spark, f["file_path"], "processed")
    spark.stop()

if __name__ == "__main__":
    main()
