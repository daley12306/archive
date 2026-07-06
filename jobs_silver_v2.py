import os
import re
import unicodedata
from collections import Counter
from datetime import datetime
from functools import reduce

import logging
import transformers
import unidecode
import pandas as pd
import numpy as np

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
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
from transformers import AutoTokenizer, AutoModelForTokenClassification, logger, pipeline
from pyspark.sql.functions import pandas_udf
from datetime import date as python_date

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

S3_ENDPOINT   = "http://minio:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"

BRONZE_BASE = "s3a://warehouse/bronze"
PLATFORMS   = ["careerviet", "topcv", "vietnamworks"]

NER_MODEL_NAME   = "zikay3624/careerlake-ner-skill"

SILVER_TABLE = "nessie.silver.jobs"

_LOCAL_MODEL_PATH = "/opt/models/hf_cache/careerlake-ner-skill"
NER_MODEL_NAME = _LOCAL_MODEL_PATH if os.path.isdir(_LOCAL_MODEL_PATH) else "zikay3624/careerlake-ner-skill"

# ─────────────────────────────────────────────────────────────────────────────
# Spark session
# ─────────────────────────────────────────────────────────────────────────────

def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("jobs_bronze_to_silver")
        .config("spark.sql.catalog.nessie.ref", "test")
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.filterPushdown", "false")
        .config("spark.hadoop.parquet.filter.columnindex.enabled", "false")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Common normalizer
# ─────────────────────────────────────────────────────────────────────────────

def normalize_common(text: str) -> str | None:
    if text is None:
        return None
    cleaned = re.sub(r"\s+", " ", text, flags=re.UNICODE).strip().lower()
    cleaned = re.sub(r"\(\s+", "(", cleaned)
    cleaned = re.sub(r"\s+\)", ")", cleaned)
    cleaned = re.sub(r"([^\s(])\(", r"\1 (", cleaned)
    return cleaned or None

normalize_common_udf = F.udf(normalize_common, StringType())


# ─────────────────────────────────────────────────────────────────────────────
# Salary parsing
# ─────────────────────────────────────────────────────────────────────────────

SALARY_CONFIG = {
    "currency_alias": {
        "tr":    ("VND", 1_000_000),
        "triệu": ("VND", 1_000_000),
        "trieu": ("VND", 1_000_000),
        "k":     ("VND", 1_000),
        "vnd":   ("VND", 1),
        "đ":     ("VND", 1),
        "₫":     ("VND", 1),
        "usd":   ("USD", 1),
        "$":     ("USD", 1),
    },
    "patterns": [
        {
            "id":    "range_dash",
            "kind":  "range",
            "regex": r"([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?\s*[-–]\s*([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?",
        },
        {
            "id":    "range_to",
            "kind":  "range",
            "regex": r"từ\s*([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?\s*(?:đến|to)\s*([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?",
        },
        {
            "id":    "upto",
            "kind":  "upto",
            "regex": r"(lên đến|upto|up to|tối đa)\s*([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?",
        },
        {
            "id":    "at_least",
            "kind":  "at_least",
            "regex": r"(từ|ít nhất|tối thiểu|>=)\s*([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)?",
        },
        {
            "id":    "single",
            "kind":  "single",
            "regex": r"([\d.,]+)\s*(tr|triệu|k|usd|vnd|₫)",
        },
        {
            "id":    "negotiable",
            "kind":  "negotiable",
            "regex": r"(thoả thuận|thương lượng|cạnh tranh|negotiable|competitive)",
        },
    ],
}

_salary_schema = StructType([
    StructField("min_salary",   LongType(),   True),
    StructField("max_salary",   LongType(),   True),
    StructField("currency",     StringType(), True),
    StructField("salary_type",  StringType(), True),
    StructField("parse_status", StringType(), True),
    StructField("pattern_id",   StringType(), True),
])


def _parse_money_number(token: str | None, *, allow_decimal: bool) -> float | None:
    if not token:
        return None
    s = token.strip()
    if not s:
        return None
    s = re.sub(r"[^0-9\.,]", "", s)
    if not s:
        return None
    if not allow_decimal:
        s2 = s.replace(",", "").replace(".", "")
        return float(s2) if s2.isdigit() else None
    if "." in s and "," in s:
        last_dot   = s.rfind(".")
        last_comma = s.rfind(",")
        dec_sep  = "." if last_dot > last_comma else ","
        thou_sep = "," if dec_sep == "." else "."
        s = s.replace(thou_sep, "").replace(dec_sep, ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _apply_currency(value: float | None, unit: str | None, config: dict):
    if value is None:
        return None, None
    currency = config.get((unit or "").lower(), ("VND", 1))
    return int(round(value * currency[1])), currency[0]


def _allow_decimal_for_unit(unit: str | None) -> bool:
    return (unit or "").lower() in ("tr", "triệu", "trieu", "usd", "$")


def parse_salary(text: str | None):
    if not text or not text.strip():
        return (None, None, None, None, "missing", None)
    value  = text.strip().lower()
    config = SALARY_CONFIG["currency_alias"]

    for pattern in SALARY_CONFIG["patterns"]:
        match = re.search(pattern["regex"], value)
        if not match:
            continue
        kind = pattern["kind"]

        if kind == "negotiable":
            return (None, None, None, "negotiable", "tag", pattern["id"])

        if kind == "range":
            min_raw, unit_min, max_raw, unit_max = (
                match.group(1), match.group(2), match.group(3), match.group(4)
            )
            unit_for_min = unit_min or unit_max
            unit_for_max = unit_max or unit_min
            min_val = _parse_money_number(min_raw, allow_decimal=_allow_decimal_for_unit(unit_for_min))
            max_val = _parse_money_number(max_raw, allow_decimal=_allow_decimal_for_unit(unit_for_max))
            min_final, currency = _apply_currency(min_val, unit_for_min, config)
            max_final, _        = _apply_currency(max_val, unit_for_max, config)
            if min_final is not None and max_final is not None and min_final > max_final:
                min_final, max_final = max_final, min_final
            status = "parsed" if min_final is not None and max_final is not None else "partial"
            return (min_final, max_final, currency, "range", status, pattern["id"])

        if kind == "upto":
            val_raw, unit = match.group(2), match.group(3)
            max_val = _parse_money_number(val_raw, allow_decimal=_allow_decimal_for_unit(unit))
            max_final, currency = _apply_currency(max_val, unit, config)
            return (None, max_final, currency, "upto",
                    "parsed" if max_final is not None else "partial", pattern["id"])

        if kind == "at_least":
            val_raw, unit = match.group(2), match.group(3)
            min_val = _parse_money_number(val_raw, allow_decimal=_allow_decimal_for_unit(unit))
            min_final, currency = _apply_currency(min_val, unit, config)
            return (min_final, None, currency, "at_least",
                    "parsed" if min_final is not None else "partial", pattern["id"])

        if kind == "single":
            val_raw, unit = match.group(1), match.group(2)
            val = _parse_money_number(val_raw, allow_decimal=_allow_decimal_for_unit(unit))
            final, currency = _apply_currency(val, unit, config)
            return (final, final, currency, "single",
                    "parsed" if final is not None else "partial", pattern["id"])

    digits = re.findall(r"[\d.,]+", value)
    parsed = [v for d in digits if (v := _parse_money_number(d, allow_decimal=True)) is not None]
    if len(parsed) == 1:
        final, currency = _apply_currency(parsed[0], None, config)
        return (final, final, currency, "single", "assumed_vnd", "fallback_single")
    if len(parsed) >= 2:
        mn, mx = min(parsed), max(parsed)
        min_final, currency = _apply_currency(mn, None, config)
        max_final, _        = _apply_currency(mx, None, config)
        return (min_final, max_final, currency, "range", "assumed_vnd", "fallback_range")
    return (None, None, None, None, "unparsed", "no_match")

parse_salary_udf = F.udf(parse_salary, _salary_schema)


# ─────────────────────────────────────────────────────────────────────────────
# Experience parsing
# ─────────────────────────────────────────────────────────────────────────────

_experience_schema = StructType([
    StructField("min_years",       DoubleType(), True),
    StructField("max_years",       DoubleType(), True),
    StructField("experience_type", StringType(), True),
    StructField("parse_status",    StringType(), True),
])

_NO_EXP_KEYWORDS    = ("không yêu cầu", "không cần kinh nghiệm", "no experience", "fresh", "mới tốt nghiệp")
_EXP_RANGE_PATTERN  = re.compile(
    r"(?:từ|from)?\s*(\d+(?:[\.,]\d+)?)\s*(?:\+)?\s*(?:năm|nam|years?|yrs?)?\s*(?:-|–|to|đến)\s*(\d+(?:[\.,]\d+)?)\s*(?:\+)?\s*(?:năm|nam|years?|yrs?)",
    re.IGNORECASE,
)
_AT_LEAST_PATTERN   = re.compile(
    r"(?:ít nhất|tối thiểu|>=|>\s*=?|from|at least|minimum|trên)\s*(\d+(?:[\.,]\d+)?)\s*(?:\+)?\s*(?:năm|nam|years?|yrs?)",
    re.IGNORECASE,
)
_MAX_ONLY_PATTERN   = re.compile(
    r"(?:tối đa|<=|<\s*=?|dưới|up to)\s*(\d+(?:[\.,]\d+)?)\s*(?:năm|nam|years?|yrs?)",
    re.IGNORECASE,
)
_SINGLE_EXP_PATTERN = re.compile(
    r"(\d+(?:[\.,]\d+)?)\s*(?:\+)?\s*(?:năm|nam|years?|yrs?)",
    re.IGNORECASE,
)


def _parse_year_number(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value.replace(",", "."))
    except ValueError:
        return None


def parse_experience(text: str | None):
    if not text or not text.strip():
        return (None, None, None, "missing")
    lowered = text.strip().lower()
    if any(kw in lowered for kw in _NO_EXP_KEYWORDS):
        return (0.0, 0.0, "none", "tag")
    m = _EXP_RANGE_PATTERN.search(lowered)
    if m:
        mn, mx = _parse_year_number(m.group(1)), _parse_year_number(m.group(2))
        return (mn, mx, "range", "parsed" if None not in (mn, mx) else "partial")
    m = _AT_LEAST_PATTERN.search(lowered)
    if m:
        mn = _parse_year_number(m.group(1))
        return (mn, None, "at_least", "parsed" if mn is not None else "partial")
    m = _MAX_ONLY_PATTERN.search(lowered)
    if m:
        mx = _parse_year_number(m.group(1))
        return (None, mx, "upto", "parsed" if mx is not None else "partial")
    m = _SINGLE_EXP_PATTERN.search(lowered)
    if m:
        v = _parse_year_number(m.group(1))
        return (v, v, "single", "parsed" if v is not None else "partial")
    digits = re.findall(r"\d+(?:[\.,]\d+)?", lowered)
    if len(digits) == 1:
        v = _parse_year_number(digits[0])
        return (v, v, "single", "assumed_years" if v is not None else "partial")
    if len(digits) >= 2:
        mn, mx = _parse_year_number(digits[0]), _parse_year_number(digits[1])
        return (mn, mx, "range", "assumed_years" if None not in (mn, mx) else "partial")
    return (None, None, None, "unparsed")

parse_experience_udf = F.udf(parse_experience, _experience_schema)


# ─────────────────────────────────────────────────────────────────────────────
# Expired date parsing
# ─────────────────────────────────────────────────────────────────────────────

_DMY_PATTERN = re.compile(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})")


def _extract_date_fragment(text: str) -> str:
    if "hạn nộp hồ sơ" in text.lower():
        return text.split(":")[-1].strip()
    return text


def parse_expired_date(text: str | None):
    if text is None:
        return None
    fragment = _extract_date_fragment(text.strip())
    try:
        return datetime.fromisoformat(fragment.replace("Z", "+00:00")).date()
    except ValueError:
        m = _DMY_PATTERN.search(fragment)
        if not m:
            return None
        day, month, year = m.groups()
        try:
            return datetime(int(year), int(month), int(day)).date()
        except ValueError:
            return None

parse_expired_date_udf = F.udf(parse_expired_date, DateType())


# ─────────────────────────────────────────────────────────────────────────────
# Normalize for matching (remove diacritics → lowercase ASCII)
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_for_matching(text: str | None) -> str | None:
    if text is None:
        return None
    nfd = unicodedata.normalize("NFD", text)
    stripped = "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")
    stripped = stripped.replace("đ", "d").replace("Đ", "D")
    normalized = re.sub(r"\s+", " ", stripped).strip().lower()
    return normalized or None


# ─────────────────────────────────────────────────────────────────────────────
# Level categorization
# ─────────────────────────────────────────────────────────────────────────────

_LEVEL_RULES = [
    ("executive", (
        "tổng giám đốc",
        "giám đốc và cấp cao hơn",
        "giám đốc / cấp quản lý cao cấp",
        "giám đốc",
        "phó giám đốc",
    )),
    ("senior_manager", (
        "trưởng/phó phòng",
        "trưởng phòng",
        "phó phòng",
        "trưởng bộ phận",
    )),
    ("manager", (
        "quản lý / giám đốc",
        "quản lý",
    )),
    ("lead_supervisor", (
        "trưởng nhóm / giám sát",
        "quản lý / giám sát",
        "trưởng nhóm",
        "giám sát",
    )),
    ("staff", (
        "chuyên viên",
        "nhân viên",
    )),
    ("intern", (
        "thực tập sinh/sinh viên",
        "sinh viên/ thực tập sinh",
        "thực tập sinh",
        "sinh viên",
    )),
    ("fresher", (
        "mới tốt nghiệp",
    )),
]

_LEVEL_RULES_NORMALIZED = [
    (group, tuple(filter(None, (_normalize_for_matching(k) for k in keywords))))
    for group, keywords in _LEVEL_RULES
]

_level_schema = StructType([
    StructField("level_group",        StringType(), True),
    StructField("level_parse_status", StringType(), True),
    StructField("level_keyword",      StringType(), True),
])


def categorize_level(text: str | None):
    cleaned = normalize_common(text)
    if not cleaned:
        return (None, "missing", None)
    normalized = _normalize_for_matching(cleaned)
    if not normalized:
        return (None, "missing", None)
    for group, keywords in _LEVEL_RULES_NORMALIZED:
        for kw in keywords:
            if kw and kw in normalized:
                return (group, "matched", kw)
    return (None, "unmapped", None)

categorize_level_udf = F.udf(categorize_level, _level_schema)


# ─────────────────────────────────────────────────────────────────────────────
# Education categorization
# ─────────────────────────────────────────────────────────────────────────────

_EDUCATION_RULES = [
    ("0",     ("trung học", "bất kỳ")),
    ("5",     ("tiến sĩ",)),
    ("4",     ("thạc sĩ", "sau đại học")),
    ("3",     ("cử nhân", "đại học")),
    ("2",     ("cao đẳng",)),
    ("1",     ("trung cấp", "nghề")),
    ("other", ("khác",)),
]

_EDUCATION_RULES_NORMALIZED = [
    (group, tuple(filter(None, (_normalize_for_matching(k) for k in keywords))))
    for group, keywords in _EDUCATION_RULES
]

_education_schema = StructType([
    StructField("education_group",        StringType(), True),
    StructField("education_parse_status", StringType(), True),
    StructField("education_keyword",      StringType(), True),
])


def categorize_education(text: str | None):
    cleaned = normalize_common(text)
    if not cleaned:
        return (None, "missing", None)
    normalized = _normalize_for_matching(cleaned)
    if not normalized:
        return (None, "missing", None)
    for group, keywords in _EDUCATION_RULES_NORMALIZED:
        for kw in keywords:
            if kw and kw in normalized:
                return (group, "matched", kw)
    return (None, "unmapped", None)

categorize_education_udf = F.udf(categorize_education, _education_schema)


# ─────────────────────────────────────────────────────────────────────────────
# Work form categorization
# ─────────────────────────────────────────────────────────────────────────────

_PART_TIME_KEYWORDS  = ("ban thoi gian", "viec lam online")
_INTERNSHIP_KEYWORDS = ("thuc tap",)
_OTHER_WORKFORMS     = ("khac",)


def categorize_work_form(text: str | None) -> str | None:
    cleaned = normalize_common(text)
    if not cleaned:
        return None
    normalized = _normalize_for_matching(cleaned)
    if not normalized:
        return None
    if any(kw in normalized for kw in _INTERNSHIP_KEYWORDS):
        return "internship"
    if any(kw in normalized for kw in _PART_TIME_KEYWORDS):
        return "part_time"
    if any(kw in normalized for kw in _OTHER_WORKFORMS):
        return "other"
    return "full_time"

categorize_work_form_udf = F.udf(categorize_work_form, StringType())


# ─────────────────────────────────────────────────────────────────────────────
# Quantity normalizer
# ─────────────────────────────────────────────────────────────────────────────

_QUANTITY_PATTERN = re.compile(r"(\d+(?:[.,]\d+)?)")


def normalize_quantity(value: str | None) -> float | None:
    if value is None:
        return 1.0
    m = _QUANTITY_PATTERN.search(value.lower())
    if not m:
        return 1.0
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return 1.0

normalize_quantity_udf = F.udf(normalize_quantity, DoubleType())

# ─────────────────────────────────────────────────────────────────────────────
# Category rule-based mapping  (platform category → chuẩn VSIC Việt Nam)
# ─────────────────────────────────────────────────────────────────────────────

_VN_SECTOR = {
    "AGRICULTURE":   "NÔNG NGHIỆP, LÂM NGHIỆP VÀ THỦY SẢN",
    "MINING":        "KHAI KHOÁNG",
    "MANUFACTURING": "CÔNG NGHIỆP CHẾ BIẾN, CHẾ TẠO",
    "UTILITIES":     "SẢN XUẤT VÀ PHÂN PHỐI ĐIỆN, KHÍ ĐỐT, NƯỚC NÓNG, HƠI NƯỚC VÀ ĐIỀU HOÀ KHÔNG KHÍ",
    "WATER_WASTE":   "CUNG CẤP NƯỚC; HOẠT ĐỘNG QUẢN LÝ VÀ XỬ LÝ RÁC THẢI, NƯỚC THẢI",
    "CONSTRUCTION":  "XÂY DỰNG",
    "RETAIL":        "BÁN BUÔN VÀ BÁN LẺ",
    "TRANSPORT":     "VẬN TẢI, KHO BÃI",
    "HOSPITALITY":   "DỊCH VỤ LƯU TRÚ VÀ ĂN UỐNG",
    "MEDIA":         "HOẠT ĐỘNG XUẤT BẢN, PHÁT SÓNG, SẢN XUẤT VÀ PHÂN PHỐI NỘI DUNG",
    "ICT":           "HOẠT ĐỘNG VIỄN THÔNG; LẬP TRÌNH MÁY TÍNH, TƯ VẤN, CƠ SỞ HẠ TẦNG MÁY TÍNH VÀ CÁC DỊCH VỤ THÔNG TIN KHÁC",
    "FINANCE":       "HOẠT ĐỘNG TÀI CHÍNH, NGÂN HÀNG VÀ BẢO HIỂM",
    "REALESTATE":    "HOẠT ĐỘNG KINH DOANH BẤT ĐỘNG SẢN",
    "PROFESSIONAL":  "HOẠT ĐỘNG CHUYÊN MÔN, KHOA HỌC VÀ CÔNG NGHỆ",
    "ADMIN":         "HOẠT ĐỘNG HÀNH CHÍNH VÀ DỊCH VỤ HỖ TRỢ",
    "PUBLIC":        "HOẠT ĐỘNG CỦA ĐẢNG CỘNG SẢN, TỔ CHỨC CHÍNH TRỊ - XÃ HỘI, QUẢN LÝ NHÀ NƯỚC, AN NINH QUỐC PHÒNG; BẢO ĐẢM XÃ HỘI BẮT BUỘC",
    "EDUCATION":     "GIÁO DỤC VÀ ĐÀO TẠO",
    "HEALTH":        "Y TẾ VÀ HOẠT ĐỘNG TRỢ GIÚP XÃ HỘI",
    "ARTS":          "NGHỆ THUẬT, THỂ THAO VÀ GIẢI TRÍ",
    "OTHER_SVC":     "HOẠT ĐỘNG DỊCH VỤ KHÁC",
    "HOUSEHOLD":     "HOẠT ĐỘNG LÀM THUÊ CÁC CÔNG VIỆC TRONG CÁC HỘ GIA ĐÌNH, SẢN XUẤT SẢN PHẨM VẬT CHẤT VÀ DỊCH VỤ TỰ TIÊU DÙNG CỦA HỘ GIA ĐÌNH",
    "INTERNATIONAL": "HOẠT ĐỘNG CỦA CÁC TỔ CHỨC VÀ CƠ QUAN QUỐC TẾ",
}

S = _VN_SECTOR  # shorthand

_CATEGORY_RULES: dict[str, list[tuple[str, str]]] = {

    # ── TopCV ─────────────────────────────────────────────────────────────
    "topcv": [
        ("nhan vien kinh doanh", S["RETAIL"]),
        ("ke toan",              S["FINANCE"]),
        ("marketing",            S["RETAIL"]),
        ("hanh chinh nhan su",   S["ADMIN"]),
        ("cham soc khach hang",  S["ADMIN"]),
        ("ngan hang",            S["FINANCE"]),
        ("it",                   S["ICT"]),
        ("lao dong pho thong",   S["OTHER_SVC"]),
        ("senior",               S["OTHER_SVC"]),
        ("ky su xay dung",       S["CONSTRUCTION"]),
        ("thiet ke do hoa",      S["ARTS"]),
        ("bat dong san",         S["REALESTATE"]),
        ("giao duc",             S["EDUCATION"]),
        ("telesales",            S["RETAIL"]),
    ],

    # ── CareerViet ────────────────────────────────────────────────────────
    "careerviet": [
        ("ban hang",            S["RETAIL"]),
        ("tiep thi",            S["RETAIL"]),
        ("cham soc suc khoe",   S["HEALTH"]),
        ("hang tieu dung",      S["RETAIL"]),
        ("hanh chinh",          S["ADMIN"]),
        ("nhan su",             S["ADMIN"]),
        ("ke toan",             S["FINANCE"]),
        ("tai chinh",           S["FINANCE"]),
        ("cong nghe thong tin", S["ICT"]),
        ("may tinh",            S["ICT"]),
        ("truyen thong",        S["MEDIA"]),
        ("media",               S["MEDIA"]),
        ("giao duc",            S["EDUCATION"]),
        ("dao tao",             S["EDUCATION"]),
        ("khoa hoc ky thuat",   S["PROFESSIONAL"]),
        ("khoa hoc",            S["PROFESSIONAL"]),
        ("ky thuat",            S["PROFESSIONAL"]),
        ("khach san",           S["HOSPITALITY"]),
        ("du lich",             S["HOSPITALITY"]),
        ("san xuat",            S["MANUFACTURING"]),
        ("xay dung",            S["CONSTRUCTION"]),
        ("dich vu",             S["OTHER_SVC"]),
        ("nhom nganh khac",     S["OTHER_SVC"]),
    ],

    # ── VietnamWorks ──────────────────────────────────────────────────────
    "vietnamworks": [
        ("giao duc",             S["EDUCATION"]),
        ("ke toan",              S["FINANCE"]),
        ("kiem toan",            S["FINANCE"]),
        ("ngan hang",            S["FINANCE"]),
        ("bao hiem",             S["FINANCE"]),
        ("dich vu tai chinh",    S["FINANCE"]),
        ("hanh chinh van phong", S["ADMIN"]),
        ("nhan su",              S["ADMIN"]),
        ("tuyen dung",           S["ADMIN"]),
        ("ceo",                  S["ADMIN"]),
        ("general management",   S["ADMIN"]),
        ("dich vu khach hang",   S["ADMIN"]),
        ("nong lam ngu nghiep",  S["AGRICULTURE"]),
        ("kien truc",            S["CONSTRUCTION"]),
        ("xay dung",             S["CONSTRUCTION"]),
        ("nghe thuat",           S["MEDIA"]),
        ("in an",                S["MEDIA"]),
        ("xuat ban",             S["MEDIA"]),
        ("truyen thong",         S["MEDIA"]),
        ("quang cao",            S["MEDIA"]),
        ("tiep thi",             S["RETAIL"]),
        ("cong nghe thong tin",  S["ICT"]),
        ("vien thong",           S["ICT"]),
        ("thiet ke",             S["ARTS"]),
        ("khoa hoc",             S["PROFESSIONAL"]),
        ("ky thuat",             S["PROFESSIONAL"]),
        ("phap ly",              S["PROFESSIONAL"]),
        ("dich vu an uong",      S["HOSPITALITY"]),
        ("nha hang",             S["HOSPITALITY"]),
        ("khach san",            S["HOSPITALITY"]),
        ("phi loi nhuan",        S["PUBLIC"]),
        ("chinh phu",            S["PUBLIC"]),
        ("y te",                 S["HEALTH"]),
        ("cham soc suc khoe",    S["HEALTH"]),
        ("duoc",                 S["HEALTH"]),
        ("bat dong san",         S["REALESTATE"]),
        ("hau can",              S["TRANSPORT"]),
        ("xuat nhap khau",       S["TRANSPORT"]),
        ("kho bai",              S["TRANSPORT"]),
        ("van tai",              S["TRANSPORT"]),
        ("san xuat",             S["MANUFACTURING"]),
        ("det may",              S["MANUFACTURING"]),
        ("da giay",              S["MANUFACTURING"]),
        ("ban le",               S["RETAIL"]),
        ("tieu dung",            S["RETAIL"]),
        ("kinh doanh",           S["RETAIL"]),
        ("khac",                 S["OTHER_SVC"]),
    ],
}


def categorize_category_by_rule(platform: str | None, category: str | None) -> str | None:
    if not platform or not category:
        return None
    rules = _CATEGORY_RULES.get(platform.strip().lower())
    if not rules:
        return None
    cat_norm = _normalize_for_matching(category)
    if not cat_norm:
        return None
    for keyword, sector in rules:
        if keyword in cat_norm:
            return sector
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Prepare source DataFrame
# ─────────────────────────────────────────────────────────────────────────────

def _prepare_source(df: DataFrame, platform: str) -> DataFrame:
    base_columns = (
        "title", "salary", "location", "experience", "expired_date",
        "company", "job_description", "level", "education",
        "quantity", "work_form", "skills", "category", "link",
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

    from pyspark.sql.types import ArrayType as SparkArrayType
    if isinstance(df.schema["skills"].dataType, SparkArrayType):
        df = df.withColumn("skills", F.col("skills").cast(ArrayType(StringType())))
    else:
        logger.info("START TECH NER")

        df = df.withColumn(
            "skills",
            F.when(F.col("skills").isNull(), F.array().cast(ArrayType(StringType())))
             .otherwise(F.array(F.col("skills").cast(StringType())))
        )

        logger.info("END TECH NER")

    return (
        df
        .withColumn("platform",        F.lit(platform))
        .withColumn("title",           F.col("title").cast(StringType()))
        .withColumn("salary",          F.col("salary").cast(StringType()))
        .withColumn("location",        F.col("location").cast(StringType()))
        .withColumn("experience",      F.col("experience").cast(StringType()))
        .withColumn("expired_date",    F.col("expired_date").cast(StringType()))
        .withColumn("company",         F.col("company").cast(StringType()))
        .withColumn("job_description", F.col("job_description").cast(StringType()))
        .withColumn("level",           F.col("level").cast(StringType()))
        .withColumn("education",       F.col("education").cast(StringType()))
        .withColumn("quantity",        F.col("quantity").cast(StringType()))
        .withColumn("work_form",       F.col("work_form").cast(StringType()))
        .withColumn("category",        F.col("category").cast(StringType()))
        .withColumn("link",            F.col("link").cast(StringType()))
    )


# ─────────────────────────────────────────────────────────────────────────────
# File tracker / batch loading
# ─────────────────────────────────────────────────────────────────────────────

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
    safe_path   = file_path.replace("'", "''")
    safe_status = new_status.replace("'", "''")
    try:
        spark.sql(f"""
            UPDATE nessie.meta.file_tracker
            SET status = '{safe_status}', updated_at = current_timestamp()
            WHERE file_path = '{safe_path}' AND status = 'pending'
        """)
    except Exception as e:
        print(f"Failed to update status for {file_path}: {e}")


def load_batch(spark: SparkSession, files) -> DataFrame:
    dfs = []
    for row in files:
        file_path = row["file_path"]
        platform  = row["platform"]
        try:
            df_raw = spark.read.parquet(file_path)
            if len(df_raw.take(1)) == 0:
                print(f"Skipping empty file: {file_path}")
                update_status_by_file_path(spark, file_path, "failed")
                continue
            if "quantity" in df_raw.columns:
                df_raw = df_raw.withColumn("quantity", F.col("quantity").cast(StringType()))
            else:
                df_raw = df_raw.withColumn("quantity", F.lit("1"))
            dfs.append(_prepare_source(df_raw, platform))
        except Exception as e:
            print(f"Failed to read {file_path}: {e}")
            update_status_by_file_path(spark, file_path, "failed")

    if not dfs:
        raise RuntimeError("Không tìm thấy hoặc đọc được bất kỳ file nào trong batch.")

    df_union = reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        dfs,
    )

    # Dedup trong batch theo link
    df_union = df_union.withColumn("link", F.col("link").cast(StringType()))
    link_window = Window.partitionBy("link").orderBy(
        F.col("expired_date").desc_nulls_last(),
        F.col("title").asc_nulls_last(),
    )
    return (
        df_union
        .withColumn("rn_link_batch", F.row_number().over(link_window))
        .filter((F.col("link").isNull()) | (F.col("rn_link_batch") == 1))
        .drop("rn_link_batch")
    )


# ─────────────────────────────────────────────────────────────────────────────
# Standardize
# ─────────────────────────────────────────────────────────────────────────────

def standardize_jobs(df_union: DataFrame) -> DataFrame:
    df_std = (
        df_union
        .withColumn("title_clean",    normalize_common_udf(F.col("title")))
        .withColumn("location_clean", normalize_common_udf(F.col("location")))
        .withColumn("company_clean",  normalize_common_udf(F.col("company")))

        # ── Salary ──────────────────────────────────────────────────────────
        .withColumn("salary_struct",       parse_salary_udf(F.col("salary")))
        .withColumn("min_salary",          F.col("salary_struct.min_salary"))
        .withColumn("max_salary",          F.col("salary_struct.max_salary"))
        .withColumn("currency",            F.col("salary_struct.currency"))
        .withColumn("salary_type",         F.col("salary_struct.salary_type"))
        .withColumn("salary_parse_status", F.col("salary_struct.parse_status"))
        .withColumn("salary_pattern_id",   F.col("salary_struct.pattern_id"))
        .withColumn(
            "currency",
            F.when(F.col("salary_type") == "negotiable", F.lit("VND"))
             .otherwise(F.col("currency"))
        )

        # ── Experience ──────────────────────────────────────────────────────
        .withColumn("experience_struct",       parse_experience_udf(F.col("experience")))
        .withColumn("min_years",               F.col("experience_struct.min_years"))
        .withColumn("max_years",               F.col("experience_struct.max_years"))
        .withColumn("experience_type",         F.col("experience_struct.experience_type"))
        .withColumn("experience_parse_status", F.col("experience_struct.parse_status"))
        .withColumn("min_years",
            F.when(F.col("min_years").isNull(), F.lit(0.0)).otherwise(F.col("min_years")))
        .withColumn("max_years",
            F.when(F.col("max_years").isNull(), F.lit(0.0)).otherwise(F.col("max_years")))
        .withColumn("experience_type",
            F.when(
                (F.col("min_years") == 0.0) & (F.col("max_years") == 0.0) & F.col("experience_type").isNull(),
                F.lit("none")
            ).otherwise(F.col("experience_type")))

        # ── Expired date ────────────────────────────────────────────────────
        .withColumn("expired_date_norm", parse_expired_date_udf(F.col("expired_date")))

        # ── Level ───────────────────────────────────────────────────────────
        .withColumn("level_struct",       categorize_level_udf(F.col("level")))
        .withColumn("level_standard",     F.col("level_struct.level_group"))
        .withColumn("level_parse_status", F.col("level_struct.level_parse_status"))
        .withColumn("level_keyword",      F.col("level_struct.level_keyword"))

        # ── Education ───────────────────────────────────────────────────────
        .withColumn("education_struct",        categorize_education_udf(F.col("education")))
        .withColumn("education_standard",      F.col("education_struct.education_group"))
        .withColumn("education_parse_status",  F.col("education_struct.education_parse_status"))
        .withColumn("education_keyword",       F.col("education_struct.education_keyword"))
        .withColumn("education_standard",
            F.coalesce(F.col("education_standard"), F.lit("0")))

        # ── Work form ───────────────────────────────────────────────────────
        .withColumn("work_form",          F.col("work_form").cast(StringType()))
        .withColumn("work_form_standard", categorize_work_form_udf(F.col("work_form")))

        # ── Quantity ────────────────────────────────────────────────────────
        .withColumn("quantity_normalized", normalize_quantity_udf(F.col("quantity")))

        .drop("salary_struct", "experience_struct", "level_struct", "education_struct")
    )

    return df_std.filter(
        F.col("title_clean").isNotNull() & (F.col("title_clean") != "")
    )


# ─────────────────────────────────────────────────────────────────────────────
# NER: preprocessing
# ─────────────────────────────────────────────────────────────────────────────

# Các thuật ngữ cần giữ nguyên trước khi tách ký tự đặc biệt
_KEEP_INTACT_TERMS = [
    "B2B/B2C", "B2B", "B2C",
    "QA/QC",
    "R&D",
    "F&B",
    "MEP", "MEPF",
    "LV/MV", "MV/LV", "MV/LV/ELV",
    "PCCC",
]


def preprocess_jd(text: str) -> str:
    """
    Tiền xử lý text JD trước khi đưa vào NER:
    - Bảo vệ các thuật ngữ ghép (B2B/B2C, R&D, …) bằng placeholder
    - Tách dấu đặc biệt (/ & – , . ; : () []) thành token riêng
    - Chuẩn hóa khoảng trắng
    """
    # Step 1: Placeholder các thuật ngữ cần giữ nguyên
    placeholders = {}
    for i, term in enumerate(_KEEP_INTACT_TERMS):
        pattern     = re.compile(re.escape(term), re.IGNORECASE)
        placeholder = f"__TERM{i}__"
        matches     = pattern.findall(text)
        if matches:
            placeholders[placeholder] = matches[0]  # giữ casing gốc
            text = pattern.sub(placeholder, text)

    # Step 2: Tách ký tự đặc biệt thành token riêng
    text = re.sub(r"/",       " / ",  text)
    text = re.sub(r"&",       " & ",  text)
    text = re.sub(r"[–—]",   " – ",  text)

    # Step 3: Dấu câu có khoảng trắng xung quanh
    text = re.sub(r"([.,;:()\[\]\"'])", r" \1 ", text)

    # Step 4: Chuẩn hóa khoảng trắng
    text = re.sub(r"\s+", " ", text).strip()

    # Step 5: Restore placeholders
    for placeholder, original in placeholders.items():
        text = text.replace(placeholder, original)

    # Step 6: Cleanup lại sau restore
    return re.sub(r"\s+", " ", text).strip()


# ─────────────────────────────────────────────────────────────────────────────
# NER: model loading & extraction
# ─────────────────────────────────────────────────────────────────────────────

def load_ner_pipeline():
    """Load model NER từ HuggingFace Hub: zikay3624/careerlake-ner-skill."""
    tokenizer = AutoTokenizer.from_pretrained(NER_MODEL_NAME)
    model     = AutoModelForTokenClassification.from_pretrained(NER_MODEL_NAME)
    return pipeline(
        "token-classification",
        model=model,
        tokenizer=tokenizer,
        aggregation_strategy="simple",
    )


_START_PATTERNS = [
    # Cứng - tiêu đề section rõ ràng
    r"YÊU\s*CẦU\s*ỨNG\s*VIÊN",
    r"Yêu\s*[Cc]ầu\s*[Ứứ]ng\s*[Vv]iên",
    r"YÊU\s*CẦU\s*CÔNG\s*VIỆC",
    r"Yêu\s*[Cc]ầu\s*[Cc]ông\s*[Vv]iệc",
    r"YÊU\s*CẦU",
    r"TIÊU\s*CHUẨN\s*TUYỂN\s*DỤNG",
    r"Tiêu\s*[Cc]huẩn\s*[Tt]uyển\s*[Dd]ụng",
    r"TIÊU\s*CHUẨN",
    r"Tiêu\s*[Cc]huẩn",
    r"YÊU\s*CẦU\s*KỸ\s*NĂNG",
    r"Yêu\s*[Cc]ầu\s*[Kk]ỹ\s*[Nn]ăng",
    r"ĐIỀU\s*KIỆN",
    r"Điều\s*[Kk]iện",
    r"TRÌNH\s*ĐỘ\s*YÊU\s*CẦU",
    r"Trình\s*[Đđ]ộ\s*[Yy]êu\s*[Cc]ầu",
    r"JOB\s*REQUIREMENTS?",
    r"REQUIREMENTS?",
    r"QUALIFICATIONS?",
    r"[Rr]equirements?",
    r"[Qq]ualifications?",
    r"SKILLS?\s*(?:REQUIRED|NEEDED)?",
    r"Skills?\s*(?:Required|Needed)?",
    r"[Rr]equired\s*[Ss]kills?",
    # Mềm - theo sau dấu hai chấm thường thấy trong dữ liệu mẫu
    r"(?:^|\n)\s*:\s*(?=\S)",  # dòng bắt đầu bằng ": " (pattern thấy trong data mẫu)
]

_END_PATTERNS = [
    r"QUYỀN\s*LỢI",
    r"[Qq]uyền\s*[Ll]ợi",
    r"PHÚC\s*LỢI",
    r"[Pp]húc\s*[Ll]ợi",
    r"THU\s*NHẬP",
    r"[Tt]hu\s*[Nn]hập",
    r"ĐỊA\s*ĐIỂM\s*LÀM\s*VIỆC",
    r"[Đđ]ịa\s*[Đđ]iểm\s*[Ll]àm\s*[Vv]iệc",
    r"THÔNG\s*TIN\s*KHÁC",
    r"[Tt]hông\s*[Tt]in\s*[Kk]hác",
    r"CÁC\s*PHÚC\s*LỢI",
    r"[Cc]ác\s*[Pp]húc\s*[Ll]ợi",
    r"MÔ\s*TẢ\s*CÔNG\s*VIỆC",   # Tránh lấy ngược vào desc
    r"[Mm]ô\s*[Tt]ả\s*[Cc]ông\s*[Vv]iệc",
    r"BENEFITS?",
    r"[Bb]enefits?",
    r"COMPENSATION",
    r"[Cc]ompensation",
    r"WHY\s*(?:JOIN|US)",
    r"[Ww]hy\s*(?:[Jj]oin|[Uu]s)",
    r"WORKING\s*(?:HOURS?|LOCATION|ENVIRONMENT)",
    r"[Ww]orking\s*(?:[Hh]ours?|[Ll]ocation|[Ee]nvironment)",
]

# Pattern dùng để tách thẳng khi không tìm được header
_SPLIT_FALLBACK_PATTERNS = [
    # Hai section cách nhau bằng dấu gạch ngang hoặc dòng trống kép
    r"\n{3,}",
    r"\n[-─═]{5,}\n",
]
def extract_qualification(text: str) -> str:
    if not isinstance(text, str) or not text.strip():
        return ""
    best_start_match = None
    best_start_pos   = len(text) + 1

    for pat in _START_PATTERNS:
        m = re.search(pat, text)
        if m and m.start() < best_start_pos:
            # Ưu tiên header xuất hiện sớm nhất & có độ dài hợp lý
            # Bỏ qua nếu match ngay đầu chuỗi (thường là toàn bộ text là requirement)
            if m.start() > 20 or len(_START_PATTERNS) <= 1:
                best_start_pos   = m.start()
                best_start_match = m
    
    if best_start_match:
        # Lấy content SAU header (bỏ qua ký tự phân cách như ":"  "–" ngay sau)
        content_start = best_start_match.end()
        # Bỏ qua dấu câu và khoảng trắng ngay sau header
        skip = re.match(r"[\s:*\-–—|]+", text[content_start:])
        if skip:
            content_start += skip.end()

        # Tìm điểm kết thúc
        end_pos = len(text)
        for end_pat in _END_PATTERNS:
            m_end = re.search(end_pat, text[content_start:])
            if m_end:
                candidate = content_start + m_end.start()
                if candidate < end_pos and candidate > content_start + 10:
                    end_pos = candidate

        extracted = text[content_start:end_pos].strip()
        if len(extracted) > 30:   # đủ dài để có nghĩa
            return extracted
    
    for sep_pat in _SPLIT_FALLBACK_PATTERNS:
        parts = re.split(sep_pat, text)
        if len(parts) >= 2:
            # Chọn phần dài nhất không phải phần đầu (thường là desc)
            candidate_parts = sorted(parts[1:], key=len, reverse=True)
            for part in candidate_parts:
                part = part.strip()
                if len(part) > 50:
                    return part
                
    return text.strip()

def _insert_space_lower_upper(text: str) -> str:
    """Thêm khoảng trắng giữa chữ thường và chữ hoa liền nhau (camelCase → camel Case)."""
    if not text:
        return text
    chars = list(text)
    out   = [chars[0]]
    for i in range(1, len(chars)):
        if chars[i - 1].islower() and chars[i].isupper():
            out.append(" ")
        out.append(chars[i])
    return "".join(out)


def preprocess_requirement(text: str) -> str:
    """Làm sạch đoạn text requirement trước khi đưa vào NER."""
    if not isinstance(text, str):
        return ""
    text = re.sub(r"</?p>|</?strong>",               " ",  text, flags=re.IGNORECASE)
    text = re.sub(r"^[\-\–]\s*",                      "",   text, flags=re.MULTILINE)
    text = re.sub(r"[➢▸ü·\r\t•●▪◦\*\+]+",           " ",  text)
    text = re.sub(r"\n+",                             " ",  text)
    text = re.sub(r"\u2026+",                         ".",  text)
    text = re.sub(r"\.{2,}",                          ".",  text)
    text = re.sub(r"[()]",                            " ",  text)
    text = _insert_space_lower_upper(text)
    text = re.sub(r"([\.!?;:])",                      r" \1 ", text)
    text = re.sub(r"\b([A-Za-z]|[IVXLCM]+|\d+)\s*\.", " ", text)
    text = re.sub(r"\s{2,}",                          " ",  text)
    return text.strip()


VOWELS = set("aeiouy")


def is_reasonable_skill_token(s: str) -> bool:
    if not s:
        return False
    s_stripped = s.strip()
    if not s_stripped:
        return False
    s_norm  = unidecode.unidecode(s_stripped)
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
        if len(re.findall(r"[bcdfghjklmnpqrstvwxyz]", s_lower)) < 2:
            return False
        return True
    if len(s_lower.replace(" ", "")) < 4:
        return False
    if not any(len(w) >= 4 for w in words):
        return False
    return True


def normalize_and_split_skill_span(span: str) -> list[str]:
    if not isinstance(span, str):
        return []
    s = span.strip().strip("()[]\"' ")
    s = s.replace("–", "-").replace("—", "-").replace("…", ".")
    s = _insert_space_lower_upper(s)
    s = re.sub(r"^[\s\.,;:!?\-]+",  "", s)
    s = re.sub(r"[\s\.,;:!?\-]+$",  "", s)
    tokens = s.split()
    if len(tokens) > 1 and len(tokens[-1]) == 1 and tokens[-1].isupper():
        tokens = tokens[:-1]
    s = re.sub(r"\s{2,}", " ", " ".join(tokens))
    out = []
    for p in re.split(r"[,/;]", s):
        p = p.strip()
        if p and is_reasonable_skill_token(p):
            out.append(p)
    return out


def extract_skills_by_label(
    text: str,
    ner_pipe,
    label: str,
) -> list[str]:
    """
    Trích các entity có entity_group == label từ text đã được tiền xử lý.
    Dùng cho cả 'TechSkill' và 'SoftSkill'.
    """
    if not isinstance(text, str) or not text.strip():
        return []

    preprocessed = preprocess_jd(text)
    entities = ner_pipe(preprocessed)

    target_ents = sorted(
        [
            e for e in entities
            if (e.get("entity_group") or "").strip() == label
            and "start" in e and "end" in e
        ],
        key=lambda e: e["start"],
    )
    if not target_ents:
        return []

    # Merge các entity liền kề
    merged   = []
    cur_start = target_ents[0]["start"]
    cur_end   = target_ents[0]["end"]
    for ent in target_ents[1:]:
        s, e = int(ent["start"]), int(ent["end"])
        if re.fullmatch(r"[\s,;/\-]*", preprocessed[cur_end:s] or ""):
            cur_end = e
        else:
            merged.append(preprocessed[cur_start:cur_end])
            cur_start, cur_end = s, e
    merged.append(preprocessed[cur_start:cur_end])

    skills, seen = [], set()
    for raw_span in merged:
        for p in normalize_and_split_skill_span(raw_span):
            key = p.lower()
            if key not in seen:
                seen.add(key)
                skills.append(p)
    return skills

def merge_skill_lists(original, ner_list: list) -> list[str]:
    merged, seen = [], set()
    original_list = (
        original if isinstance(original, list)
        else re.split(r"[;,/|]", original) if isinstance(original, str)
        else []
    )
    for s in original_list + (ner_list if isinstance(ner_list, list) else []):
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


# ─────────────────────────────────────────────────────────────────────────────
# Regular UDFs (qualification extraction, category)
# ─────────────────────────────────────────────────────────────────────────────

extract_qualification_udf   = F.udf(extract_qualification,   StringType())
preprocess_requirement_udf  = F.udf(preprocess_requirement,  StringType())

def _categorize_category(platform, category):
    return categorize_category_by_rule(platform, category) or "HOẠT ĐỘNG DỊCH VỤ KHÁC"

categorize_category_udf = F.udf(_categorize_category, StringType())

# ─────────────────────────────────────────────────────────────────────────────
# Pandas UDFs for NER (model loaded once per worker process, cached globally)
# ─────────────────────────────────────────────────────────────────────────────

_NER_PIPE_CACHE = None

def _get_cached_ner_pipe():
    global _NER_PIPE_CACHE
    if _NER_PIPE_CACHE is None:
        _NER_PIPE_CACHE = load_ner_pipeline()
    return _NER_PIPE_CACHE


# @pandas_udf(ArrayType(StringType()))
# def extract_tech_skills_pudf(texts: pd.Series) -> pd.Series:
#     pipe = _get_cached_ner_pipe()
#     return texts.apply(
#         lambda x: extract_skills_by_label(x, pipe, "TechSkill")
#         if isinstance(x, str) and x.strip() else []
#     )


# @pandas_udf(ArrayType(StringType()))
# def extract_soft_skills_pudf(texts: pd.Series) -> pd.Series:
#     pipe = _get_cached_ner_pipe()
#     return texts.apply(
#         lambda x: extract_skills_by_label(x, pipe, "SoftSkill")
#         if isinstance(x, str) and x.strip() else []
#     )


@pandas_udf(ArrayType(StringType()))
def merge_skills_all_pudf(
    orig: pd.Series, tech: pd.Series, soft: pd.Series
) -> pd.Series:
    def _merge(o, t, s):
        return merge_skill_lists(merge_skill_lists(o, t), s)
    return pd.Series([_merge(o, t, s) for o, t, s in zip(orig, tech, soft)])


# ─────────────────────────────────────────────────────────────────────────────
# Enrich: NER skills + category (rule-based first, embedding fallback)
# ─────────────────────────────────────────────────────────────────────────────

def enrich_with_ner_and_category(df_std: DataFrame, spark: SparkSession) -> DataFrame:
    df = (
        df_std
        .withColumn("qualification_raw", extract_qualification_udf(F.col("job_description")))
        .withColumn("qualification",     preprocess_requirement_udf(F.col("qualification_raw")))
    )

    df = (
        df
        .withColumn("tech_skills_ner_raw", F.array().cast(ArrayType(StringType())))
        .withColumn("soft_skills_ner_raw", F.array().cast(ArrayType(StringType())))
    )

    df = (
        df
        .withColumn("tech_skills_ner", F.col("tech_skills_ner_raw"))
        .withColumn("soft_skills_ner", F.col("soft_skills_ner_raw"))
        .withColumn("tech_skills",     F.col("tech_skills_ner"))
        .withColumn("soft_skills",     F.col("soft_skills_ner"))
    )

    df = (
        df
        .withColumn(
            "skills_safe",
            F.when(F.col("skills").isNull(), F.array().cast(ArrayType(StringType())))
             .otherwise(F.col("skills").cast(ArrayType(StringType())))
        )
        .withColumn(
            "skills_all",
            merge_skills_all_pudf(
                F.col("skills_safe"),
                F.col("tech_skills_ner"),
                F.col("soft_skills_ner"),
            )
        )
        .drop("skills_safe")
    )

    df = df.withColumn(
        "category_name_final",
        categorize_category_udf(F.col("platform"), F.col("category"))
    )

    df = (
        df
        .withColumn("description", F.col("job_description"))
        .withColumn("requirement",  F.col("qualification"))
    )

    return df

# ─────────────────────────────────────────────────────────────────────────────
# Location normalization
# ─────────────────────────────────────────────────────────────────────────────

def normalize_location(jobs_df: DataFrame, spark: SparkSession) -> DataFrame:
    """Chuẩn hóa location về tên tỉnh thành chuẩn của Việt Nam."""
    vietnam_provinces = [
        "An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu",
        "Bắc Ninh", "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước",
        "Bình Thuận", "Cà Mau", "Cần Thơ", "Cao Bằng", "Đà Nẵng",
        "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp",
        "Gia Lai", "Hà Giang", "Hà Nam", "Hà Nội", "Hà Tĩnh",
        "Hải Dương", "Hải Phòng", "Hậu Giang", "Hòa Bình", "Hưng Yên",
        "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng",
        "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An",
        "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình",
        "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng",
        "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên", "Thanh Hóa",
        "Thừa Thiên Huế", "Tiền Giang", "Hồ Chí Minh", "Trà Vinh",
        "Tuyên Quang", "Vĩnh Long", "Vĩnh Phúc", "Yên Bái",
    ]

    unidecode_udf = F.udf(lambda x: unidecode.unidecode(x) if x else None, StringType())

    vietnam_provinces_df = (
        spark.createDataFrame(vietnam_provinces, "string").toDF("province")
        .withColumn("province_clean", unidecode_udf(F.col("province")))
        .withColumn("province_clean", F.regexp_replace(F.col("province_clean"), r"[^\w\s]", ""))
        .withColumn("province_clean", F.regexp_replace(F.col("province_clean"), r"\s+", " "))
        .withColumn("province_clean", F.trim(F.col("province_clean")))
        .withColumn("province_clean", F.lower(F.col("province_clean")))
    )

    jobs_norm_df = (
        jobs_df
        .withColumn("location_clean", unidecode_udf(F.col("location_clean")))
        .withColumn("location_clean", F.regexp_replace(F.col("location_clean"), r"[^\w\s]", ""))
        .withColumn("location_clean", F.regexp_replace(F.col("location_clean"), r"\s+", " "))
        .withColumn("location_clean", F.trim(F.col("location_clean")))
        .withColumn("location_clean", F.lower(F.col("location_clean")))
    )

    vietnam_provinces_df.createOrReplaceTempView("provinces")
    jobs_norm_df.createOrReplaceTempView("jobs")

    return spark.sql("""
        WITH joined AS (
            SELECT
                j.*,
                p.province,
                ROW_NUMBER() OVER (
                    PARTITION BY j.platform, j.title_clean, j.location_clean
                    ORDER BY LENGTH(p.province_clean) DESC
                ) AS rn
            FROM jobs j
            LEFT JOIN provinces p
                ON j.location_clean LIKE CONCAT('%', p.province_clean, '%')
        )
        SELECT
            platform,
            title_clean,
            COALESCE(province, 'Khác') AS location_clean,
            company_clean,
            min_salary,
            max_salary,
            currency,
            salary_type,
            min_years,
            max_years,
            experience_type,
            education_standard,
            level_standard,
            work_form_standard,
            quantity_normalized,
            expired_date_norm,
            tech_skills,
            soft_skills,
            skills_all,
            category_name_final,
            description,
            requirement,
            link
        FROM joined
        WHERE rn = 1
    """)


# ─────────────────────────────────────────────────────────────────────────────
# Silver schema & append-only upsert
# ─────────────────────────────────────────────────────────────────────────────
#
# Silver layer design:
#   - Append-only: không overwrite bản ghi cũ
#   - Mỗi lần chạy chỉ insert record có link chưa tồn tại trong Silver
#   - job_key (sha256) là fingerprint nội dung → Gold dùng để phát hiện thay đổi
#   - processed_date ghi ngày xử lý batch
#
# Gold layer (SCD Type 1):
#   - LEFT ANTI JOIN Silver ON link để lấy record mới
#   - Link đã có trong Gold nhưng job_key đổi → UPDATE
#   - Kết quả: 1 bản ghi / link (latest state)

SILVER_COLS = [
    "job_key",              # content fingerprint (sha256)
    "processed_date",       # ngày xử lý batch
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
    "level_standard",
    "work_form_standard",
    "quantity_normalized",
    "expired_date_norm",
    "tech_skills",          # kỹ năng kỹ thuật (TechSkill từ NER)
    "soft_skills",          # kỹ năng mềm (SoftSkill từ NER)
    "skills_all",           # union tech + soft (backward compat)
    "category_name_final",
    "description",
    "requirement",
    "link",
]

_JOB_KEY_COLS = [
    "link",
    "title_clean",
    "expired_date_norm",
    "processed_date",
]


def _compute_job_key(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "job_key",
        F.sha2(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast(StringType()), F.lit("")) for c in _JOB_KEY_COLS],
            ),
            256,
        ),
    )


def append_to_silver(df_new: DataFrame, spark: SparkSession) -> None:
    df_new = df_new.withColumn("processed_date", F.current_date())
    df_new = _compute_job_key(df_new)
    df_new = df_new.select(*SILVER_COLS)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    try:
        df_existing  = spark.read.table(SILVER_TABLE)
        has_existing = True
    except AnalysisException:
        has_existing = False

    df_to_append = (
        df_new.join(df_existing.select("link").distinct(), on="link", how="left_anti")
        if has_existing else df_new
    )

    # ── FIX: cache trước khi count để saveAsTable không recompute NER ──
    df_to_append = df_to_append.persist()

    record_count = df_to_append.count()   # ← trigger NER một lần, kết quả được cache
    if record_count == 0:
        print("Không có record mới để append vào Silver.")
        df_to_append.unpersist()
        return

    print(f"Appending {record_count} new records to Silver.")
    (
        df_to_append                       # ← đọc từ cache, không tính lại NER
        .write
        .format("iceberg")
        .mode("append")
        .saveAsTable(SILVER_TABLE)
    )
    df_to_append.unpersist()


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    spark = build_spark_session()

    files            = get_batch_files(spark, 10)
    df_union         = load_batch(spark, files)
    df_standardized  = standardize_jobs(df_union)
    df_enriched      = enrich_with_ner_and_category(df_standardized, spark)
    df_enriched      = normalize_location(df_enriched, spark)

    append_to_silver(df_enriched, spark)

    for f in files:
        update_status_by_file_path(spark, f["file_path"], "processed")

    spark.stop()


if __name__ == "__main__":
    main()
