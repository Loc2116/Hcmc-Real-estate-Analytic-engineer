"""
Module: Dashboard/dashboard_analytics.py
Description: Pure data loading, normalization, and market analytics helpers for the
             HCMC Real Estate Streamlit Dashboard.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PARQUET_PATH = PROJECT_ROOT / "data_exports" / "mart_listings_clean.parquet"
DEFAULT_CSV_PATH = PROJECT_ROOT / "data_exports" / "mart_listings_clean.csv"

AREA_SEGMENT_ORDER = ["< 30 m²", "30 - 60 m²", "60 - 100 m²", "> 100 m²"]
LANE_TYPE_ORDER = ["Mặt tiền", "Hẻm ô tô", "Hẻm xe máy", "Chưa xác định"]

logger = logging.getLogger("dashboard_analytics")


def _load_env_file() -> None:
    """Load environment variables from .env if present."""
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_path)
    except ImportError:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("'\"")
                if k and k not in os.environ:
                    os.environ[k] = v


def format_district_label(value: Any) -> str:
    """Normalize raw district strings into consistent Vietnamese display names."""
    if pd.isna(value):
        return "Chưa xác định"
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "unknown"}:
        return "Chưa xác định"
    if text.isdigit():
        return f"Quận {int(text)}"
    if text.lower() == "thủ đức":
        return "TP. Thủ Đức"
    return text


def assign_area_segment(area_m2: pd.Series) -> pd.Categorical:
    """Bucket property area (m²) into standard market segments."""
    bins = [-np.inf, 30.0, 60.0, 100.0, np.inf]
    return pd.cut(
        pd.to_numeric(area_m2, errors="coerce"),
        bins=bins,
        labels=AREA_SEGMENT_ORDER,
        right=False,
    )


def prepare_listings_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Standardize column dtypes and derive analytics fields for dashboard consumption."""
    df = raw_df.copy()

    numeric_cols = [
        "price_billion_vnd",
        "area_m2",
        "price_per_m2",
        "floors",
        "bedrooms",
        "bathrooms",
        "total_rooms",
        "lane_width_m",
        "frontage_m",
        "duplicate_broker_count",
        "is_lane_type_missing",
        "is_lane_width_missing",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "crawled_date" in df.columns:
        df["crawled_date"] = pd.to_datetime(df["crawled_date"], errors="coerce").dt.date

    if "crawled_at" in df.columns:
        df["crawled_at"] = pd.to_datetime(df["crawled_at"], errors="coerce")

    for cat_col, fallback in [
        ("ward", "Chưa xác định"),
        ("street", "Chưa xác định"),
        ("lane_type", "Chưa xác định"),
        ("legal_status", "Không có"),
        ("furniture_status", "Trống / Nhà thô"),
    ]:
        if cat_col in df.columns:
            df[cat_col] = df[cat_col].fillna(fallback).astype(str).str.strip()
            df.loc[df[cat_col].isin(["", "nan", "None", "null"]), cat_col] = fallback

    if "district" in df.columns:
        df["district_label"] = df["district"].apply(format_district_label)
    else:
        df["district_label"] = "Chưa xác định"

    if "area_m2" in df.columns:
        df["area_segment"] = assign_area_segment(df["area_m2"])

    if "duplicate_broker_count" in df.columns:
        df["duplicate_broker_count"] = df["duplicate_broker_count"].fillna(1).astype(int)
        df["has_multiple_brokers"] = df["duplicate_broker_count"] > 1

    if "listing_url" not in df.columns and "link" in df.columns:
        df["listing_url"] = df["link"]

    return df


def load_mart_from_local(
    parquet_path: Path = DEFAULT_PARQUET_PATH,
    csv_path: Path = DEFAULT_CSV_PATH,
) -> Tuple[pd.DataFrame, str]:
    """
    Load mart_listings_clean from local Parquet (ignoring BigQuery dbdate extension
    metadata so pandas works without db-dtypes) with CSV fallback.
    """
    if parquet_path.exists():
        try:
            table = pq.read_table(parquet_path)
            df = table.to_pandas(ignore_metadata=True)
            return prepare_listings_df(df), f"Local Parquet ({parquet_path.name})"
        except Exception as exc:
            logger.warning("Failed to read Parquet '%s': %s. Falling back to CSV.", parquet_path, exc)

    if csv_path.exists():
        df = pd.read_csv(csv_path)
        return prepare_listings_df(df), f"Local CSV ({csv_path.name})"

    raise FileNotFoundError(
        f"Không tìm thấy dữ liệu local tại '{parquet_path}' hoặc '{csv_path}'. "
        "Hãy chạy `python export_mart.py` hoặc kết nối Google BigQuery."
    )


def load_mart_from_bigquery(
    project_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    credentials_info: Optional[Mapping[str, Any]] = None,
    credentials_path: Optional[str] = None,
) -> Tuple[pd.DataFrame, str]:
    """Query mart_listings_clean directly from Google BigQuery."""
    _load_env_file()
    from google.cloud import bigquery

    resolved_project = (project_id or os.getenv("GCP_PROJECT_ID", "")).strip()
    resolved_dataset = (dataset_id or os.getenv("BIGQUERY_DATASET", "hcmc_real_estate")).strip()

    client: bigquery.Client
    if credentials_info:
        from google.oauth2 import service_account

        creds = service_account.Credentials.from_service_account_info(dict(credentials_info))
        resolved_project = resolved_project or str(credentials_info.get("project_id", "")).strip()
        client = bigquery.Client(project=resolved_project or None, credentials=creds)
    else:
        cred_file = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
        if cred_file:
            cred_path_obj = Path(cred_file)
            if not cred_path_obj.is_absolute():
                cred_path_obj = PROJECT_ROOT / cred_path_obj
            if not cred_path_obj.exists():
                raise FileNotFoundError(f"BigQuery key file not found at '{cred_path_obj}'.")
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(cred_path_obj)
        elif not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            raise RuntimeError("Thiếu thông tin xác thực Google BigQuery (GOOGLE_APPLICATION_CREDENTIALS hoặc st.secrets).")

        if not resolved_project:
            raise RuntimeError("Thiếu biến môi trường GCP_PROJECT_ID.")
        client = bigquery.Client(project=resolved_project)

    full_table = f"{resolved_project}.{resolved_dataset}.mart_listings_clean"
    query = f"SELECT * FROM `{full_table}` ORDER BY crawled_date DESC, price_billion_vnd DESC"
    df = client.query(query).to_dataframe()
    return prepare_listings_df(df), f"Google BigQuery ({full_table})"


def load_mart_data(
    mode: str = "auto",
    credentials_info: Optional[Mapping[str, Any]] = None,
    project_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    parquet_path: Path = DEFAULT_PARQUET_PATH,
    csv_path: Path = DEFAULT_CSV_PATH,
) -> Tuple[pd.DataFrame, str, Optional[str]]:
    """
    Resilient loader supporting 'auto' (BigQuery live -> local snapshot fallback),
    'bigquery', or 'local'. Returns (df, source_label, fallback_warning).
    """
    if mode == "local":
        df, src = load_mart_from_local(parquet_path=parquet_path, csv_path=csv_path)
        return df, src, None

    if mode == "bigquery":
        df, src = load_mart_from_bigquery(
            project_id=project_id,
            dataset_id=dataset_id,
            credentials_info=credentials_info,
        )
        return df, src, None

    # Auto mode: prefer live BigQuery, fall back cleanly to local snapshot
    try:
        df, src = load_mart_from_bigquery(
            project_id=project_id,
            dataset_id=dataset_id,
            credentials_info=credentials_info,
        )
        return df, src, None
    except Exception as bq_exc:
        try:
            df, src = load_mart_from_local(parquet_path=parquet_path, csv_path=csv_path)
            warning = f"Đang dùng dữ liệu snapshot cục bộ do chưa kết nối BigQuery ({bq_exc})."
            return df, src, warning
        except FileNotFoundError as local_exc:
            raise RuntimeError(
                f"Không thể kết nối BigQuery ({bq_exc}) và cũng không có snapshot cục bộ ({local_exc})."
            ) from local_exc


def compute_kpis(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """Compute top-level market KPIs and frontage-vs-alley premium."""
    if df.empty:
        return {
            "total_listings": 0,
            "median_price_billion": None,
            "median_price_per_m2": None,
            "median_area_m2": None,
            "frontage_median_m2": None,
            "alley_median_m2": None,
            "frontage_premium_pct": None,
            "multi_broker_share_pct": 0.0,
            "avg_broker_count": 0.0,
        }

    frontage_mask = df["lane_type"] == "Mặt tiền"
    alley_mask = df["lane_type"].isin(["Hẻm ô tô", "Hẻm xe máy"])

    frontage_med = float(df.loc[frontage_mask, "price_per_m2"].median()) if frontage_mask.any() else None
    alley_med = float(df.loc[alley_mask, "price_per_m2"].median()) if alley_mask.any() else None

    premium_pct: Optional[float] = None
    if frontage_med is not None and alley_med is not None and alley_med > 0:
        premium_pct = ((frontage_med - alley_med) / alley_med) * 100.0

    multi_share = (
        float((df["duplicate_broker_count"] > 1).mean() * 100.0)
        if "duplicate_broker_count" in df.columns
        else 0.0
    )
    avg_brokers = (
        float(df["duplicate_broker_count"].mean())
        if "duplicate_broker_count" in df.columns
        else 1.0
    )

    return {
        "total_listings": int(len(df)),
        "median_price_billion": float(df["price_billion_vnd"].median()),
        "median_price_per_m2": float(df["price_per_m2"].median()),
        "median_area_m2": float(df["area_m2"].median()),
        "frontage_median_m2": frontage_med,
        "alley_median_m2": alley_med,
        "frontage_premium_pct": premium_pct,
        "multi_broker_share_pct": multi_share,
        "avg_broker_count": avg_brokers,
    }


def summarize_by_dimension(df: pd.DataFrame, group_col: str, min_count: int = 1) -> pd.DataFrame:
    """
    Aggregate listing count, median/IQR unit price, median total price, and multi-broker rate
    across any categorical column (e.g., district_label, lane_type, area_segment, legal_status).
    """
    if df.empty or group_col not in df.columns:
        return pd.DataFrame()

    grouped = (
        df.groupby(group_col, observed=True)
        .agg(
            listing_count=("price_per_m2", "count"),
            median_price_per_m2=("price_per_m2", "median"),
            p25_price_per_m2=("price_per_m2", lambda s: float(s.quantile(0.25))),
            p75_price_per_m2=("price_per_m2", lambda s: float(s.quantile(0.75))),
            median_price_billion=("price_billion_vnd", "median"),
            median_area_m2=("area_m2", "median"),
            multi_broker_pct=("duplicate_broker_count", lambda s: float((s > 1).mean() * 100.0)),
        )
        .reset_index()
    )

    grouped = grouped[grouped["listing_count"] >= min_count].copy()
    if grouped.empty:
        return grouped

    grouped["share_pct"] = (grouped["listing_count"] / len(df)) * 100.0
    return grouped


def compute_lane_width_elasticity(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze relationship between street/alley width (lane_width_m) and unit price (price_per_m2).
    Returns correlation, linear slope (million VND/m² per +1m width), sample count, and bucketed table.
    """
    valid = df.dropna(subset=["lane_width_m", "price_per_m2"]).copy()
    valid = valid[(valid["lane_width_m"] > 0) & (valid["lane_width_m"] <= 40)]

    if len(valid) < 5:
        return {
            "sample_count": int(len(valid)),
            "pearson_r": None,
            "slope_per_meter": None,
            "bucket_summary": pd.DataFrame(),
            "valid_df": valid,
        }

    x = valid["lane_width_m"].to_numpy(dtype=float)
    y = valid["price_per_m2"].to_numpy(dtype=float)

    slope: Optional[float] = None
    pearson_r: Optional[float] = None
    if np.std(x) > 0 and np.std(y) > 0:
        pearson_r = float(np.corrcoef(x, y)[0, 1])
        slope = float(np.polyfit(x, y, 1)[0])

    width_bins = [0, 3, 4, 6, 10, 45]
    width_labels = ["< 3m (Hẻm nhỏ)", "3 - 4m", "4 - 6m (Ô tô tránh)", "6 - 10m", "> 10m (Đường lớn)"]
    valid["width_bucket"] = pd.cut(valid["lane_width_m"], bins=width_bins, labels=width_labels, right=False)
    bucket_summary = summarize_by_dimension(valid, "width_bucket", min_count=1)

    return {
        "sample_count": int(len(valid)),
        "pearson_r": pearson_r,
        "slope_per_meter": slope,
        "bucket_summary": bucket_summary,
        "valid_df": valid,
    }

