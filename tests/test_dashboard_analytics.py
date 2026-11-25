"""
Unit tests for dashboard_analytics.py covering local Parquet loading (including BigQuery
dbdate extension metadata handling), formatting, KPIs, and lane-width elasticity.
"""

from pathlib import Path
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = PROJECT_ROOT / "Dashboard"
for path in (PROJECT_ROOT, DASHBOARD_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from dashboard_analytics import (
    compute_kpis,
    compute_lane_width_elasticity,
    format_district_label,
    load_mart_data,
    load_mart_from_local,
    prepare_listings_df,
    summarize_by_dimension,
)


def _sample_raw_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "listing_id": 1,
                "link": "https://example.com/1",
                "crawled_date": "2026-11-14",
                "district": "1",
                "ward": "Bến Nghé",
                "street": "Lê Lợi",
                "lane_type": "Mặt tiền",
                "legal_status": "Sổ hồng",
                "furniture_status": "Đầy đủ",
                "price_billion_vnd": 30.0,
                "area_m2": 100.0,
                "price_per_m2": 300.0,
                "lane_width_m": 12.0,
                "duplicate_broker_count": 3,
            },
            {
                "listing_id": 2,
                "link": "https://example.com/2",
                "crawled_date": "2026-11-14",
                "district": "Thủ Đức",
                "ward": "Thảo Điền",
                "street": "Xuân Thủy",
                "lane_type": "Hẻm ô tô",
                "legal_status": "Sổ hồng",
                "furniture_status": "Cơ bản",
                "price_billion_vnd": 10.0,
                "area_m2": 50.0,
                "price_per_m2": 200.0,
                "lane_width_m": 5.0,
                "duplicate_broker_count": 1,
            },
            {
                "listing_id": 3,
                "link": "https://example.com/3",
                "crawled_date": "2026-11-13",
                "district": "7",
                "ward": "Tân Phong",
                "street": "Nguyễn Văn Linh",
                "lane_type": "Hẻm xe máy",
                "legal_status": "Không có",
                "furniture_status": "Trống / Nhà thô",
                "price_billion_vnd": 3.0,
                "area_m2": 25.0,
                "price_per_m2": 120.0,
                "lane_width_m": 2.5,
                "duplicate_broker_count": 2,
            },
            {
                "listing_id": 4,
                "link": "https://example.com/4",
                "crawled_date": "2026-11-13",
                "district": None,
                "ward": "Phường 1",
                "street": None,
                "lane_type": "Hẻm ô tô",
                "legal_status": "Sổ đỏ",
                "furniture_status": "Đầy đủ",
                "price_billion_vnd": 7.5,
                "area_m2": 50.0,
                "price_per_m2": 150.0,
                "lane_width_m": 4.5,
                "duplicate_broker_count": 1,
            },
            {
                "listing_id": 5,
                "link": "https://example.com/5",
                "crawled_date": "2026-11-12",
                "district": "Bình Thạnh",
                "ward": "Phường 25",
                "street": "D2",
                "lane_type": "Mặt tiền",
                "legal_status": "Sổ hồng",
                "furniture_status": "Đầy đủ",
                "price_billion_vnd": 26.0,
                "area_m2": 120.0,
                "price_per_m2": 216.7,
                "lane_width_m": 10.0,
                "duplicate_broker_count": 1,
            },
        ]
    )


def test_format_district_label():
    assert format_district_label("1") == "Quận 1"
    assert format_district_label("12") == "Quận 12"
    assert format_district_label("Thủ Đức") == "TP. Thủ Đức"
    assert format_district_label("Bình Thạnh") == "Bình Thạnh"
    assert format_district_label(None) == "Chưa xác định"
    assert format_district_label("") == "Chưa xác định"


def test_prepare_and_kpis():
    df = prepare_listings_df(_sample_raw_df())
    assert list(df["district_label"]) == ["Quận 1", "TP. Thủ Đức", "Quận 7", "Chưa xác định", "Bình Thạnh"]
    assert df["area_segment"].astype(str).tolist() == [
        "> 100 m²",
        "30 - 60 m²",
        "< 30 m²",
        "30 - 60 m²",
        "> 100 m²",
    ]

    kpis = compute_kpis(df)
    assert kpis["total_listings"] == 5
    # Frontage median: median(300.0, 216.7) = 258.35; Alley median: median(200.0, 150.0, 120.0) = 150.0
    assert kpis["frontage_median_m2"] == pytest.approx(258.35, rel=1e-3)
    assert kpis["alley_median_m2"] == pytest.approx(150.0, rel=1e-3)
    assert kpis["frontage_premium_pct"] == pytest.approx(((258.35 - 150.0) / 150.0) * 100.0, rel=1e-3)
    assert kpis["multi_broker_share_pct"] == pytest.approx(40.0)


def test_summarize_by_dimension_and_elasticity():
    df = prepare_listings_df(_sample_raw_df())
    lane_summary = summarize_by_dimension(df, "lane_type")
    assert set(lane_summary["lane_type"]) == {"Mặt tiền", "Hẻm ô tô", "Hẻm xe máy"}
    assert lane_summary["share_pct"].sum() == pytest.approx(100.0)

    elasticity = compute_lane_width_elasticity(df)
    assert elasticity["sample_count"] == 5
    assert elasticity["pearson_r"] > 0.8
    assert elasticity["slope_per_meter"] > 0


def test_load_mart_from_local_handles_dbdate_metadata(tmp_path: Path):
    # Simulate BigQuery Parquet pandas metadata containing unsupported 'dbdate' extension type
    table = pa.Table.from_pandas(_sample_raw_df())
    meta = dict(table.schema.metadata or {})
    meta[b"pandas"] = b'{"index_columns": [], "columns": [{"name": "crawled_date", "pandas_type": "dbdate", "numpy_type": "dbdate"}]}'
    bad_meta_table = table.replace_schema_metadata(meta)

    pq_path = tmp_path / "mart_listings_clean.parquet"
    pq.write_table(bad_meta_table, pq_path)

    loaded_df, source_label = load_mart_from_local(parquet_path=pq_path, csv_path=tmp_path / "missing.csv")
    assert len(loaded_df) == 5
    assert "Local Parquet" in source_label


def test_load_mart_data_auto_falls_back_to_local_when_bq_fails(monkeypatch):
    def _boom(*args, **kwargs):
        raise RuntimeError("Simulated BigQuery network failure")

    monkeypatch.setattr("dashboard_analytics.load_mart_from_bigquery", _boom)

    real_parquet = PROJECT_ROOT / "data_exports" / "mart_listings_clean.parquet"
    real_csv = PROJECT_ROOT / "data_exports" / "mart_listings_clean.csv"
    if not real_parquet.exists() and not real_csv.exists():
        pytest.skip("No local export snapshot present")

    df, source_label, warning = load_mart_data(mode="auto", parquet_path=real_parquet, csv_path=real_csv)
    assert len(df) > 0
    assert "Local" in source_label
    assert warning is not None


def test_load_mart_data_reports_both_errors_when_bq_and_local_fail(monkeypatch, tmp_path: Path):
    def _boom(*args, **kwargs):
        raise RuntimeError("Missing GCP credentials")

    monkeypatch.setattr("dashboard_analytics.load_mart_from_bigquery", _boom)

    with pytest.raises(RuntimeError, match="Missing GCP credentials"):
        load_mart_data(
            mode="auto",
            parquet_path=tmp_path / "missing.parquet",
            csv_path=tmp_path / "missing.csv",
        )

