"""
Unit tests for HCMC Real Estate Incremental & Fault-tolerant Crawler.
Testing framework: pytest + unittest.mock

Test cases verify:
1. Tenacity retry mechanism on HTTP 503 and network failure (up to 4 attempts).
2. Per-listing error isolation (1 listing failing does not crash pipeline).
3. Incremental crawling & early stopping on 5 consecutive duplicate listings.
4. Counter reset when a fresh listing breaks a duplicate streak.
5. Rate limiting delay range and HTTP User-Agent header generation.
6. Session statistics tracking and logging summary output.
"""

import json
from pathlib import Path
import sys
from unittest.mock import MagicMock, call, patch

import pytest

# Ensure module directory is on sys.path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "Module" / "01_data_collection"))

from scrapling_crawler import (
    CrawlStats,
    IncrementalStateManager,
    RetryableHTTPError,
    _prepare_dataframe,
    apply_rate_limit,
    crawl_pipeline,
    extract_property_details,
    fetch_page_with_retry,
    get_hive_partition_path,
    get_random_headers,
)


# =============================================================================
# 1. FAULT TOLERANCE TESTS (Tenacity Retry on 503 & Network Errors)
# =============================================================================
class DummyResponse:
    """Mock response object compatible with Scrapling Fetcher."""

    def __init__(self, status: int = 200, text: str = ""):
        self.status = status
        self.text = text

    def css(self, selector: str):
        mock_sel = MagicMock()
        mock_sel.getall.return_value = []
        mock_sel.get.return_value = None
        return mock_sel


def test_tenacity_retry_on_503_succeeds_eventually():
    """
    Simulate HTTP 503 on first 2 attempts, followed by HTTP 200 on attempt 3.
    Verify tenacity retries up to attempt 3 and returns the successful response.
    """
    mock_fetcher = MagicMock()
    # 503 -> 503 -> 200
    mock_fetcher.get.side_effect = [
        DummyResponse(status=503),
        DummyResponse(status=503),
        DummyResponse(status=200, text="Success"),
    ]

    with patch("time.sleep", return_value=None):  # Fast-forward retry sleep
        resp = fetch_page_with_retry(
            url="https://batdongsan.com.vn/test-503",
            fetcher_override=mock_fetcher,
        )

    assert resp.status == 200
    assert mock_fetcher.get.call_count == 3


def test_tenacity_retry_exceeds_max_4_attempts_raises_error():
    """
    Simulate persistent HTTP 503 (4+ times).
    Verify tenacity attempts exactly 4 times and then raises RetryableHTTPError.
    """
    mock_fetcher = MagicMock()
    mock_fetcher.get.return_value = DummyResponse(status=503)

    with patch("time.sleep", return_value=None):
        with pytest.raises(RetryableHTTPError) as exc_info:
            fetch_page_with_retry(
                url="https://batdongsan.com.vn/persistent-error",
                fetcher_override=mock_fetcher,
            )

    assert "HTTP 503" in str(exc_info.value)
    assert mock_fetcher.get.call_count == 4


def test_tenacity_retry_on_connection_error():
    """
    Simulate network ConnectionError on attempt 1 and 2, followed by 200 on attempt 3.
    """
    mock_fetcher = MagicMock()
    mock_fetcher.get.side_effect = [
        ConnectionError("DNS resolution failed"),
        ConnectionError("Connection reset by peer"),
        DummyResponse(status=200, text="Connected"),
    ]

    with patch("time.sleep", return_value=None):
        resp = fetch_page_with_retry(
            url="https://batdongsan.com.vn/test-conn-error",
            fetcher_override=mock_fetcher,
        )

    assert resp.status == 200
    assert mock_fetcher.get.call_count == 3


# =============================================================================
# 2. PER-LISTING ERROR ISOLATION TESTS
# =============================================================================
def test_per_listing_error_isolation(tmp_path):
    """
    Verify that an unhandled error on one listing does not crash the crawl pipeline.
    Listing 1 succeeds, Listing 2 crashes, Listing 3 succeeds.
    """
    state_file = tmp_path / "state.json"
    out_dir = tmp_path / "output"

    urls = [
        "https://batdongsan.com.vn/pr101",
        "https://batdongsan.com.vn/pr102-fail",
        "https://batdongsan.com.vn/pr103",
    ]

    # Mock catalog page returning these 3 URLs
    mock_catalog_resp = MagicMock()
    mock_catalog_sel = MagicMock()
    mock_catalog_sel.getall.return_value = urls
    mock_catalog_resp.css.return_value = mock_catalog_sel

    def mock_extract(url, fetcher=None):
        if "fail" in url:
            raise RuntimeError("Corrupted HTML payload or parsing crash!")
        return {
            "Link": url,
            "Tiêu đề": f"Nhà đẹp tại {url}",
            "Địa chỉ": "Quận 1, TP.HCM",
            "Mô tả": "Sổ hồng riêng, hẻm xe hơi.",
        }

    with patch("scrapling_crawler.fetch_page_with_retry", return_value=mock_catalog_resp):
        with patch("scrapling_crawler.extract_property_details", side_effect=mock_extract):
            with patch("scrapling_crawler.apply_rate_limit", return_value=0.0):
                df_raw, df_filled, stats = crawl_pipeline(
                    target_samples=10,
                    output_dir=str(out_dir),
                    state_file=str(state_file),
                    early_stopping_limit=5,
                    min_delay=0.0,
                    max_delay=0.0,
                )

    # Pipeline continued despite failure on pr102
    assert stats.new_collected == 2
    assert stats.failed_errors == 1
    assert stats.total_scanned == 3
    assert len(df_raw) == 2
    assert "https://batdongsan.com.vn/pr101" in df_raw["Link"].values
    assert "https://batdongsan.com.vn/pr103" in df_raw["Link"].values


# =============================================================================
# 3. INCREMENTAL CRAWLING & EARLY STOPPING TESTS
# =============================================================================
def test_early_stopping_on_5_consecutive_duplicates(tmp_path):
    """
    Pre-populate state with 5 URLs.
    Provide a feed containing those 5 duplicates followed by new URLs.
    Verify the crawler stops immediately after the 5th duplicate and does NOT scan further.
    """
    state_file = tmp_path / "state.json"
    out_dir = tmp_path / "output"

    existing_urls = [
        "https://batdongsan.com.vn/pr001",
        "https://batdongsan.com.vn/pr002",
        "https://batdongsan.com.vn/pr003",
        "https://batdongsan.com.vn/pr004",
        "https://batdongsan.com.vn/pr005",
    ]

    # Pre-populate state
    state = IncrementalStateManager(state_file=str(state_file))
    for u in existing_urls:
        state.mark_seen(u)
    state.save()

    feed_urls = existing_urls + [
        "https://batdongsan.com.vn/pr006-new",
        "https://batdongsan.com.vn/pr007-new",
    ]

    mock_catalog_resp = MagicMock()
    mock_catalog_sel = MagicMock()
    mock_catalog_sel.getall.return_value = feed_urls
    mock_catalog_resp.css.return_value = mock_catalog_sel

    mock_extract = MagicMock()

    with patch("scrapling_crawler.fetch_page_with_retry", return_value=mock_catalog_resp):
        with patch("scrapling_crawler.extract_property_details", mock_extract):
            with patch("scrapling_crawler.apply_rate_limit", return_value=0.0):
                df_raw, df_filled, stats = crawl_pipeline(
                    target_samples=10,
                    output_dir=str(out_dir),
                    state_file=str(state_file),
                    early_stopping_limit=5,
                    min_delay=0.0,
                    max_delay=0.0,
                )

    # Exactly 5 duplicates were scanned before early stopping triggered
    assert stats.skipped_duplicates == 5
    assert stats.total_scanned == 5
    assert stats.new_collected == 0
    # pr006-new was NEVER extracted because early stopping halted execution
    mock_extract.assert_not_called()


def test_duplicate_counter_resets_on_fresh_listing(tmp_path):
    """
    Feed has 4 duplicates -> 1 new listing -> 2 duplicates.
    Streak of 4 is broken by the new listing, so early stopping (threshold=5) should NOT trigger.
    """
    state_file = tmp_path / "state.json"
    out_dir = tmp_path / "output"

    state = IncrementalStateManager(state_file=str(state_file))
    state.mark_seen("https://batdongsan.com.vn/pr1")
    state.mark_seen("https://batdongsan.com.vn/pr2")
    state.mark_seen("https://batdongsan.com.vn/pr3")
    state.mark_seen("https://batdongsan.com.vn/pr4")
    state.mark_seen("https://batdongsan.com.vn/pr6")
    state.mark_seen("https://batdongsan.com.vn/pr7")
    state.save()

    # 4 duplicates -> 1 fresh -> 2 duplicates
    feed_urls = [
        "https://batdongsan.com.vn/pr1",
        "https://batdongsan.com.vn/pr2",
        "https://batdongsan.com.vn/pr3",
        "https://batdongsan.com.vn/pr4",
        "https://batdongsan.com.vn/pr5-FRESH",
        "https://batdongsan.com.vn/pr6",
        "https://batdongsan.com.vn/pr7",
    ]

    mock_catalog_resp = MagicMock()
    mock_catalog_sel = MagicMock()
    mock_catalog_sel.getall.return_value = feed_urls
    mock_catalog_resp.css.return_value = mock_catalog_sel

    def mock_extract(url, fetcher=None):
        return {
            "Link": url,
            "Tiêu đề": "Căn nhà mới",
            "Địa chỉ": "Bình Thạnh, TP.HCM",
            "Mô tả": "Nhà đẹp chính chủ",
        }

    with patch("scrapling_crawler.fetch_page_with_retry", return_value=mock_catalog_resp):
        with patch("scrapling_crawler.extract_property_details", side_effect=mock_extract):
            with patch("scrapling_crawler.apply_rate_limit", return_value=0.0):
                df_raw, df_filled, stats = crawl_pipeline(
                    target_samples=1,  # Target is 1 new listing
                    output_dir=str(out_dir),
                    state_file=str(state_file),
                    early_stopping_limit=5,
                    min_delay=0.0,
                    max_delay=0.0,
                )

    assert stats.new_collected == 1
    assert stats.skipped_duplicates == 4
    assert stats.total_scanned == 5
    assert len(df_raw) == 1
    assert df_raw.iloc[0]["Link"] == "https://batdongsan.com.vn/pr5-FRESH"


# =============================================================================
# 4. RATE LIMITING & HEADERS TESTS
# =============================================================================
def test_rate_limiting_delay():
    """Verify that apply_rate_limit sleeps within [min_delay, max_delay]."""
    with patch("time.sleep") as mock_sleep:
        delay = apply_rate_limit(min_delay=1.5, max_delay=3.5)
        assert 1.5 <= delay <= 3.5
        mock_sleep.assert_called_once_with(delay)


def test_user_agent_headers_generation():
    """Verify headers contain realistic User-Agent and browser headers."""
    headers = get_random_headers()
    assert "User-Agent" in headers
    assert len(headers["User-Agent"]) > 20
    assert "Accept-Language" in headers
    assert "gzip" in headers["Accept-Encoding"]


# =============================================================================
# 5. LOGGING & SESSION STATISTICS TESTS
# =============================================================================
def test_crawl_stats_summary_calculation(caplog):
    """Verify CrawlStats metrics and log_summary format."""
    stats = CrawlStats()
    stats.total_scanned = 150
    stats.new_collected = 45
    stats.skipped_duplicates = 100
    stats.failed_errors = 5

    with caplog.at_level("INFO"):
        stats.log_summary()

    log_output = caplog.text
    assert "CRAWL SESSION SUMMARY" in log_output
    assert "Total listings scanned  : 150" in log_output
    assert "New listings collected  : 45" in log_output
    assert "Skipped (duplicates)    : 100" in log_output
    assert "Failed (errors)         : 5" in log_output
    assert "Duration" in log_output


# =============================================================================
# 6. APACHE PARQUET & HIVE PARTITIONING TESTS
# =============================================================================
def test_get_hive_partition_path():
    """Verify Hive-style partition directory and timestamped parquet filename format."""
    from datetime import datetime
    dt = datetime(2026, 11, 12, 14, 30, 45)
    partition_dir, filename = get_hive_partition_path("data_1/Raw", dt=dt)

    assert str(partition_dir).replace("\\", "/").endswith("data_1/Raw/year=2026/month=11/day=12")
    assert filename == "listings_20261112_143045.parquet"


def test_prepare_dataframe_schema_and_crawled_at():
    """Verify crawled_at ISO timestamp injection and explicit string type casting."""
    import pandas as pd
    records = [
        {
            "Link": "https://batdongsan.com.vn/pr999",
            "Mã tin": 45066335,
            "Tiêu đề": "Nhà phố Quận 1",
            "Latitude": "10.7769",
            "Longitude": 106.7009,
            "Mô tả": None,
        }
    ]
    df = _prepare_dataframe(records)

    assert "crawled_at" in df.columns
    assert isinstance(df["crawled_at"].iloc[0], str)
    assert df["Latitude"].dtype == "float64"
    assert df["Longitude"].dtype == "float64"
    assert df["Link"].dtype == "string"
    assert df["Mã tin"].dtype == "string"
    assert df["Tiêu đề"].dtype == "string"
    assert df["Mô tả"].dtype == "string"


def test_incremental_state_manager_loads_parquet_and_legacy_csv(tmp_path):
    """Verify IncrementalStateManager loads seen URLs from both Parquet files and legacy CSV."""
    import pandas as pd
    state_file = tmp_path / "state.json"
    data_dir = tmp_path / "data_1" / "Raw"
    data_dir.mkdir(parents=True, exist_ok=True)

    # 1. Write a legacy CSV file
    legacy_csv = data_dir / "data_bds_scrapling_raw.csv"
    df_csv = pd.DataFrame([
        {"Link": "https://batdongsan.com.vn/pr-legacy-01", "Mã tin": "9901"},
        {"Link": "https://batdongsan.com.vn/pr-legacy-02", "Mã tin": "9902"},
    ])
    df_csv.to_csv(legacy_csv, index=False, encoding="utf-8-sig")

    # 2. Write a Hive Parquet partition file
    partition_dir = data_dir / "year=2026" / "month=11" / "day=12"
    partition_dir.mkdir(parents=True, exist_ok=True)
    parquet_file = partition_dir / "listings_20261112_100000.parquet"
    df_parquet = pd.DataFrame([
        {"Link": "https://batdongsan.com.vn/pr-parquet-01", "Mã tin": "8801"},
        {"Link": "https://batdongsan.com.vn/pr-parquet-02", "Mã tin": "8802"},
    ])
    try:
        df_parquet.to_parquet(parquet_file, index=False, engine="pyarrow")
    except Exception:
        # Fallback if pyarrow engine not installed in local environment
        pass

    state = IncrementalStateManager(state_file=str(state_file), data_dir=str(data_dir))
    assert state.is_seen("https://batdongsan.com.vn/pr-legacy-01")
    assert state.is_seen("https://batdongsan.com.vn/pr-legacy-02")
    if parquet_file.exists():
        assert state.is_seen("https://batdongsan.com.vn/pr-parquet-01")


