"""
Module: scrapling_crawler.py
Description: Production-Grade Incremental & Fault-tolerant Real Estate Crawler for batdongsan.com.vn.
Author: Senior Data Engineer
Target: Harvest real estate listings and serialize directly to Apache Parquet with Hive-style partitioning:
        `data_1/Raw/year=YYYY/month=MM/day=DD/listings_YYYYMMDD_HHMMSS.parquet`

Key Capabilities:
    1. Incremental Crawling & Zero Duplication:
       - Multi-tier state tracking via `IncrementalStateManager`.
       - Scans `state.json`, all existing Hive `.parquet` partition files in `data_1/Raw/`,
         and legacy CSV (`data_bds_scrapling_raw.csv`) to guarantee 100% duplicate prevention.
       - Early stopping on consecutive duplicate streak (default: 15, configurable).
    2. Apache Parquet & Hive Partitioning:
       - Organized by execution date: `year=YYYY/month=MM/day=DD/listings_YYYYMMDD_HHMMSS.parquet`.
       - Atomic file write via temporary files (`.tmp` -> `os.replace`).
       - Explicit string casting for text/metadata columns to eliminate Parquet schema collisions.
       - ISO-8601 UTC timestamp `crawled_at` attached to every record.
       - Snappy compression using PyArrow engine.
    3. Fault Tolerance & Resilience:
       - Tenacity exponential backoff with jitter on network failures and HTTP 403, 429, 5xx.
       - Explicit 20s request timeout with backward-compatible fallback.
       - Per-item error isolation (try-except) preventing pipeline crashes.
       - Graceful shutdown on Ctrl+C (KeyboardInterrupt) with data-first atomic flush via `finally:`.
       - Resilient catalog navigation with `max_bad_pages` threshold.
    4. Anti-blocking:
       - Random jitter delay (1.5s - 3.5s) between requests.
       - Realistic User-Agent pool rotation and full browser imitation headers.
    5. Observability:
       - Lazy structured logging (console + crawler.log).
       - Guaranteed final session summary via `finally:` block with `stop_reason`.
"""

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import random
import re
import shutil
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import pandas as pd
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

# -----------------------------------------------------------------------------
# Project Root & Path Resolution (Jupyter Notebook & CLI Agnostic)
# -----------------------------------------------------------------------------
def _find_project_root() -> Path:
    current = Path(__file__).resolve().parent
    for parent in [current] + list(current.parents):
        if (parent / ".env").exists() or (parent / "storage_b2.py").exists() or (parent / "CODEBOOK.md").exists():
            return parent
    return Path(__file__).resolve().parents[1]

PROJECT_ROOT = _find_project_root()

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _resolve_project_path(path: Union[str, Path]) -> Path:
    """Resolve relative path against PROJECT_ROOT to guarantee consistency across execution contexts."""
    p = Path(path)
    if p.is_absolute():
        return p
    return (PROJECT_ROOT / p).resolve()

_SCRAPLING_IMPORT_ERROR = None
Fetcher = None

# Priority 1: High-resilience curl_cffi with Chrome TLS Impersonation (Bypasses Cloudflare & HTTP 403 WAF)
try:
    from curl_cffi import requests as c_requests

    try:
        from scrapling import Selector as _ParserClass
    except ImportError:
        try:
            from scrapling.parser import Selector as _ParserClass
        except ImportError:
            try:
                from scrapling import Adaptor as _ParserClass
            except ImportError:
                from parsel import Selector as _ParserClass

    class _AdaptiveHTTPFetcher:
        """
        Production-grade HTTP Fetcher with Chrome TLS impersonation (curl_cffi).
        Mimics genuine Chrome JA3/JA4 TLS fingerprints, bypassing Cloudflare WAF HTTP 403 blocks.
        """
        @classmethod
        def get(
            cls,
            url: str,
            headers: Optional[Dict[str, str]] = None,
            stealthy_headers: bool = True,
            timeout: float = 20.0,
            **kwargs,
        ) -> Any:
            resp_obj = c_requests.get(
                url,
                headers=headers,
                impersonate="chrome124",
                timeout=timeout,
            )
            page = _ParserClass(resp_obj.text)
            setattr(page, "status", getattr(resp_obj, "status_code", 200))
            setattr(page, "status_code", getattr(resp_obj, "status_code", 200))
            return page

    Fetcher = _AdaptiveHTTPFetcher
except Exception as _err_cffi:
    # Priority 2: Standard Scrapling Fetcher
    try:
        from scrapling.fetchers import Fetcher
    except Exception as _err1:
        # Priority 3: Direct submodule import
        try:
            from scrapling.fetchers.requests import Fetcher
        except Exception as _err2:
            Fetcher = None
            _SCRAPLING_IMPORT_ERROR = _err2 or _err1 or _err_cffi

# -----------------------------------------------------------------------------
# Configuration & Constants
# -----------------------------------------------------------------------------
BASE_URL = "https://batdongsan.com.vn/ban-nha-dat-tp-hcm"
QUERY = "?avrs=1"

# Realistic User-Agent rotation pool
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]

# Defaults shared by crawl_pipeline() and CLI
DEFAULT_EARLY_STOP_THRESHOLD = 15   # VIP listings pinned at top of page 1 must not trigger premature stop
DEFAULT_BATCH_FLUSH = 10            # Checkpoint to disk every N new listings
DEFAULT_MAX_BAD_PAGES = 3           # Consecutive failed/empty catalog pages before halting
REQUEST_TIMEOUT = 20.0              # Seconds, explicit per-request timeout
MAX_RETRY_ATTEMPTS = 4
RETRYABLE_STATUS = (403, 429, 500, 502, 503, 504)

# Legal status mapping
LEGAL_MAP = {
    "sổ hồng": "Sổ hồng",
    "sổ đỏ": "Sổ đỏ",
    "shr": "Sổ hồng",
    "hợp đồng mua bán": "HĐMB",
    "hdmb": "HĐMB",
    "giấy tờ tay": "Giấy tay",
    "giấy tay": "Giấy tay",
    "chờ sổ": "Chờ sổ",
    "đang làm sổ": "Chờ sổ",
}

# Directions regex pattern
DIRECTIONS = (
    r"(đông[\s-]?nam|đông[\s-]?bắc|tây[\s-]?nam|tây[\s-]?bắc"
    r"|đông|tây|nam|bắc)"
)


# -----------------------------------------------------------------------------
# Criterion 4: Logging Setup (Lazy Initialization)
# -----------------------------------------------------------------------------
def setup_crawler_logger(log_file: Optional[str] = "crawler.log") -> logging.Logger:
    """Configure logger with both console and file handlers (idempotent)."""
    lg = logging.getLogger("bds_crawler")
    lg.setLevel(logging.INFO)
    lg.propagate = False

    if not lg.handlers:
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # 1. Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.INFO)
        lg.addHandler(console_handler)

        # 2. File handler
        if log_file:
            try:
                file_handler = logging.FileHandler(log_file, encoding="utf-8")
                file_handler.setFormatter(formatter)
                file_handler.setLevel(logging.INFO)
                lg.addHandler(file_handler)
            except Exception as e:
                lg.warning(f"Could not initialize file logger '{log_file}': {e}")

    return lg


def get_logger(log_file: Optional[str] = "crawler.log") -> logging.Logger:
    """Return the crawler logger, configuring handlers only if none exist yet."""
    lg = logging.getLogger("bds_crawler")
    if not lg.handlers:
        setup_crawler_logger(log_file)
    return lg


logger = logging.getLogger("bds_crawler")


# -----------------------------------------------------------------------------
# Criterion 4: Session Statistics Tracker
# -----------------------------------------------------------------------------
class CrawlStats:
    """Tracks crawl session metrics."""

    def __init__(self):
        self.total_scanned: int = 0
        self.new_collected: int = 0
        self.skipped_duplicates: int = 0
        self.failed_errors: int = 0
        self.start_time: float = time.time()
        self.end_time: Optional[float] = None
        self.stop_reason: str = "running"

    def finish(self) -> None:
        self.end_time = time.time()

    @property
    def duration_seconds(self) -> float:
        end = self.end_time or time.time()
        return round(end - self.start_time, 2)

    def log_summary(self) -> None:
        """Output structured crawl session statistics."""
        self.finish()
        logger.info("=" * 60)
        logger.info("CRAWL SESSION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total listings scanned  : {self.total_scanned}")
        logger.info(f"New listings collected  : {self.new_collected}")
        logger.info(f"Skipped (duplicates)    : {self.skipped_duplicates}")
        logger.info(f"Failed (errors)         : {self.failed_errors}")
        logger.info(f"Duration                : {self.duration_seconds}s")
        logger.info(f"Stop reason             : {self.stop_reason}")
        if self.duration_seconds > 0:
            speed = self.new_collected / self.duration_seconds
            logger.info(f"Speed                   : {speed:.2f} new listings/sec")
        logger.info("=" * 60)


# -----------------------------------------------------------------------------
# Parquet Hive Partitioning & Atomic Write Helpers
# -----------------------------------------------------------------------------
def get_hive_partition_path(
    base_dir: Union[str, Path] = "data_1/Raw",
    dt: Optional[datetime] = None,
) -> Tuple[Path, str]:
    """
    Generate Hive-style partition directory and timestamped parquet filename.
    Format:
        Directory: `<base_dir>/year=YYYY/month=MM/day=DD`
        Filename:  `listings_YYYYMMDD_HHMMSS.parquet`
    """
    if dt is None:
        dt = datetime.now()

    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    ts = dt.strftime("%Y%m%d_%H%M%S")

    partition_dir = Path(base_dir) / f"year={year}" / f"month={month}" / f"day={day}"
    filename = f"listings_{ts}.parquet"
    return partition_dir, filename


def _backup_corrupt(path: Path) -> None:
    """Keep a copy of an unreadable file so it is never silently overwritten."""
    try:
        bak = path.with_name(f"{path.name}.corrupt-{time.strftime('%Y%m%d-%H%M%S')}")
        shutil.copy2(path, bak)
        logger.warning(f"Backed up unreadable file '{path}' to '{bak}'.")
    except Exception as e:
        logger.error(f"Could not back up unreadable file '{path}': {e}")


def _prepare_dataframe(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Prepare records DataFrame with standardized schema, crawled_at timestamp,
    and explicit string dtypes to prevent Parquet schema collision across partitions.
    """
    df = pd.DataFrame(records)
    if df.empty:
        return df

    # Requirement 4: Ensure crawled_at ISO-8601 timestamp exists for every record
    utc_now_iso = datetime.now(timezone.utc).isoformat()
    if "crawled_at" not in df.columns:
        df["crawled_at"] = utc_now_iso
    else:
        df["crawled_at"] = df["crawled_at"].fillna(utc_now_iso).astype("string")

    # Requirement 4: Explicit schema type enforcement
    # GPS numeric columns stay float64; all other columns explicitly cast to string
    numeric_cols = {"Latitude", "Longitude"}
    for col in df.columns:
        if col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        else:
            df[col] = df[col].astype("string")

    return df


def _atomic_write_parquet(df: pd.DataFrame, path: Path) -> Optional[pd.DataFrame]:
    """
    Write DataFrame to Parquet atomically:
    1. Write to temporary file with '.tmp' suffix.
    2. Use PyArrow engine with Snappy compression.
    3. Atomically replace the target file via os.replace.
    """
    try:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        df.to_parquet(tmp, index=False, engine="pyarrow", compression="snappy")
        os.replace(tmp, path)
        return df
    except Exception as e:
        logger.error(f"Failed to write Parquet file '{path}': {e}", exc_info=True)
        return None


def _write_parquet(records: List[Dict[str, Any]], parquet_file: Path) -> Optional[pd.DataFrame]:
    """Atomically serialize records to Parquet. Returns prepared DataFrame on success, None on error."""
    if not records:
        return pd.DataFrame()
    try:
        df = _prepare_dataframe(records)
        return _atomic_write_parquet(df, parquet_file)
    except Exception as e:
        logger.error(f"Failed to prepare/serialize Parquet for '{parquet_file}': {e}", exc_info=True)
        return None


# -----------------------------------------------------------------------------
# Criterion 1: Incremental State Management
# -----------------------------------------------------------------------------
class IncrementalStateManager:
    """
    Manages local state of scraped listing IDs/URLs.
    Supports O(1) duplicate checks and atomic persistence.

    Loads existing state from:
      1. state.json (atomic local state tracker)
      2. Any existing Hive-partitioned .parquet files in data_dir (e.g. data_1/Raw/)
      3. Legacy CSV file (data_bds_scrapling_raw.csv) if present
    """

    def __init__(
        self,
        state_file: Union[str, Path] = "state.json",
        data_dir: Optional[Union[str, Path]] = "data_1/Raw",
    ):
        self.state_file = _resolve_project_path(state_file)
        self.data_dir = _resolve_project_path(data_dir) if data_dir else None
        self.seen_urls: Set[str] = set()
        self.seen_ids: Set[str] = set()
        self.load()

    def extract_id(self, url: str) -> str:
        """Extract unique property ID from batdongsan URL (e.g., 'pr45066335')."""
        match = re.search(r"pr(\d+)", url)
        if match:
            return match.group(1)
        return url.strip().lower()

    def load(self) -> None:
        """Load state from state.json, existing Parquet files, and legacy CSV."""
        # 1. Load from state.json
        if self.state_file.exists():
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.seen_urls = set(data.get("seen_urls", []))
                    self.seen_ids = set(data.get("seen_ids", []))
                logger.info(
                    f"Loaded state from '{self.state_file}': {len(self.seen_urls)} URLs, {len(self.seen_ids)} IDs."
                )
            except Exception as e:
                logger.warning(f"Failed to read state file '{self.state_file}': {e}. Starting fresh.")
                _backup_corrupt(self.state_file)
                self.seen_urls = set()
                self.seen_ids = set()
        else:
            logger.info(f"No existing state file at '{self.state_file}'. Creating new state tracker.")

        # 2. Scan and load from existing Parquet files in data_dir (Requirement 5)
        if self.data_dir and self.data_dir.exists():
            parquet_files = [
                p for p in self.data_dir.rglob("*.parquet")
                if not (p.name.endswith(".tmp") or ".tmp." in p.name)
            ]
            parquet_links = 0
            for pfile in parquet_files:
                try:
                    df_p = pd.read_parquet(pfile, columns=["Link"])
                except Exception:
                    try:
                        df_p = pd.read_parquet(pfile)
                    except Exception as err:
                        logger.warning(f"Could not read parquet file '{pfile}' for state: {err}")
                        continue

                if "Link" in df_p.columns:
                    for link in df_p["Link"].dropna():
                        self.mark_seen(str(link))
                        parquet_links += 1
                if "Mã tin" in df_p.columns:
                    for pid in df_p["Mã tin"].dropna():
                        self.seen_ids.add(str(pid).strip())

            if parquet_files:
                logger.info(
                    f"Loaded {parquet_links} listings from {len(parquet_files)} Parquet partition files in '{self.data_dir}'."
                )

            # 3. Read legacy CSV file if it exists in data_dir (Requirement 5)
            legacy_csv = self.data_dir / "data_bds_scrapling_raw.csv"
            if legacy_csv.exists():
                try:
                    df_csv = pd.read_csv(
                        legacy_csv,
                        usecols=lambda c: c in ["Link", "Mã tin"],
                        encoding="utf-8-sig",
                    )
                    csv_links = 0
                    if "Link" in df_csv.columns:
                        for link in df_csv["Link"].dropna():
                            self.mark_seen(str(link))
                            csv_links += 1
                    if "Mã tin" in df_csv.columns:
                        for pid in df_csv["Mã tin"].dropna():
                            self.seen_ids.add(str(pid).strip())
                    logger.info(
                        f"Loaded {csv_links} legacy listings from '{legacy_csv}'."
                    )
                except Exception as err:
                    logger.warning(f"Could not read legacy CSV '{legacy_csv}': {err}")

    def is_seen(self, url: str) -> bool:
        """Check if URL or its property ID has already been crawled."""
        norm_url = url.strip()
        prop_id = self.extract_id(norm_url)
        return norm_url in self.seen_urls or prop_id in self.seen_ids

    def mark_seen(self, url: str) -> None:
        """Mark a listing URL and ID as crawled."""
        norm_url = url.strip()
        prop_id = self.extract_id(norm_url)
        self.seen_urls.add(norm_url)
        self.seen_ids.add(prop_id)

    def save(self) -> bool:
        """Persist state atomically to disk."""
        data = {
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "total_seen_urls": len(self.seen_urls),
            "total_seen_ids": len(self.seen_ids),
            "seen_urls": sorted(self.seen_urls),
            "seen_ids": sorted(self.seen_ids),
        }
        temp_file = self.state_file.with_suffix(".tmp")
        try:
            self.state_file.parent.mkdir(parents=True, exist_ok=True)
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, self.state_file)
            logger.debug(f"Saved state to '{self.state_file}' ({len(self.seen_urls)} URLs).")
            return True
        except Exception as e:
            logger.error(f"Failed to save state to '{self.state_file}': {e}")
            return False

    def sync_from_b2(
        self,
        b2_manager: Optional[Any] = None,
        s3_key: str = "state/state.json",
    ) -> bool:
        """
        Download remote state checkpoint from Backblaze B2 and merge into current state.
        Ensures idempotency and deduplication continuity across stateless/ephemeral environments
        such as GitHub Actions runners.
        """
        try:
            if b2_manager is None:
                from storage_b2 import get_b2_manager
                b2_manager = get_b2_manager()

            if not b2_manager.object_exists(s3_key):
                logger.info(
                    f"No remote state object found at b2://{b2_manager.bucket_name}/{s3_key}. Using local state."
                )
                return False

            logger.info(f"Downloading remote state checkpoint from b2://{b2_manager.bucket_name}/{s3_key}...")
            temp_download = self.state_file.with_name(f"b2_{self.state_file.name}.tmp")
            b2_manager.download_file(s3_key=s3_key, local_path=temp_download)

            if temp_download.exists():
                with open(temp_download, "r", encoding="utf-8") as f:
                    data = json.load(f)
                b2_urls = set(data.get("seen_urls", []))
                b2_ids = set(data.get("seen_ids", []))

                prev_urls_cnt = len(self.seen_urls)
                self.seen_urls.update(b2_urls)
                self.seen_ids.update(b2_ids)

                logger.info(
                    f"Successfully synced state from B2: {len(b2_urls)} URLs loaded. "
                    f"Total seen URLs now: {len(self.seen_urls)} (+{len(self.seen_urls) - prev_urls_cnt} new)."
                )
                try:
                    temp_download.unlink(missing_ok=True)
                except Exception:
                    pass
                self.save()
                return True
            return False
        except Exception as e:
            logger.warning(f"Could not sync state checkpoint from Backblaze B2: {e}")
            return False

    def upload_to_b2(
        self,
        b2_manager: Optional[Any] = None,
        s3_key: str = "state/state.json",
    ) -> bool:
        """
        Upload current state.json to Backblaze B2 so subsequent runs or CI/CD runners
        have the latest checkpoint.
        """
        try:
            self.save()
            if not self.state_file.exists():
                logger.warning(f"Cannot upload state to B2: '{self.state_file}' does not exist.")
                return False

            if b2_manager is None:
                from storage_b2 import get_b2_manager
                b2_manager = get_b2_manager()

            logger.info(
                f"Uploading latest state ({len(self.seen_urls)} URLs, {len(self.seen_ids)} IDs) "
                f"-> b2://{b2_manager.bucket_name}/{s3_key}..."
            )
            b2_manager.upload_file(local_path=self.state_file, s3_key=s3_key)
            logger.info(f"State checkpoint successfully uploaded to b2://{b2_manager.bucket_name}/{s3_key}.")
            return True
        except Exception as e:
            logger.error(f"Failed to upload state checkpoint to Backblaze B2: {e}", exc_info=True)
            return False


# -----------------------------------------------------------------------------
# Criterion 2: Fault-Tolerant Network Fetcher (tenacity Retry)
# -----------------------------------------------------------------------------
class RetryableHTTPError(Exception):
    """Raised for transient HTTP errors (403, 429, 5xx) or network failures that should be retried."""
    pass


def get_random_headers(custom_user_agent: Optional[str] = None) -> Dict[str, str]:
    """Criterion 3: Build full HTTP headers with randomized User-Agent."""
    ua = custom_user_agent or random.choice(USER_AGENTS)
    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/",
        "Cache-Control": "max-age=0",
        "DNT": "1",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }


def _log_retry_attempt(retry_state) -> None:
    """Log retry details when tenacity triggers a backoff wait."""
    exception = retry_state.outcome.exception() if retry_state.outcome else "Unknown error"
    url = retry_state.args[0] if (retry_state.args and len(retry_state.args) > 0) else retry_state.kwargs.get("url", "unknown_url")
    sleep_time = getattr(retry_state.next_action, "sleep", 0.0)
    logger.warning(
        f"Retry {retry_state.attempt_number}/{MAX_RETRY_ATTEMPTS} for URL: '{url}' "
        f"after failure: {exception}. Backoff wait: {sleep_time:.2f}s"
    )


@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1.0, min=2.0, max=20.0) + wait_random(0, 2.0),
    retry=retry_if_exception_type(
        (RetryableHTTPError, ConnectionError, TimeoutError, OSError)
    ),
    reraise=True,
    before_sleep=_log_retry_attempt,
)
def fetch_page_with_retry(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    fetcher_override: Any = None,
) -> Any:
    """
    Fetch a web page with max 4 attempts, exponential backoff + random jitter.
    Retries on network errors and HTTP status 403/429/5xx with explicit 20s timeout.
    """
    fetcher = fetcher_override or Fetcher
    if fetcher is None:
        raise RuntimeError("Scrapling is not installed (pip install scrapling) and no fetcher was provided.")
    req_headers = headers or get_random_headers()

    try:
        try:
            response = fetcher.get(url, headers=req_headers, stealthy_headers=True, timeout=REQUEST_TIMEOUT)
        except TypeError as te:
            if "timeout" not in str(te):
                raise
            # Older fetcher versions without timeout argument
            response = fetcher.get(url, headers=req_headers, stealthy_headers=True)
    except Exception as e:
        logger.warning(f"Network error while requesting '{url}': {e}")
        raise RetryableHTTPError(f"Network failure for {url}: {e}") from e

    status = getattr(response, "status", None) or getattr(response, "status_code", 200)

    if status in RETRYABLE_STATUS:
        msg = f"HTTP {status} transient error received from '{url}'"
        logger.warning(msg)
        raise RetryableHTTPError(msg)

    if status != 200:
        logger.warning(f"HTTP {status} received from '{url}' (non-retryable client error).")

    return response


# -----------------------------------------------------------------------------
# Criterion 3: Rate Limiting
# -----------------------------------------------------------------------------
def apply_rate_limit(min_delay: float = 1.5, max_delay: float = 3.5) -> float:
    """Enforce random delay between requests to respect rate limits."""
    delay = random.uniform(min_delay, max_delay)
    logger.debug(f"Rate limiting: sleeping for {delay:.2f}s...")
    time.sleep(delay)
    return delay


# -----------------------------------------------------------------------------
# Heuristic Feature Extraction from Description
# -----------------------------------------------------------------------------
def extract_from_desc(desc: Any) -> Dict[str, Any]:
    """Extract auxiliary properties from free-text real estate description."""
    if not isinstance(desc, str) or not desc.strip():
        return {}
    txt = desc.lower()
    res: Dict[str, Any] = {}

    # 1. Land Area
    m = re.search(r"\((\d+[,.]?\d*)\s*m[²2]", txt)
    if not m:
        m = re.search(
            r"di[eệ]n t[ií]ch(?:\s+(?:đ[aấ]t|khu[ôo]n đ[aấ]t|thực tế))?"
            r"[:\s]*(\d+[,.]?\d*)\s*m",
            txt,
        )
    res["DT đất (desc)"] = m.group(1).replace(",", ".") if m else np.nan

    # 2. Dimensions (Width x Length)
    m_dim = re.search(
        r"(?:ngang[:\s]*)?\(?([,\d\.]+)\s*[x×]\s*([,\d\.]+)\s*m?\)?", txt
    )
    res["Ngang (desc)"] = (
        m_dim.group(1).replace(",", ".") if m_dim else np.nan
    )
    res["Dài (desc)"] = (
        m_dim.group(2).replace(",", ".") if m_dim else np.nan
    )

    # 3. Floors
    m_floor = re.search(r"(\d+[,.]?\d*)\s*t[aầ]ng", txt)
    if m_floor:
        res["Số tầng (desc)"] = m_floor.group(1)
    else:
        m_tret = re.search(r"(\d+)\s*tr[eệ]t", txt)
        m_lau = re.search(r"(\d+)\s*l[aầ]u", txt)
        if m_tret and m_lau:
            res["Số tầng (desc)"] = str(
                int(m_tret.group(1)) + int(m_lau.group(1))
            )
        elif m_tret:
            res["Số tầng (desc)"] = m_tret.group(1)
        elif m_lau:
            res["Số tầng (desc)"] = str(int(m_lau.group(1)) + 1)
        else:
            res["Số tầng (desc)"] = np.nan

    # 4. Bedrooms & Bathrooms
    m_pn = re.search(r"(\d+)\s*(?:phòng[\s-]?ngủ|pn\b|bedroom)", txt)
    res["Số PN (desc)"] = m_pn.group(1) if m_pn else np.nan

    m_wc = re.search(r"(\d+)\s*(?:toilet|wc\b|nhà vệ sinh|phòng tắm\b)", txt)
    res["Số WC (desc)"] = m_wc.group(1) if m_wc else np.nan

    # 5. Orientation
    m_dir = re.search(
        r"hướng(?:\s+(?:cửa chính|nhà|chính))?[:\s]*" + DIRECTIONS, txt
    )
    res["Hướng nhà (desc)"] = m_dir.group(1).title() if m_dir else np.nan

    # 6. Legal status
    res["Pháp lý (desc)"] = np.nan
    for k, v in LEGAL_MAP.items():
        if k in txt:
            res["Pháp lý (desc)"] = v
            break

    # 7. Furniture
    if re.search(
        r"full n[oộ]i th[aấ]t|đầy đủ n[oộ]i th[aấ]t|n[oộ]i th[aấ]t cao c[aấ]p",
        txt,
    ):
        res["Nội thất (desc)"] = "Đầy đủ"
    elif re.search(r"n[oộ]i th[aấ]t c[oơ] b[aả]n|c[oơ] b[aả]n", txt):
        res["Nội thất (desc)"] = "Cơ bản"
    elif re.search(r"không n[oộ]i th[aấ]t|bàn giao thô|nhà thô", txt):
        res["Nội thất (desc)"] = "Không"
    else:
        res["Nội thất (desc)"] = np.nan

    return res


def fill_specs_with_desc(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing specs from parsed description features."""
    if "Mô tả" not in df.columns or df.empty:
        return df
    df_res = df.copy()
    desc_extra = pd.DataFrame(df_res["Mô tả"].apply(extract_from_desc).tolist())

    mapping = {
        "Diện tích": "DT đất (desc)",
        "Số tầng": "Số tầng (desc)",
        "Số phòng ngủ": "Số PN (desc)",
        "Số phòng tắm, vệ sinh": "Số WC (desc)",
        "Hướng nhà": "Hướng nhà (desc)",
        "Pháp lý": "Pháp lý (desc)",
        "Nội thất": "Nội thất (desc)",
        "Mặt tiền": "Ngang (desc)",
    }

    for target_col, desc_col in mapping.items():
        if target_col in df_res.columns and desc_col in desc_extra.columns:
            df_res[target_col] = df_res[target_col].fillna(desc_extra[desc_col])

    return df_res


# -----------------------------------------------------------------------------
# Extraction Functions (Catalog URLs & Property Details)
# -----------------------------------------------------------------------------
def get_listing_urls(page: int = 1, fetcher: Any = None) -> List[str]:
    """Fetch catalog listing URLs for a given page with tenacity retry."""
    url = f"{BASE_URL}{f'/p{page}' if page > 1 else ''}{QUERY}"
    logger.info(f"Scanning catalog page {page}: {url}")

    resp = fetch_page_with_retry(url, fetcher_override=fetcher)
    if resp is None:
        return []

    link_sel = ".re__srp-list .js__product-link-for-product-id::attr(href)"
    try:
        rel_links = resp.css(link_sel).getall()
    except Exception as e:
        logger.warning(f"Could not parse CSS selector on page {page}: {e}")
        return []

    full_links = []
    for link in rel_links:
        if not link:
            continue
        full_url = f"https://batdongsan.com.vn{link}" if link.startswith("/") else link
        if "?" not in full_url:
            full_url += "?vrs=1"
        elif "vrs=1" not in full_url:
            full_url += "&vrs=1"
        full_links.append(full_url)

    logger.info(f"Page {page} discovered {len(full_links)} listings.")
    return full_links


def extract_property_details(url: str, fetcher: Any = None) -> Dict[str, Any]:
    """Extract property specifications from detail page with fault tolerance."""
    utc_timestamp = datetime.now(timezone.utc).isoformat()
    info: Dict[str, Any] = {
        "Link": url,
        "Mã tin": np.nan,
        "Tiêu đề": np.nan,
        "Địa chỉ": np.nan,
        "Khoảng giá": np.nan,
        "Diện tích": np.nan,
        "Mặt tiền": np.nan,
        "Đường vào": np.nan,
        "Số phòng ngủ": np.nan,
        "Số phòng tắm, vệ sinh": np.nan,
        "Số tầng": np.nan,
        "Hướng nhà": np.nan,
        "Hướng ban công": np.nan,
        "Pháp lý": np.nan,
        "Nội thất": np.nan,
        "Ngày đăng": np.nan,
        "Ngày hết hạn": np.nan,
        "Loại tin": np.nan,
        "Latitude": np.nan,
        "Longitude": np.nan,
        "Mô tả": np.nan,
        "crawled_at": utc_timestamp,  # Requirement 4: ISO-8601 UTC timestamp
    }

    page = fetch_page_with_retry(url, fetcher_override=fetcher)
    if page is None:
        return info

    # 1. Title
    title = page.css("h1::text, .re__pr-title::text").get()
    if title:
        info["Tiêu đề"] = title.strip()

    # 2. Address
    addr = page.css(".re__address-line-1::text, .js__pr-address::text").get()
    if addr:
        info["Địa chỉ"] = addr.strip()

    # 3. GPS Coordinates from iframe
    iframe_src = page.css("iframe.lazyload::attr(data-src)").get()
    if iframe_src and "q=" in iframe_src:
        try:
            coords = iframe_src.split("q=")[1].split("&")[0].split(",")
            info["Latitude"] = float(coords[0].strip())
            info["Longitude"] = float(coords[1].strip())
        except Exception:
            pass

    # 4. Specifications (Specs)
    for item in page.css(".re__pr-specs-content-item"):
        t = item.css(".re__pr-specs-content-item-title::text").get()
        v = item.css(".re__pr-specs-content-item-value::text").get()
        if t and v:
            t_key = t.strip()
            if t_key in info:
                info[t_key] = v.strip()

    # 5. Metadata (Dates, Type, ID)
    for si in page.css(".re__pr-short-info-item"):
        t = si.css(".title::text").get()
        v = si.css(".value::text").get()
        if t and v:
            t_clean = " ".join(t.split())
            if t_clean == "Ngày đăng":
                info["Ngày đăng"] = v.strip()
            elif t_clean == "Ngày hết hạn":
                info["Ngày hết hạn"] = v.strip()
            elif t_clean == "Loại tin":
                info["Loại tin"] = v.strip()
            elif t_clean == "Mã tin":
                info["Mã tin"] = v.strip()

    # 6. Description
    desc_sel = ".re__section-body.re__detail-content::text"
    desc_elements = page.css(desc_sel).getall()
    if desc_elements:
        desc_text = " ".join(desc_elements).strip()
        info["Mô tả"] = desc_text if desc_text else np.nan

    return info


# -----------------------------------------------------------------------------
# Main Crawling Pipeline (Hive Parquet Storage, Incremental, Fault-tolerant)
# -----------------------------------------------------------------------------
def crawl_pipeline(
    target_samples: int = 50,
    output_dir: Union[str, Path] = "data_1/Raw",
    state_file: Union[str, Path] = "state.json",
    early_stop_threshold: int = DEFAULT_EARLY_STOP_THRESHOLD,
    start_page: int = 1,
    min_delay: float = 1.5,
    max_delay: float = 3.5,
    batch_flush: int = DEFAULT_BATCH_FLUSH,
    fetcher: Any = None,
    max_bad_pages: int = DEFAULT_MAX_BAD_PAGES,
    log_file: Optional[Union[str, Path]] = "crawler.log",
    early_stopping_limit: Optional[int] = None,
    session_dt: Optional[datetime] = None,
    upload_b2: bool = False,
    b2_state_key: str = "state/state.json",
) -> Tuple[pd.DataFrame, pd.DataFrame, CrawlStats]:
    """
    Execute Incremental & Fault-tolerant Crawl Pipeline with Hive-partitioned Apache Parquet output.

    Parameters:
        target_samples: Target number of new listings to collect.
        output_dir: Base directory where Hive partitions are created (default: data_1/Raw).
        state_file: File path to JSON state tracker.
        early_stop_threshold: Consecutive duplicate listings before halting (default: 15).
        min_delay / max_delay: Random delay range between requests.
        batch_flush: Checkpoint to disk every N new listings.
        fetcher: HTTP fetcher instance or mock (for testing).
        max_bad_pages: Stop after this many consecutive failed/empty catalog pages.
        log_file: Log file path (None = console only).
        early_stopping_limit: Deprecated alias of early_stop_threshold.
        session_dt: Datetime used for Hive partition calculation (default: current local time).
        upload_b2: If True, automatically upload the written Parquet file to Backblaze B2 Data Lake.

    Returns:
        (df_raw, df_filled, stats)
    """
    resolved_log = _resolve_project_path(log_file) if log_file else None
    get_logger(str(resolved_log) if resolved_log else None)
    if early_stopping_limit is not None:
        early_stop_threshold = early_stopping_limit
    batch_flush = max(1, int(batch_flush))
    early_stop_threshold = max(0, int(early_stop_threshold))
    max_bad_pages = max(1, int(max_bad_pages))
    start_page = max(1, int(start_page))

    if fetcher is None and Fetcher is None:
        raise ImportError(
            f"Scrapling Fetcher is not available ({_SCRAPLING_IMPORT_ERROR}). "
            "Please run: pip install patchright (or: pip install \"scrapling[fetchers]\")"
        )

    stats = CrawlStats()
    out_path = _resolve_project_path(output_dir)
    resolved_state = _resolve_project_path(state_file)

    # Initialize state manager: reads state.json + existing Parquet partitions + legacy CSV
    state = IncrementalStateManager(state_file=resolved_state, data_dir=out_path)

    # Pre-crawl state synchronization from Backblaze B2 (essential for stateless CI/CD runners)
    b2_mgr = None
    if upload_b2:
        try:
            from storage_b2 import get_b2_manager
            b2_mgr = get_b2_manager()
            state.sync_from_b2(b2_manager=b2_mgr, s3_key=b2_state_key)
        except Exception as b2_init_err:
            logger.warning(f"Could not initialize B2 manager for pre-crawl state sync: {b2_init_err}")

    # Requirement 3: Build Hive-style partition path:
    # `data_1/Raw/year=YYYY/month=MM/day=DD/listings_YYYYMMDD_HHMMSS.parquet`
    run_dt = session_dt or datetime.now()
    partition_dir, parquet_filename = get_hive_partition_path(base_dir=out_path, dt=run_dt)
    parquet_file = partition_dir / parquet_filename

    records: List[Dict[str, Any]] = []
    consecutive_duplicates = 0
    consecutive_bad_pages = 0
    last_flush_count = 0
    stop_reason: Optional[str] = None
    page = start_page
    max_pages = max(page + 100, page + (target_samples // 20) + 20)
    df_raw, df_filled = pd.DataFrame(), pd.DataFrame()

    logger.info(
        f"Starting crawl: Target={target_samples} new listings | "
        f"Early stopping threshold={early_stop_threshold} consecutive duplicates | "
        f"Output Parquet={parquet_file} | Delay range=[{min_delay}s, {max_delay}s]"
    )

    try:
        while stats.new_collected < target_samples and page <= max_pages and stop_reason is None:
            try:
                apply_rate_limit(min_delay, max_delay)
                page_urls = get_listing_urls(page=page, fetcher=fetcher)
            except Exception as e:
                stats.failed_errors += 1
                consecutive_bad_pages += 1
                logger.error(f"Failed to fetch catalog page {page}: {e}. Skipping to next page.")
                if consecutive_bad_pages >= max_bad_pages:
                    stop_reason = f"{consecutive_bad_pages} consecutive catalog page failures"
                    logger.warning(f"Halting crawler: {stop_reason} (network down or blocked).")
                page += 1
                continue

            if not page_urls:
                consecutive_bad_pages += 1
                logger.info(f"Catalog page {page} returned 0 URLs. End of listings or anti-bot challenge.")
                if consecutive_bad_pages >= max_bad_pages:
                    stop_reason = f"{consecutive_bad_pages} consecutive empty catalog pages"
                    break
                page += 1
                continue
            consecutive_bad_pages = 0

            for url in page_urls:
                stats.total_scanned += 1

                # Criterion 1: Incremental check & Early Stopping
                if state.is_seen(url):
                    consecutive_duplicates += 1
                    stats.skipped_duplicates += 1
                    threshold_str = str(early_stop_threshold) if early_stop_threshold > 0 else "disabled"
                    logger.info(
                        f"[DUPLICATE] Listing '{url}' already crawled. "
                        f"Consecutive duplicate count: {consecutive_duplicates}/{threshold_str}"
                    )

                    if early_stop_threshold > 0 and consecutive_duplicates >= early_stop_threshold:
                        logger.warning(
                            f"🛑 EARLY STOPPING TRIGGERED: Reached {consecutive_duplicates} consecutive duplicates. "
                            "No new listings found in recent feed. Halting crawler."
                        )
                        stop_reason = "early stopping (consecutive duplicates)"
                        break
                    continue

                # Found a fresh listing -> Reset duplicate streak
                consecutive_duplicates = 0

                # Criterion 2: Per-listing error isolation
                try:
                    apply_rate_limit(min_delay, max_delay)
                    details = extract_property_details(url=url, fetcher=fetcher)

                    if details and (pd.notna(details.get("Tiêu đề")) or pd.notna(details.get("Địa chỉ")) or pd.notna(details.get("Mô tả"))):
                        records.append(details)
                        state.mark_seen(url)
                        stats.new_collected += 1
                        logger.info(
                            f"[{stats.new_collected}/{target_samples}] Scraped: "
                            f"{str(details.get('Tiêu đề', 'No Title'))[:45]}..."
                        )
                    else:
                        logger.warning(f"Empty details extracted for '{url}'.")
                        stats.failed_errors += 1

                except Exception as e:
                    logger.error(f"Error extracting listing '{url}': {e}", exc_info=True)
                    stats.failed_errors += 1

                # Periodic Checkpoint: Parquet file FIRST (atomic), state AFTER
                if stats.new_collected - last_flush_count >= batch_flush:
                    if _write_parquet(records, parquet_file) is not None and state.save():
                        last_flush_count = stats.new_collected
                        logger.info(
                            f"💾 Checkpoint saved: {len(records)} listings -> '{parquet_file}'."
                        )

                if stats.new_collected >= target_samples:
                    logger.info(f"Reached target sample count of {target_samples}. Stopping crawl.")
                    break

            page += 1

        stop_reason = stop_reason or (
            "target reached" if stats.new_collected >= target_samples else "max pages reached"
        )

    except KeyboardInterrupt:
        stop_reason = "interrupted by user (KeyboardInterrupt)"
        logger.warning("Interrupted by user. Saving collected data before exit...")
    except Exception as e:
        stop_reason = f"unexpected error: {type(e).__name__}: {e}"
        logger.error(f"Unexpected error, saving collected data before exit: {e}", exc_info=True)
    finally:
        stats.stop_reason = stop_reason or "aborted"
        try:
            if records:
                df_written = _write_parquet(records, parquet_file)
                if df_written is not None:
                    df_raw = df_written
                    logger.info(
                        f"✅ Successfully persisted {len(df_raw)} listings to Parquet: '{parquet_file}'."
                    )
                    try:
                        df_filled = fill_specs_with_desc(df_raw)
                    except Exception as e:
                        logger.error(f"Failed to fill specs from description: {e}", exc_info=True)
                    state.save()

                    # Criterion: Automatic Backblaze B2 Upload if requested
                    if upload_b2:
                        try:
                            if b2_mgr is None:
                                from storage_b2 import get_b2_manager
                                b2_mgr = get_b2_manager()
                            uploaded_key = b2_mgr.upload_partitioned_parquet(
                                local_path=parquet_file,
                                prefix="raw",
                                overwrite=False,
                            )
                            if uploaded_key:
                                logger.info(
                                    f"🚀 Backblaze B2 Data Lake: Parquet ready at b2://{b2_mgr.bucket_name}/{uploaded_key}"
                                )
                        except Exception as b2_err:
                            logger.error(f"Failed to auto-upload to Backblaze B2: {b2_err}", exc_info=True)
                else:
                    logger.error(
                        f"Failed to write Parquet file '{parquet_file}'. State not updated to avoid data loss."
                    )
            else:
                logger.info("No new listings collected in this session. Parquet file not created.")
                state.save()

            # Always sync updated state checkpoint back to Backblaze B2
            if upload_b2:
                try:
                    if b2_mgr is None:
                        from storage_b2 import get_b2_manager
                        b2_mgr = get_b2_manager()
                    state.upload_to_b2(b2_manager=b2_mgr, s3_key=b2_state_key)
                except Exception as b2_state_err:
                    logger.error(f"Failed to upload state checkpoint to Backblaze B2: {b2_state_err}", exc_info=True)
        except BaseException as e:
            logger.error(f"Final persistence failed: {e}", exc_info=True)
        try:
            stats.log_summary()
        except Exception:
            pass

    return df_raw, df_filled, stats


def build_arg_parser():
    import argparse

    parser = argparse.ArgumentParser(description="HCMC Real Estate Incremental & Fault-tolerant Parquet Crawler")
    parser.add_argument("--samples", type=int, default=20, help="Target new listings to crawl (default: 20)")
    parser.add_argument("--start-page", type=int, default=1, help="Catalog page to start crawling from (default: 1)")
    parser.add_argument("--output-dir", type=str, default="data_1/Raw",
                        help="Base output directory for Hive Parquet files (default: data_1/Raw)")
    parser.add_argument("--state-file", type=str, default="state.json", help="Path to state tracking JSON file")
    parser.add_argument("--early-stop", type=int, default=DEFAULT_EARLY_STOP_THRESHOLD,
                        help="Consecutive duplicates limit for early stopping (default: 15, set 0 to disable for deep backfill)")
    parser.add_argument("--min-delay", type=float, default=1.5, help="Min delay between requests (seconds)")
    parser.add_argument("--max-delay", type=float, default=3.5, help="Max delay between requests (seconds)")
    parser.add_argument("--batch-flush", type=int, default=DEFAULT_BATCH_FLUSH,
                        help="Checkpoint to disk every N new listings (default: 10)")
    parser.add_argument("--max-bad-pages", type=int, default=DEFAULT_MAX_BAD_PAGES,
                        help="Stop after N consecutive failed/empty catalog pages (default: 3)")
    parser.add_argument("--log-file", type=str, default="crawler.log", help="Log file path")
    parser.add_argument("--upload-b2", action="store_true", default=False,
                        help="Automatically upload crawled Parquet file to Backblaze B2 Data Lake upon completion")
    parser.add_argument("--b2-state-key", type=str, default="state/state.json",
                        help="Backblaze B2 object key for state tracker (default: state/state.json)")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    get_logger(args.log_file)
    _, _, stats = crawl_pipeline(
        target_samples=args.samples,
        output_dir=args.output_dir,
        state_file=args.state_file,
        early_stop_threshold=args.early_stop,
        start_page=args.start_page,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        batch_flush=args.batch_flush,
        max_bad_pages=args.max_bad_pages,
        log_file=args.log_file,
        upload_b2=args.upload_b2,
        b2_state_key=args.b2_state_key,
    )
    if stats.stop_reason.startswith("interrupted"):
        return 130
    if stats.stop_reason.startswith("unexpected error"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
