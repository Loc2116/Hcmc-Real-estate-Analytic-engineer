"""
Module: ingest_legacy_csv.py
Description: Ingest historical raw CSV (data_bds_scrapling_raw.csv) into Modern ELT Data Platform:
             1. Standardize schema & dtypes (matching crawler Parquet specification).
             2. Parse 'Ngày đăng' into ISO-8601 UTC timestamp for 'crawled_at'.
             3. Serialize to Hive-partitioned Apache Parquet (Snappy).
             4. Upload to Backblaze B2 Data Lake (raw/year=2026/month=11/day=06/).
             5. Synchronize all listing IDs/URLs into state.json and upload to B2.
"""

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
import sys
import pandas as pd

# Project Root Setup
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage_b2 import get_b2_manager

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [LegacyIngest] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("legacy_ingest")


def parse_date_to_iso(date_str: Any) -> str:
    """Parse 'DD/MM/YYYY' into ISO-8601 UTC timestamp string."""
    if pd.isna(date_str) or not str(date_str).strip():
        return "2026-11-06T12:00:00Z"
    s = str(date_str).strip()
    match = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if match:
        day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            dt = datetime(year, month, day, 12, 0, 0, tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            pass
    return "2026-11-06T12:00:00Z"


def main():
    csv_path = PROJECT_ROOT / "data_1" / "Raw" / "data_bds_scrapling_raw.csv"
    if not csv_path.exists():
        logger.error(f"File not found: {csv_path}")
        return 1

    logger.info(f"Reading historical CSV from '{csv_path}'...")
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns.")

    # 1. Ensure 'crawled_at' exists
    if "crawled_at" not in df.columns:
        logger.info("Generating 'crawled_at' from 'Ngày đăng'...")
        df["crawled_at"] = df["Ngày đăng"].apply(parse_date_to_iso)
    else:
        df["crawled_at"] = df["crawled_at"].fillna("2026-11-06T12:00:00Z").astype(str)

    # 2. Strict type enforcement: Latitude/Longitude float64, all others string
    numeric_cols = {"Latitude", "Longitude"}
    for col in df.columns:
        if col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        else:
            df[col] = df[col].astype("string")

    # 3. Hive partition destination
    partition_dir = PROJECT_ROOT / "data_1" / "Raw" / "year=2026" / "month=11" / "day=06"
    partition_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = partition_dir / "listings_20261106_historical_backfill.parquet"

    logger.info(f"Serializing {len(df)} rows to Parquet Snappy: '{parquet_path}'...")
    df.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
    logger.info(f"Parquet file written successfully ({parquet_path.stat().st_size / 1024:.1f} KB).")

    # 4. Upload to Backblaze B2 Data Lake
    logger.info("Uploading Parquet file to Backblaze B2 Data Lake...")
    b2_mgr = get_b2_manager()
    s3_key = b2_mgr.upload_partitioned_parquet(
        local_path=parquet_path,
        prefix="raw",
        overwrite=True,
    )
    logger.info(f"Uploaded successfully to b2://{b2_mgr.bucket_name}/{s3_key}")

    # 5. Synchronize URLs and IDs into state.json and upload to B2
    state_file = PROJECT_ROOT / "state.json"
    seen_urls = set()
    seen_ids = set()
    if state_file.exists():
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                cur_state = json.load(f)
                seen_urls = set(cur_state.get("seen_urls", []))
                seen_ids = set(cur_state.get("seen_ids", []))
        except Exception as e:
            logger.warning(f"Could not read existing state.json: {e}")

    prev_count = len(seen_urls)
    for link in df["Link"].dropna():
        l_str = str(link).strip()
        seen_urls.add(l_str)
        match = re.search(r"pr(\d+)", l_str)
        if match:
            seen_ids.add(match.group(1))

    if "Mã tin" in df.columns:
        for mid in df["Mã tin"].dropna():
            seen_ids.add(str(mid).strip())

    logger.info(
        f"Updated state: {len(seen_urls)} total URLs (+{len(seen_urls) - prev_count} new), {len(seen_ids)} IDs."
    )

    state_data = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_seen_urls": len(seen_urls),
        "total_seen_ids": len(seen_ids),
        "seen_urls": sorted(seen_urls),
        "seen_ids": sorted(seen_ids),
    }

    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(state_data, f, ensure_ascii=False, indent=2)

    logger.info("Uploading updated state.json to Backblaze B2...")
    b2_mgr.upload_file(local_path=state_file, s3_key="state/state.json")
    logger.info("Historical backfill completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())

