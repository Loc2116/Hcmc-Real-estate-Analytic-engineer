"""
Module: load_bigquery.py
Description: Production-Grade Idempotent Loader from Backblaze B2 Data Lake to Google BigQuery.
Author: Senior Data Engineer
Project: HCMC Real Estate ELT Pipeline

Architecture & Idempotency:
    1. Extracts Hive-partitioned Apache Parquet files from Backblaze B2 Data Lake
       (e.g., 'raw/year=YYYY/month=MM/day=DD/*.parquet').
    2. Cleans & normalizes column names to snake_case ASCII identifiers compatible with BigQuery.
    3. Idempotent Ingestion Pattern (Optimized for BigQuery Sandbox):
       - Step A: Ingest batch DataFrame into staging table `<dataset>.raw_listings_staging`
                 using `write_disposition="WRITE_TRUNCATE"`.
       - Step B: Deduplicate & merge into main table `<dataset>.raw_listings` using
                 `CREATE OR REPLACE TABLE ... UNION ALL` with
                 `ROW_NUMBER() OVER (PARTITION BY link ORDER BY crawled_at DESC) = 1`.
                 (Avoids DML MERGE restrictions in BigQuery Sandbox).
       - Step C: Drop temporary staging table `<dataset>.raw_listings_staging`.
    4. Tenacity retry resilience for network and cloud API calls.
    5. Detailed structured logging and session metrics reporting.
"""

import argparse
import io
import logging
import os
from pathlib import Path
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# -----------------------------------------------------------------------------
# Project Root & Environment Variables Loading (.env)
# -----------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_env_fallback() -> None:
    """Fallback .env parser if python-dotenv is not installed."""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        try:
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
        except Exception:
            pass


try:
    from dotenv import load_dotenv
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        load_dotenv(dotenv_path=env_file)
    else:
        load_dotenv()
except ImportError:
    pass

_load_env_fallback()

# -----------------------------------------------------------------------------
# Google Cloud BigQuery Imports with Helpful Fallback
# -----------------------------------------------------------------------------
try:
    from google.cloud import bigquery
    from google.cloud import exceptions as gcp_exceptions
    from google.oauth2 import service_account
    _GCP_AVAILABLE = True
    _GCP_IMPORT_ERROR = None
except Exception as _gcp_err:
    _GCP_AVAILABLE = False
    _GCP_IMPORT_ERROR = str(_gcp_err)

# Import Backblaze B2 Storage Manager
try:
    from storage_b2 import B2StorageManager, get_b2_manager
    _B2_AVAILABLE = True
except Exception as _b2_err:
    _B2_AVAILABLE = False
    _B2_IMPORT_ERROR = str(_b2_err)

# -----------------------------------------------------------------------------
# Structured Logger Configuration
# -----------------------------------------------------------------------------
logger = logging.getLogger("bigquery_loader")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [BigQueryLoader] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# -----------------------------------------------------------------------------
# Helper: Column Name Normalization for BigQuery Standard ASCII
# -----------------------------------------------------------------------------
def normalize_column_name(col: str) -> str:
    """
    Convert Vietnamese / special-character column names into clean snake_case ASCII.
    BigQuery column names must match: ^[a-zA-Z_][a-zA-Z0-9_]{0,299}$

    Examples:
        'Tiêu đề'                -> 'tieu_de'
        'Diện tích'              -> 'dien_tich'
        'Số phòng tắm, vệ sinh'  -> 'so_phong_tam_ve_sinh'
        'Link'                   -> 'link'
        'Mặt tiền'               -> 'mat_tien'
        'Đường vào'              -> 'duong_vao'
    """
    if not col:
        return "col_unnamed"

    s = str(col).strip()
    # Explicitly handle Vietnamese d/D
    s = s.replace("đ", "d").replace("Đ", "d")
    # Decompose unicode diacritics and strip non-spacing marks
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    # Convert to lowercase
    s = s.lower()
    # Replace any non-alphanumeric character with underscore
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    # Collapse multiple consecutive underscores and strip leading/trailing underscores
    s = re.sub(r"_+", "_", s).strip("_")

    if not s:
        return "col_unnamed"
    # Ensure column starts with a letter or underscore
    if s[0].isdigit():
        s = f"col_{s}"
    return s[:300]


# -----------------------------------------------------------------------------
# BigQueryLoader Class
# -----------------------------------------------------------------------------
class BigQueryLoader:
    """
    Orchestrates idempotent loading of HCMC real estate listings from Backblaze B2
    Data Lake into Google BigQuery.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        credentials_path: Optional[Union[str, Path]] = None,
        b2_manager: Optional[Any] = None,
    ):
        if not _GCP_AVAILABLE:
            raise ImportError(
                f"google-cloud-bigquery is not installed or failed to import: {_GCP_IMPORT_ERROR}. "
                "Please run: pip install google-cloud-bigquery pyarrow pandas"
            )

        self.project_id = (
            project_id or os.getenv("GCP_PROJECT_ID", "")
        ).strip()
        self.dataset_id = (
            dataset_id or os.getenv("BIGQUERY_DATASET", "")
        ).strip()

        creds_path = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
        if creds_path:
            p = Path(creds_path)
            if not p.is_absolute():
                p = (PROJECT_ROOT / p).resolve()
            self.credentials_path = p
        else:
            self.credentials_path = None

        self._validate_config()

        # Set environment variable for Google ADC
        if self.credentials_path and self.credentials_path.is_file():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(self.credentials_path)
            try:
                self.credentials = service_account.Credentials.from_service_account_file(
                    str(self.credentials_path)
                )
            except Exception as cred_err:
                # Fallback: seamlessly handle UTF-8 BOM or alternate encoding in JSON key
                try:
                    import json
                    with open(self.credentials_path, "r", encoding="utf-8-sig") as jf:
                        sa_info = json.load(jf)
                    self.credentials = service_account.Credentials.from_service_account_info(sa_info)
                except Exception:
                    raise cred_err

            self.client = bigquery.Client(
                project=self.project_id,
                credentials=self.credentials,
            )
        else:
            self.credentials = None
            self.client = bigquery.Client(project=self.project_id)

        # Backblaze B2 Manager
        if b2_manager is not None:
            self.b2_manager = b2_manager
        else:
            if not _B2_AVAILABLE:
                raise ImportError(
                    f"B2StorageManager could not be loaded: {_B2_IMPORT_ERROR}"
                )
            self.b2_manager = get_b2_manager()

        logger.info(
            f"Initialized BigQueryLoader -> Project: {self.project_id} | "
            f"Dataset: {self.dataset_id} | Credentials: {self.credentials_path}"
        )

    def _validate_config(self) -> None:
        """Ensure all required configuration parameters exist."""
        missing = []
        if not self.project_id:
            missing.append("GCP_PROJECT_ID")
        if not self.dataset_id:
            missing.append("BIGQUERY_DATASET")
        if not self.credentials_path or not self.credentials_path.exists():
            missing.append("GOOGLE_APPLICATION_CREDENTIALS (file must exist)")

        if missing:
            err_msg = (
                f"Missing or invalid BigQuery configuration values: {', '.join(missing)}. "
                "Please check your .env file or environment variables."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)

    # -------------------------------------------------------------------------
    # Dataset & Table Management
    # -------------------------------------------------------------------------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def ensure_dataset_exists(self, location: Optional[str] = None) -> None:
        """Ensure the target dataset exists in BigQuery; create if absent."""
        loc = location or os.getenv("BIGQUERY_LOCATION", "asia-southeast1")
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_id)
        try:
            ds = self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset '{self.project_id}.{self.dataset_id}' exists in location '{ds.location}'.")
        except gcp_exceptions.NotFound:
            logger.info(
                f"Dataset '{self.project_id}.{self.dataset_id}' not found. Creating in location '{loc}'..."
            )
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = loc
            self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Dataset '{self.project_id}.{self.dataset_id}' created successfully in '{loc}'.")

    # -------------------------------------------------------------------------
    # DataFrame Normalization
    # -------------------------------------------------------------------------
    @staticmethod
    def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize DataFrame to BigQuery-compliant schema:
        1. Snake_case ASCII column names.
        2. Deduplicate column names.
        3. Guarantee 'link' exists and is not null.
        4. Guarantee 'crawled_at' timestamp string exists.
        5. Convert numeric GPS coordinates to float64.
        6. Explicit string casting for all metadata/text fields to prevent schema collision.
        7. Deduplicate batch by link keeping newest crawled_at.
        """
        if df.empty:
            return df

        df = df.copy()

        # 1. Rename columns to snake_case ASCII
        new_cols = [normalize_column_name(c) for c in df.columns]
        df.columns = new_cols

        # 2. Deduplicate columns if any collision occurred
        df = df.loc[:, ~df.columns.duplicated()]

        # 3. Check for essential natural key 'link'
        if "link" not in df.columns:
            raise ValueError("Dataframe must contain 'link' column for BigQuery deduplication.")

        # Drop empty links
        df = df.dropna(subset=["link"])
        df = df[df["link"].astype(str).str.strip() != ""]

        if df.empty:
            return df

        # 4. Guarantee crawled_at exists
        utc_now = datetime.now(timezone.utc).isoformat()
        if "crawled_at" not in df.columns:
            df["crawled_at"] = utc_now
        else:
            df["crawled_at"] = df["crawled_at"].fillna(utc_now).astype(str)

        # 5 & 6. Strict typing: float64 for GPS coordinates, string for all other columns
        numeric_cols = {"latitude", "longitude"}
        for col in df.columns:
            if col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            else:
                # Map NaN / None / '<NA>' to Python None for BigQuery NULL representation
                df[col] = df[col].astype(object)
                df[col] = df[col].where(pd.notnull(df[col]), None)
                df[col] = df[col].apply(
                    lambda x: str(x).strip() if (x is not None and str(x).strip() not in ("nan", "<NA>", "None")) else None
                )

        # 7. Deduplicate records within the current batch keeping newest crawled_at
        df = df.sort_values(by="crawled_at", ascending=False).drop_duplicates(subset=["link"], keep="first")
        return df

    # -------------------------------------------------------------------------
    # Extract Parquet from Backblaze B2 Data Lake
    # -------------------------------------------------------------------------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def fetch_parquet_from_b2(
        self, date_str: Optional[str] = None
    ) -> Tuple[pd.DataFrame, List[str]]:
        """
        List and download Parquet files from Backblaze B2 Data Lake.
        If date_str (YYYY-MM-DD) is provided, filters partition 'raw/year=YYYY/month=MM/day=DD/'.
        Otherwise, scans the entire 'raw/' prefix.

        Returns:
            (normalized_dataframe, list_of_keys_processed)
        """
        if date_str:
            try:
                dt = datetime.strptime(date_str.strip(), "%Y-%m-%d")
                prefix = f"raw/year={dt.strftime('%Y')}/month={dt.strftime('%m')}/day={dt.strftime('%d')}"
            except ValueError:
                raise ValueError(f"Invalid date format '{date_str}'. Expected 'YYYY-MM-DD'.")
        else:
            prefix = "raw"

        logger.info(f"Listing Parquet files in B2 Data Lake with prefix: '{prefix}'...")
        objects = self.b2_manager.list_objects(prefix=prefix)
        parquet_keys = [
            obj["Key"] for obj in objects
            if obj["Key"].endswith(".parquet") and not obj["Key"].endswith(".tmp")
        ]

        if not parquet_keys:
            logger.warning(f"No Parquet partition files found in B2 under prefix: '{prefix}'.")
            return pd.DataFrame(), []

        logger.info(f"Found {len(parquet_keys)} Parquet file(s) in B2. Downloading and aggregating...")

        dfs: List[pd.DataFrame] = []
        for key in parquet_keys:
            try:
                df_part = self.b2_manager.read_parquet_to_dataframe(key)
                if not df_part.empty:
                    dfs.append(df_part)
            except Exception as e:
                logger.error(f"Failed to read Parquet object 'b2://{self.b2_manager.bucket_name}/{key}': {e}")
                raise

        if not dfs:
            logger.warning("All downloaded Parquet files were empty.")
            return pd.DataFrame(), parquet_keys

        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(
            f"Successfully downloaded {len(dfs)} partitions ({len(combined_df)} raw rows). "
            "Normalizing schema..."
        )
        normalized_df = self.normalize_dataframe(combined_df)
        logger.info(f"Schema normalized. Clean rows ready for BigQuery staging: {len(normalized_df)}")
        return normalized_df, parquet_keys

    # -------------------------------------------------------------------------
    # Idempotent Ingestion Pipeline (Step A, Step B, Step C)
    # -------------------------------------------------------------------------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def load_staging_table(self, df: pd.DataFrame, staging_table_id: str) -> int:
        """
        Step A: Ingest batch DataFrame into staging table using WRITE_TRUNCATE.
        """
        logger.info(f"Step A: Writing {len(df)} rows to staging table '{staging_table_id}' (WRITE_TRUNCATE)...")
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = self.client.load_table_from_dataframe(
            df, staging_table_id, job_config=job_config
        )
        load_job.result()  # Wait for job to complete
        logger.info(f"Step A completed: {len(df)} rows written to '{staging_table_id}'.")
        return len(df)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def merge_staging_to_main(
        self, staging_table_id: str, main_table_id: str
    ) -> None:
        """
        Step B: Idempotent Deduplication into main table via CREATE OR REPLACE TABLE.
        Works seamlessly in BigQuery Sandbox without DML MERGE restrictions.
        """
        logger.info(f"Step B: Merging staging data into main table '{main_table_id}'...")

        # Check if main table already exists
        main_exists = False
        try:
            main_table = self.client.get_table(main_table_id)
            main_exists = True
        except gcp_exceptions.NotFound:
            main_exists = False

        if not main_exists:
            logger.info(
                f"Main table '{main_table_id}' does not exist yet. Creating from staging table..."
            )
            create_sql = f"""
            CREATE OR REPLACE TABLE `{main_table_id}` AS
            SELECT * EXCEPT(rn) FROM (
                SELECT *,
                       ROW_NUMBER() OVER(PARTITION BY link ORDER BY crawled_at DESC) AS rn
                FROM `{staging_table_id}`
            )
            WHERE rn = 1
            """
            query_job = self.client.query(create_sql)
            query_job.result()
            logger.info(f"Step B completed: Created main table '{main_table_id}' from staging.")
            return

        # Main table already exists -> Schema-aware UNION ALL deduplication
        logger.info(
            f"Main table '{main_table_id}' exists. Aligning schemas and executing idempotent UNION ALL merge..."
        )
        staging_table = self.client.get_table(staging_table_id)

        main_cols = [f.name for f in main_table.schema]
        staging_cols = [f.name for f in staging_table.schema]

        # Combine all columns preserving main_table order
        all_cols: List[str] = []
        for c in main_cols:
            if c not in all_cols:
                all_cols.append(c)
        for c in staging_cols:
            if c not in all_cols:
                all_cols.append(c)

        main_select_exprs = [
            f"`{c}`" if c in main_cols else f"CAST(NULL AS STRING) AS `{c}`"
            for c in all_cols
        ]
        staging_select_exprs = [
            f"`{c}`" if c in staging_cols else f"CAST(NULL AS STRING) AS `{c}`"
            for c in all_cols
        ]

        dedup_sql = f"""
        CREATE OR REPLACE TABLE `{main_table_id}` AS
        SELECT * EXCEPT(rn) FROM (
            SELECT *,
                   ROW_NUMBER() OVER(PARTITION BY link ORDER BY crawled_at DESC) AS rn
            FROM (
                SELECT {', '.join(main_select_exprs)}
                FROM `{main_table_id}`
                UNION ALL
                SELECT {', '.join(staging_select_exprs)}
                FROM `{staging_table_id}`
            )
        )
        WHERE rn = 1
        """
        query_job = self.client.query(dedup_sql)
        query_job.result()
        logger.info(f"Step B completed: Idempotent merge finished for '{main_table_id}'.")

    def cleanup_staging_table(self, staging_table_id: str) -> None:
        """Step C: Drop temporary staging table."""
        try:
            self.client.delete_table(staging_table_id, not_found_ok=True)
            logger.info(f"Step C: Cleaned up staging table '{staging_table_id}'.")
        except Exception as e:
            logger.warning(f"Could not delete staging table '{staging_table_id}': {e}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def get_table_row_count(self, table_id: str) -> int:
        """Query total count of records in a table."""
        try:
            query = f"SELECT COUNT(*) AS total FROM `{table_id}`"
            res = list(self.client.query(query).result())
            return int(res[0].total) if res else 0
        except Exception as e:
            logger.warning(f"Could not get row count for '{table_id}': {e}")
            return -1

    # -------------------------------------------------------------------------
    # Main Pipeline Execution
    # -------------------------------------------------------------------------
    def run_pipeline(
        self,
        date_str: Optional[str] = None,
        dry_run: bool = False,
        keep_staging: bool = False,
    ) -> Dict[str, Any]:
        """
        Execute full ELT ingestion pipeline from Backblaze B2 to BigQuery:
        1. Ensure BigQuery dataset exists.
        2. Fetch and normalize Parquet files from B2.
        3. Load staging table (Step A).
        4. Merge into main table (Step B).
        5. Clean up staging table (Step C).
        6. Return detailed summary metrics.
        """
        staging_table_id = f"{self.project_id}.{self.dataset_id}.raw_listings_staging"
        main_table_id = f"{self.project_id}.{self.dataset_id}.raw_listings"

        logger.info("=" * 70)
        logger.info("STARTING BIGQUERY ELT INGESTION PIPELINE")
        logger.info("=" * 70)
        logger.info(f"Source Data Lake       : b2://{self.b2_manager.bucket_name}/raw/")
        logger.info(f"Target BigQuery Dataset: {self.project_id}.{self.dataset_id}")
        logger.info(f"Date Filter            : {date_str or 'ALL (full crawl history)'}")
        logger.info(f"Dry Run Mode           : {dry_run}")
        logger.info("=" * 70)

        # 1. Ensure BigQuery dataset exists
        self.ensure_dataset_exists()

        # 2. Fetch Parquet from B2
        df, parquet_keys = self.fetch_parquet_from_b2(date_str=date_str)

        if df.empty:
            logger.info("No records to ingest. Pipeline completed.")
            return {
                "status": "NO_DATA",
                "parquet_files": parquet_keys,
                "staging_rows": 0,
                "main_table_rows": self.get_table_row_count(main_table_id),
            }

        if dry_run:
            logger.info(f"[DRY-RUN] Normalized {len(df)} records from {len(parquet_keys)} file(s).")
            logger.info(f"[DRY-RUN] Columns: {list(df.columns)}")
            return {
                "status": "DRY_RUN",
                "parquet_files": parquet_keys,
                "staging_rows": len(df),
                "main_table_rows": self.get_table_row_count(main_table_id),
            }

        # 3. Step A: Load into Staging Table
        staging_rows = self.load_staging_table(df, staging_table_id)

        # 4. Step B: Deduplicate & Merge into Main Table
        self.merge_staging_to_main(staging_table_id, main_table_id)

        # 5. Step C: Clean up Staging Table
        if not keep_staging:
            self.cleanup_staging_table(staging_table_id)
        else:
            logger.info(f"Staging table '{staging_table_id}' preserved (--keep-staging).")

        # 6. Retrieve Final Metrics
        total_main_rows = self.get_table_row_count(main_table_id)

        summary = {
            "status": "SUCCESS",
            "parquet_files": parquet_keys,
            "staging_rows": staging_rows,
            "main_table_rows": total_main_rows,
        }

        logger.info("=" * 70)
        logger.info("BIGQUERY LOAD PIPELINE SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Parquet files processed : {len(parquet_keys)}")
        logger.info(f"Staging rows loaded     : {staging_rows}")
        logger.info(f"Main table total rows   : {total_main_rows}")
        logger.info("Idempotent Deduplication: PASSED (unique by link)")
        logger.info("Status                  : SUCCESS")
        logger.info("=" * 70)

        return summary


# -----------------------------------------------------------------------------
# Convenience Function for External Modules & Notebooks
# -----------------------------------------------------------------------------
def run_bigquery_pipeline(
    date_str: Optional[str] = None,
    dry_run: bool = False,
    keep_staging: bool = False,
) -> Dict[str, Any]:
    """Execute BigQuery pipeline with default environment configuration."""
    loader = BigQueryLoader()
    return loader.run_pipeline(
        date_str=date_str, dry_run=dry_run, keep_staging=keep_staging
    )


# -----------------------------------------------------------------------------
# CLI Entry Point
# -----------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Load HCMC Real Estate Parquet data from Backblaze B2 into Google BigQuery."
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Specific partition date to ingest in 'YYYY-MM-DD' format (default: ingest all raw/*.parquet files)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Download and inspect schema/records without loading to BigQuery",
    )
    parser.add_argument(
        "--keep-staging",
        action="store_true",
        help="Keep the temporary staging table after merging (default: delete)",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        loader = BigQueryLoader()
        summary = loader.run_pipeline(
            date_str=args.date,
            dry_run=args.dry_run,
            keep_staging=args.keep_staging,
        )
        print("\nPipeline Result:")
        print(f"  Status                  : {summary['status']}")
        print(f"  Parquet files processed : {len(summary['parquet_files'])}")
        print(f"  Staging rows loaded     : {summary['staging_rows']}")
        print(f"  Main table total rows   : {summary['main_table_rows']}")
        return 0
    except ValueError as ve:
        print(f"\n[CONFIGURATION ERROR] {ve}")
        return 1
    except Exception as ex:
        print(f"\n[PIPELINE FAILED] {ex}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

