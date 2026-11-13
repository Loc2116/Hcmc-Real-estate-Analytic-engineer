"""
Module: storage_b2.py
Description: Backblaze B2 Object Storage Manager (S3-compatible API) via boto3.
Author: Senior Data Engineer
Project: HCMC Real Estate ELT Pipeline

Environment Variables Required (in .env or system environment):
    - B2_ENDPOINT_URL: S3-compatible endpoint (e.g., 'https://s3.us-east-005.backblazeb2.com')
    - B2_KEY_ID: Backblaze Application Key ID (maps to aws_access_key_id)
    - B2_APPLICATION_KEY: Backblaze Application Key (maps to aws_secret_access_key)
    - B2_BUCKET_NAME: Target bucket name for Data Lake

Data Lake Partitioning Strategy:
    Hive-style: raw/year=YYYY/month=MM/day=DD/<filename>.parquet
"""

import argparse
import io
import logging
import os
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, ConnectTimeoutError, EndpointConnectionError
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
# Structured Logger Configuration
# -----------------------------------------------------------------------------
logger = logging.getLogger("b2_storage")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [B2Storage] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class B2StorageManager:
    """Manages S3-compatible interactions with Backblaze B2 Data Lake."""

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        key_id: Optional[str] = None,
        application_key: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ):
        """
        Initialize B2 S3 client with environment credentials or explicit arguments.
        """
        self.endpoint_url = (
            endpoint_url or os.getenv("B2_ENDPOINT_URL", "")
        ).strip()
        if self.endpoint_url and not self.endpoint_url.startswith(("http://", "https://")):
            self.endpoint_url = f"https://{self.endpoint_url}"

        self.key_id = (
            key_id or os.getenv("B2_KEY_ID", "")
        ).strip()
        self.application_key = (
            application_key or os.getenv("B2_APPLICATION_KEY", "")
        ).strip()
        self.bucket_name = (
            bucket_name or os.getenv("B2_BUCKET_NAME", "")
        ).strip()

        self._validate_credentials()

        # Backblaze B2 requires Signature Version 4
        self.s3_config = Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        )
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.key_id,
            aws_secret_access_key=self.application_key,
            config=self.s3_config,
        )
        logger.info(
            f"Initialized B2 client -> Endpoint: {self.endpoint_url} | Bucket: {self.bucket_name}"
        )

    def _validate_credentials(self) -> None:
        """Validate presence of all required B2 configuration values."""
        missing = []
        if not self.endpoint_url:
            missing.append("B2_ENDPOINT_URL")
        if not self.key_id:
            missing.append("B2_KEY_ID")
        if not self.application_key:
            missing.append("B2_APPLICATION_KEY")
        if not self.bucket_name:
            missing.append("B2_BUCKET_NAME")

        if missing:
            err_msg = (
                f"Missing required Backblaze B2 environment variables: {', '.join(missing)}. "
                "Please configure them in your .env file or system environment."
            )
            logger.error(err_msg)
            raise ValueError(err_msg)

    # -------------------------------------------------------------------------
    # Partitioning Helpers (Hive-style: raw/year=YYYY/month=MM/day=DD/)
    # -------------------------------------------------------------------------
    @staticmethod
    def get_hive_partition_prefix(
        dt: Optional[datetime] = None, prefix: str = "raw"
    ) -> str:
        """
        Generate Hive-style partition path prefix:
        Example: 'raw/year=2026/month=05/day=12'
        """
        if dt is None:
            dt = datetime.now(timezone.utc)
        return f"{prefix.strip('/')}/year={dt.strftime('%Y')}/month={dt.strftime('%m')}/day={dt.strftime('%d')}"

    @classmethod
    def build_partitioned_key(
        cls,
        filename: str,
        dt: Optional[datetime] = None,
        prefix: str = "raw",
    ) -> str:
        """
        Build full partitioned object key in B2.
        Example: 'raw/year=2026/month=05/day=12/listings_20260512_143000.parquet'
        """
        partition_dir = cls.get_hive_partition_prefix(dt=dt, prefix=prefix)
        return f"{partition_dir}/{filename.lstrip('/')}"

    def derive_s3_key_from_local_path(
        self,
        local_path: Union[str, Path],
        prefix: str = "raw",
        base_dir: Optional[Union[str, Path]] = None,
    ) -> str:
        """
        Derive standardized Hive-style S3 key from a local parquet path.
        Example local:
            'data_1/Raw/year=2026/month=11/day=12/listings_20261112_214826.parquet'
        Returns:
            'raw/year=2026/month=11/day=12/listings_20261112_214826.parquet'
        """
        p = Path(local_path)
        normalized = str(p).replace("\\", "/")
        prefix_clean = prefix.strip("/")

        # 1. Regex search for Hive partition pattern 'year=YYYY/month=MM/day=DD/<filename>.parquet'
        match = re.search(r"(year=\d{4}/month=\d{2}/day=\d{2}/[^/]+\.parquet)$", normalized, re.IGNORECASE)
        if match:
            return f"{prefix_clean}/{match.group(1)}"

        # 2. Try relative to base_dir if provided
        if base_dir:
            try:
                base_p = Path(base_dir).resolve()
                rel = p.resolve().relative_to(base_p)
                rel_str = str(rel).replace("\\", "/")
                return f"{prefix_clean}/{rel_str.lstrip('/')}"
            except ValueError:
                pass

        # 3. Extract timestamp from filename 'listings_YYYYMMDD_HHMMSS.parquet'
        ts_match = re.search(r"(\d{4})(\d{2})(\d{2})_\d{6}", p.name)
        if ts_match:
            y, m, d = ts_match.group(1), ts_match.group(2), ts_match.group(3)
            return f"{prefix_clean}/year={y}/month={m}/day={d}/{p.name}"

        # 4. Fallback: use file modification time or current time
        mtime = p.stat().st_mtime if p.exists() else time.time()
        dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
        return self.build_partitioned_key(filename=p.name, dt=dt, prefix=prefix_clean)

    # -------------------------------------------------------------------------
    # Core Operations with Retry Resilience & Idempotency
    # -------------------------------------------------------------------------
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (ClientError, ConnectTimeoutError, EndpointConnectionError, ConnectionError)
        ),
        reraise=True,
    )
    def test_connection(self, bucket_name: Optional[str] = None) -> bool:
        """
        Verify connection and check if bucket is accessible.
        """
        target_bucket = bucket_name or self.bucket_name
        logger.info(f"Testing connectivity to B2 bucket: '{target_bucket}'...")
        try:
            self.client.head_bucket(Bucket=target_bucket)
            logger.info(f"Connection test PASSED. Bucket '{target_bucket}' is accessible.")
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            logger.error(
                f"Connection test FAILED for bucket '{target_bucket}' (Code: {error_code}): {e}"
            )
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (ConnectTimeoutError, EndpointConnectionError, ConnectionError)
        ),
        reraise=True,
    )
    def object_exists(self, s3_key: str, bucket_name: Optional[str] = None) -> bool:
        """
        Check whether an object exists in Backblaze B2 using head_object.
        Returns True if the object exists, False if not found (404/NoSuchKey).
        """
        target_bucket = bucket_name or self.bucket_name
        try:
            self.client.head_object(Bucket=target_bucket, Key=s3_key)
            return True
        except ClientError as e:
            error_code = str(e.response.get("Error", {}).get("Code", ""))
            http_status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if error_code in ("404", "NoSuchKey", "NotFound") or http_status == 404:
                return False
            logger.error(
                f"head_object failed for 'b2://{target_bucket}/{s3_key}' (Code: {error_code}, Status: {http_status}): {e}"
            )
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (ClientError, ConnectTimeoutError, EndpointConnectionError, ConnectionError)
        ),
        reraise=True,
    )
    def upload_file(
        self,
        local_path: Union[str, Path],
        s3_key: str,
        bucket_name: Optional[str] = None,
    ) -> str:
        """
        Upload local file to B2 S3 key with retry mechanism.
        Returns the s3_key upon success.
        """
        target_bucket = bucket_name or self.bucket_name
        path_obj = Path(local_path)
        if not path_obj.is_file():
            raise FileNotFoundError(f"Local file does not exist: {local_path}")

        file_size_mb = path_obj.stat().st_size / (1024 * 1024)
        logger.info(
            f"Uploading '{path_obj.name}' ({file_size_mb:.2f} MB) -> b2://{target_bucket}/{s3_key}"
        )

        with open(path_obj, "rb") as f:
            self.client.upload_fileobj(f, target_bucket, s3_key)

        logger.info(f"Upload completed successfully: b2://{target_bucket}/{s3_key}")
        return s3_key

    def upload_partitioned_parquet(
        self,
        local_path: Union[str, Path],
        prefix: str = "raw",
        bucket_name: Optional[str] = None,
        overwrite: bool = False,
    ) -> Optional[str]:
        """
        Upload local Parquet file to B2 preserving Hive partition structure.
        Checks existence via head_object first to prevent duplicate uploads.

        Parameters:
            local_path: Path to local .parquet file.
            prefix: Hive root prefix on B2 (default: 'raw').
            bucket_name: Override bucket name.
            overwrite: If False, skip upload if object already exists on B2.

        Returns:
            s3_key if uploaded or already existed, None if local file not found or on error.
        """
        target_bucket = bucket_name or self.bucket_name
        path_obj = Path(local_path)
        if not path_obj.is_file():
            logger.error(f"Local file does not exist: {local_path}")
            return None

        s3_key = self.derive_s3_key_from_local_path(path_obj, prefix=prefix)

        # Check existence on B2 to ensure idempotency
        if not overwrite:
            try:
                if self.object_exists(s3_key, bucket_name=target_bucket):
                    logger.info(
                        f"[SKIP] Object already exists in B2: b2://{target_bucket}/{s3_key} (idempotent)."
                    )
                    return s3_key
            except Exception as e:
                logger.warning(
                    f"Could not verify existence for '{s3_key}' ({e}). Proceeding with upload..."
                )

        return self.upload_file(local_path=path_obj, s3_key=s3_key, bucket_name=target_bucket)

    def sync_raw_directory(
        self,
        raw_dir: Union[str, Path] = "data_1/Raw",
        prefix: str = "raw",
        bucket_name: Optional[str] = None,
        overwrite: bool = False,
    ) -> Dict[str, Any]:
        """
        Scan a local directory for all Parquet files and upload them to B2
        maintaining their Hive-partition structure.

        Parameters:
            raw_dir: Base directory containing Hive partitions (e.g., 'data_1/Raw').
            prefix: Hive root prefix on B2 (default: 'raw').
            bucket_name: Override bucket name.
            overwrite: If True, re-upload even if object already exists.

        Returns:
            Dict summary with counts and uploaded keys.
        """
        target_bucket = bucket_name or self.bucket_name
        raw_path = Path(raw_dir)
        if not raw_path.is_absolute():
            candidate = PROJECT_ROOT / raw_dir
            if candidate.exists():
                raw_path = candidate
            else:
                raw_path = raw_path.resolve()

        if not raw_path.exists():
            logger.error(f"Raw directory not found: {raw_path}")
            return {"scanned": 0, "uploaded": 0, "skipped": 0, "failed": 0, "keys": []}

        parquet_files = sorted([
            p for p in raw_path.rglob("*.parquet")
            if not (p.name.endswith(".tmp") or ".tmp." in p.name or p.name.endswith(".corrupt"))
        ])

        logger.info(
            f"Starting B2 sync from '{raw_path}' -> b2://{target_bucket}/{prefix}/ "
            f"({len(parquet_files)} parquet files found)"
        )

        uploaded_count = 0
        skipped_count = 0
        failed_count = 0
        keys = []

        for pfile in parquet_files:
            s3_key = self.derive_s3_key_from_local_path(pfile, prefix=prefix, base_dir=raw_path)
            try:
                if not overwrite and self.object_exists(s3_key, bucket_name=target_bucket):
                    logger.info(f"[EXISTS] b2://{target_bucket}/{s3_key} (skipped)")
                    skipped_count += 1
                    keys.append(s3_key)
                    continue

                self.upload_file(local_path=pfile, s3_key=s3_key, bucket_name=target_bucket)
                uploaded_count += 1
                keys.append(s3_key)
            except Exception as e:
                logger.error(f"Failed to upload '{pfile}' to B2: {e}")
                failed_count += 1

        summary = {
            "scanned": len(parquet_files),
            "uploaded": uploaded_count,
            "skipped": skipped_count,
            "failed": failed_count,
            "keys": keys,
        }
        logger.info(
            f"Sync complete: Scanned={summary['scanned']} | "
            f"Uploaded={summary['uploaded']} | Skipped={summary['skipped']} | "
            f"Failed={summary['failed']}"
        )
        return summary

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (ClientError, ConnectTimeoutError, EndpointConnectionError, ConnectionError)
        ),
        reraise=True,
    )
    def upload_dataframe_as_parquet(
        self,
        df: pd.DataFrame,
        filename: Optional[str] = None,
        s3_key: Optional[str] = None,
        dt: Optional[datetime] = None,
        prefix: str = "raw",
        bucket_name: Optional[str] = None,
        compression: str = "snappy",
    ) -> str:
        """
        Upload pandas DataFrame directly to B2 as Parquet without local disk write.
        If s3_key is omitted, auto-generates Hive partitioned key using current date.
        """
        target_bucket = bucket_name or self.bucket_name
        if dt is None:
            dt = datetime.now(timezone.utc)

        if s3_key is None:
            if filename is None:
                ts = dt.strftime("%Y%m%d_%H%M%S")
                filename = f"hcmc_listings_{ts}.parquet"
            s3_key = self.build_partitioned_key(filename=filename, dt=dt, prefix=prefix)

        logger.info(
            f"Streaming DataFrame ({df.shape[0]} rows, {df.shape[1]} cols) as Parquet -> b2://{target_bucket}/{s3_key}"
        )

        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", compression=compression, index=False)
        buffer.seek(0)
        buffer_size_mb = buffer.getbuffer().nbytes / (1024 * 1024)

        self.client.upload_fileobj(
            buffer,
            target_bucket,
            s3_key,
            ExtraArgs={"ContentType": "application/octet-stream"},
        )
        logger.info(
            f"Parquet uploaded successfully: b2://{target_bucket}/{s3_key} ({buffer_size_mb:.2f} MB)"
        )
        return s3_key

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (ClientError, ConnectTimeoutError, EndpointConnectionError, ConnectionError)
        ),
        reraise=True,
    )
    def download_file(
        self,
        s3_key: str,
        local_path: Union[str, Path],
        bucket_name: Optional[str] = None,
    ) -> Path:
        """
        Download object from B2 to local destination path.
        """
        target_bucket = bucket_name or self.bucket_name
        dest_path = Path(local_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading b2://{target_bucket}/{s3_key} -> {dest_path}")
        with open(dest_path, "wb") as f:
            self.client.download_fileobj(target_bucket, s3_key, f)

        logger.info(f"Download complete: {dest_path}")
        return dest_path

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(
            (ClientError, ConnectTimeoutError, EndpointConnectionError, ConnectionError)
        ),
        reraise=True,
    )
    def read_parquet_to_dataframe(
        self,
        s3_key: str,
        bucket_name: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Read a Parquet file directly from B2 into a pandas DataFrame in memory.
        """
        target_bucket = bucket_name or self.bucket_name
        logger.info(f"Reading Parquet from b2://{target_bucket}/{s3_key} into DataFrame...")
        buffer = io.BytesIO()
        self.client.download_fileobj(target_bucket, s3_key, buffer)
        buffer.seek(0)
        df = pd.read_parquet(buffer, engine="pyarrow")
        logger.info(f"Successfully loaded DataFrame: {df.shape[0]} rows, {df.shape[1]} cols")
        return df

    def list_objects(
        self,
        prefix: str = "",
        bucket_name: Optional[str] = None,
        max_keys: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        List objects in the bucket matching a prefix with pagination.
        """
        target_bucket = bucket_name or self.bucket_name
        paginator = self.client.get_paginator("list_objects_v2")
        results = []

        logger.info(f"Listing objects in b2://{target_bucket}/{prefix}...")
        for page in paginator.paginate(
            Bucket=target_bucket,
            Prefix=prefix,
            PaginationConfig={"MaxItems": max_keys},
        ):
            for obj in page.get("Contents", []):
                results.append(
                    {
                        "Key": obj["Key"],
                        "Size": obj["Size"],
                        "LastModified": obj["LastModified"],
                    }
                )

        logger.info(f"Found {len(results)} objects with prefix '{prefix}'")
        return results

    def delete_object(
        self,
        s3_key: str,
        bucket_name: Optional[str] = None,
    ) -> bool:
        """Delete an object from B2 bucket."""
        target_bucket = bucket_name or self.bucket_name
        logger.info(f"Deleting b2://{target_bucket}/{s3_key}...")
        self.client.delete_object(Bucket=target_bucket, Key=s3_key)
        logger.info(f"Deleted b2://{target_bucket}/{s3_key}")
        return True


# -----------------------------------------------------------------------------
# Module Singleton Helper
# -----------------------------------------------------------------------------
_b2_manager_instance: Optional[B2StorageManager] = None


def get_b2_manager() -> B2StorageManager:
    """Get or create singleton instance of B2StorageManager."""
    global _b2_manager_instance
    if _b2_manager_instance is None:
        _b2_manager_instance = B2StorageManager()
    return _b2_manager_instance


def verify_b2_connection_and_list(
    prefix: str = "raw",
    max_keys: int = 50,
    manager: Optional[B2StorageManager] = None,
) -> List[Dict[str, Any]]:
    """
    Sanity check function:
      1. Verifies B2 connectivity via head_bucket.
      2. Lists and prints all objects matching prefix (default: 'raw').
    """
    mgr = manager or get_b2_manager()
    print("=" * 75)
    print("BACKBLAZE B2 DATA LAKE VERIFICATION")
    print("=" * 75)
    print(f"B2 Endpoint   : {mgr.endpoint_url}")
    print(f"Target Bucket : {mgr.bucket_name}")
    print("Testing connection...")
    mgr.test_connection()
    print("✅ Connection test PASSED!\n")

    print(f"Listing objects under prefix '{prefix}' (up to {max_keys}):")
    objects = mgr.list_objects(prefix=prefix, max_keys=max_keys)
    if not objects:
        print(f"  [i] No objects found with prefix '{prefix}'. Bucket has no matching files.")
    else:
        print(f"  {'Key':<58} | {'Size (KB)':<10} | {'Last Modified'}")
        print("  " + "-" * 90)
        for obj in objects:
            size_kb = obj["Size"] / 1024
            lm = str(obj["LastModified"])[:19]
            print(f"  {obj['Key']:<58} | {size_kb:>8.2f} KB | {lm}")
        print("  " + "-" * 90)
        print(f"Total objects found: {len(objects)}")
    print("=" * 75)
    return objects


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backblaze B2 S3 Storage Manager for HCMC Real Estate Data Lake"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test connection to Backblaze B2 bucket",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List objects in the bucket",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="raw",
        help="Prefix to filter listed/uploaded objects (default: 'raw')",
    )
    parser.add_argument(
        "--sync",
        type=str,
        nargs="?",
        const="data_1/Raw",
        help="Sync local directory to B2 (default: data_1/Raw)",
    )
    parser.add_argument(
        "--upload",
        type=str,
        help="Upload single local file to B2 Hive partition",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Force overwrite existing objects on B2",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        manager = get_b2_manager()

        if args.test:
            manager.test_connection()
            print("✅ B2 connection test SUCCESSFUL!")
            return 0

        if args.upload:
            uploaded_key = manager.upload_partitioned_parquet(
                local_path=args.upload,
                prefix=args.prefix,
                overwrite=args.overwrite,
            )
            if uploaded_key:
                print(f"✅ Uploaded: b2://{manager.bucket_name}/{uploaded_key}")
            return 0

        if args.sync is not None:
            sync_dir = args.sync
            summary = manager.sync_raw_directory(
                raw_dir=sync_dir,
                prefix=args.prefix,
                overwrite=args.overwrite,
            )
            print("\nSync Summary:")
            print(f"  Scanned  : {summary['scanned']}")
            print(f"  Uploaded : {summary['uploaded']}")
            print(f"  Skipped  : {summary['skipped']}")
            print(f"  Failed   : {summary['failed']}")
            return 0 if summary["failed"] == 0 else 1

        if args.list:
            verify_b2_connection_and_list(prefix=args.prefix, manager=manager)
            return 0

        # Default action when no flags given: verify connection and list
        verify_b2_connection_and_list(prefix=args.prefix, manager=manager)
        return 0

    except ValueError as ve:
        print(f"[CONFIGURATION ERROR] {ve}")
        print("\nPlease verify your .env file in the project root:")
        print("  B2_ENDPOINT_URL=s3.us-east-005.backblazeb2.com")
        print("  B2_KEY_ID=<your-application-key-id>")
        print("  B2_APPLICATION_KEY=<your-application-key>")
        print("  B2_BUCKET_NAME=<your-bucket-name>")
        return 1
    except Exception as ex:
        print(f"[OPERATION FAILED] {ex}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
