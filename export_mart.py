"""
Script: export_mart.py
Description: Export mart_listings_clean from Google BigQuery to local CSV and Parquet files.
Usage:
    python export_mart.py
    python export_mart.py --output-dir data/exported --format both
"""

import argparse
import logging
import os
from pathlib import Path
import sys

from dotenv import load_dotenv
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [ExportMart] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("export_mart")


def export_mart(output_dir: str = "data_exports", export_format: str = "both"):
    load_dotenv(PROJECT_ROOT / ".env")

    # Service account check
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and not os.path.isabs(cred_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(PROJECT_ROOT / cred_path)

    project_id = os.getenv("GCP_PROJECT_ID", "hcmcrealestatepriceclf")
    dataset_id = os.getenv("BIGQUERY_DATASET", "hcmc_real_estate")
    table_name = "mart_listings_clean"

    from google.cloud import bigquery

    logger.info(f"Connecting to BigQuery project '{project_id}'...")
    client = bigquery.Client(project=project_id)

    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    query = f"SELECT * FROM `{full_table_id}` ORDER BY crawled_date DESC, price_billion_vnd DESC"

    logger.info(f"Querying `{full_table_id}`...")
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df):,} rows and {len(df.columns)} columns.")

    out_path = PROJECT_ROOT / output_dir
    out_path.mkdir(parents=True, exist_ok=True)

    csv_file = out_path / f"{table_name}.csv"
    parquet_file = out_path / f"{table_name}.parquet"

    if export_format in ("csv", "both"):
        logger.info(f"Exporting CSV to '{csv_file}'...")
        df.to_csv(csv_file, index=False, encoding="utf-8-sig")
        logger.info(f"CSV saved ({csv_file.stat().st_size / 1024:.1f} KB).")

    if export_format in ("parquet", "both"):
        logger.info(f"Exporting Parquet (Snappy) to '{parquet_file}'...")
        df.to_parquet(parquet_file, index=False, engine="pyarrow", compression="snappy")
        logger.info(f"Parquet saved ({parquet_file.stat().st_size / 1024:.1f} KB).")

    print("\n" + "=" * 60)
    print(" EXPORT COMPLETED SUCCESSFULLY!")
    print(f" Total rows:    {len(df):,}")
    print(f" Total columns: {len(df.columns)}")
    if export_format in ("csv", "both"):
        print(f" CSV Path:      {csv_file.resolve()}")
    if export_format in ("parquet", "both"):
        print(f" Parquet Path:  {parquet_file.resolve()}")
    print("=" * 60 + "\n")
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export mart_listings_clean from BigQuery")
    parser.add_argument("--output-dir", type=str, default="data_exports", help="Folder to save files")
    parser.add_argument("--format", type=str, choices=["csv", "parquet", "both"], default="both", help="Export format")
    args = parser.parse_args()

    export_mart(output_dir=args.output_dir, export_format=args.format)
