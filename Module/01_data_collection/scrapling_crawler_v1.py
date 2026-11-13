"""
Module: scrapling_crawler_v1.py
Description: Production-Grade Incremental & Fault-tolerant Real Estate Crawler for batdongsan.com.vn.
Note: Synchronized with scrapling_crawler.py to support Apache Parquet with Hive-style partitioning.
"""

from scrapling_crawler import (
    CrawlStats,
    IncrementalStateManager,
    RetryableHTTPError,
    apply_rate_limit,
    crawl_pipeline,
    extract_from_desc,
    extract_property_details,
    fetch_page_with_retry,
    fill_specs_with_desc,
    get_hive_partition_path,
    get_listing_urls,
    get_logger,
    get_random_headers,
    main,
    setup_crawler_logger,
)

if __name__ == "__main__":
    import sys
    sys.exit(main())
