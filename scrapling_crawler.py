"""
Root-level CLI entrypoint for HCMC Real Estate Scrapling Crawler.
Delegates execution to Module.01_data_collection.scrapling_crawler.

Usage:
    python scrapling_crawler.py --samples 50 --upload-b2
"""

import importlib
from pathlib import Path
import sys

# Ensure repository root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Dynamically import module with numeric prefix folder
_crawler_mod = importlib.import_module("Module.01_data_collection.scrapling_crawler")
main = _crawler_mod.main

if __name__ == "__main__":
    sys.exit(main())

