"""Resolve paths to shared SEC data files.

By default, data lives in the sec-data-core root directory (parent of this package).
Override with the SEC_DATA_ROOT environment variable to point elsewhere.

    export SEC_DATA_ROOT=/path/to/sec-data-core
"""

import os
from pathlib import Path

# Default: sec-data-core/ root (two levels up from this file: sec_core/paths.py)
_DEFAULT_ROOT = Path(__file__).parent.parent

ROOT = Path(os.environ.get("SEC_DATA_ROOT", _DEFAULT_ROOT))

DB_PATH      = ROOT / "sec_viewer.db"
MART_DB_PATH = ROOT / "sec_annual.db"
DATA_DIR   = ROOT / "Data"
ODS_PATH   = ROOT / "2025_GAAP_Taxonomy_Small.ods"
CALC_PARQUET = ROOT / "2025_GAAP_Taxonomy_Small_Calculation.parquet"
PRES_PARQUET = ROOT / "2025_GAAP_Taxonomy_Small_Presentation.parquet"
