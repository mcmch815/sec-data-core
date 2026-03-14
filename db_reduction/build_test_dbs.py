"""Build test databases using only 2024q4 and 2025q4 data.

Produces:
    test_db/test_viewer.db  — raw store (2 quarters only)
    test_db/test_annual.db  — deduplicated annual mart from the above

Usage:
    conda run -n tf python -m db_reduction.build_test_dbs           # build (skip if exists)
    conda run -n tf python -m db_reduction.build_test_dbs --force   # rebuild from scratch
"""

import sqlite3
import sys
import os
from pathlib import Path

from sec_core.paths import DATA_DIR, ROOT
from sec_core.db_loader import (
    load_sub, load_tag, load_num, load_pre, create_indexes, INDEXES
)
from db_reduction.mart_loader import build_annual_mart
from db_reduction.verify_mart import main as verify_main

TEST_QUARTERS = ["2024q4", "2025q4"]

TEST_DIR        = ROOT / "test_db"
TEST_VIEWER_DB  = TEST_DIR / "test_viewer.db"
TEST_ANNUAL_DB  = TEST_DIR / "test_annual.db"

# With sub.period filtering, only actual filing dates are included — Apple has 2 (FY24+FY25).
MIN_APPLE_PERIODS = 2


def build_test_viewer(force: bool = False) -> Path:
    if TEST_VIEWER_DB.exists():
        if force:
            print(f"Removing existing {TEST_VIEWER_DB} ...")
            TEST_VIEWER_DB.unlink()
        else:
            print(f"test_viewer.db already exists, skipping. Use --force to rebuild.")
            return TEST_VIEWER_DB

    TEST_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(TEST_VIEWER_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    for quarter in TEST_QUARTERS:
        qdir = DATA_DIR / quarter
        if not qdir.is_dir():
            print(f"WARNING: {qdir} not found, skipping")
            continue

        print(f"\nLoading {quarter} ...")
        sub_path = qdir / "sub.txt"
        tag_path = qdir / "tag.txt"
        num_path = qdir / "num.txt"
        pre_path = qdir / "pre.txt"

        if not sub_path.exists():
            print(f"  WARNING: sub.txt not found, skipping quarter")
            continue
        valid_adsh = load_sub(conn, str(sub_path), quarter)

        if tag_path.exists():
            load_tag(conn, str(tag_path), quarter)
        if num_path.exists():
            load_num(conn, str(num_path), quarter, valid_adsh)
        if pre_path.exists():
            load_pre(conn, str(pre_path), quarter, valid_adsh)

    create_indexes(conn)
    conn.close()

    size_mb = TEST_VIEWER_DB.stat().st_size / 1_048_576
    print(f"\ntest_viewer.db: {size_mb:.0f} MB")
    return TEST_VIEWER_DB


def main() -> int:
    force = "--force" in sys.argv

    print("=== Step 1: Build test_viewer.db ===")
    build_test_viewer(force=force)

    print("\n=== Step 2: Build test_annual.db ===")
    build_annual_mart(
        force=force,
        src_path=TEST_VIEWER_DB,
        mart_path=TEST_ANNUAL_DB,
    )

    print("\n=== Step 3: Verify test_annual.db ===")
    rc = verify_main(
        src_path=str(TEST_VIEWER_DB),
        mart_path=str(TEST_ANNUAL_DB),
        min_apple_periods=MIN_APPLE_PERIODS,
        # With only 2 quarters, the pre table has fewer EQ/CI/UN entries,
        # so the IS/BS/CF filter excludes proportionally less.
        exclusion_min_pct=10.0,
        exclusion_max_pct=99.0,
        # NULL values exist legitimately in source (e.g. CommitmentsAndContingencies).
        # With a small dataset they're a higher fraction; 5% is the threshold.
        max_null_value_pct=5.0,
    )

    print("\n=== Size comparison ===")
    for p in (TEST_VIEWER_DB, TEST_ANNUAL_DB):
        if p.exists():
            mb = p.stat().st_size / 1_048_576
            print(f"  {p.name}: {mb:.1f} MB")

    return rc


if __name__ == "__main__":
    sys.exit(main())
