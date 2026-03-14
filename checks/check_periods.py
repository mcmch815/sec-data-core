"""Check that each CIK has exactly one period end per fiscal year.

A violation means two different year-end dates map to the same fy label for
the same company — e.g. a company appearing with both ddate=20240831 and
ddate=20240930 under fy='2024'. This should not happen in a clean annual mart.

Usage:
    conda run -n tf python -m checks.check_periods
    conda run -n tf python -m checks.check_periods --mart test_db/test_annual.db
"""

import sys
import argparse
import sqlite3
from pathlib import Path

from sec_core.paths import MART_DB_PATH


def run(mart_path: Path) -> int:
    if not mart_path.exists():
        print(f"ERROR: mart DB not found at {mart_path}")
        return 1

    conn = sqlite3.connect(f"file:{mart_path}?mode=ro", uri=True)

    # Find any (cik, fy) with more than one distinct ddate
    violations = conn.execute("""
        SELECT p.cik, c.name, p.fy, COUNT(DISTINCT p.ddate) AS n_dates,
               GROUP_CONCAT(p.ddate, ', ') AS dates
        FROM periods p
        JOIN companies c ON p.cik = c.cik
        GROUP BY p.cik, p.fy
        HAVING n_dates > 1
        ORDER BY p.cik, p.fy
    """).fetchall()

    conn.close()

    if not violations:
        print("PASS  one_period_end_per_fy — all CIKs have exactly one period end per fiscal year")
        return 0

    print(f"FAIL  one_period_end_per_fy — {len(violations)} violation(s):\n")
    for cik, name, fy, n_dates, dates in violations:
        print(f"  CIK {cik} ({name})  fy={fy}  →  {n_dates} dates: {dates}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mart", default=None, help="Path to mart DB (default: sec_annual.db)")
    args = parser.parse_args()

    mart_path = Path(args.mart) if args.mart else MART_DB_PATH
    return run(mart_path)


if __name__ == "__main__":
    sys.exit(main())
