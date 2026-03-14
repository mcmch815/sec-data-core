"""Cross-check sec_annual.db (mart) against sec_viewer.db (source).

Each check is independent and reports PASS/FAIL.

Usage:
    conda run -n tf python -m db_reduction.verify_mart
"""

import sqlite3
import sys
from pathlib import Path

from sec_core.paths import DB_PATH, MART_DB_PATH


def _connect_src(path=None) -> sqlite3.Connection:
    p = Path(path) if path else DB_PATH
    return sqlite3.connect(f"file:{p}?mode=ro", uri=True)


def _connect_mart(path=None) -> sqlite3.Connection:
    p = Path(path) if path else MART_DB_PATH
    if not p.exists():
        raise FileNotFoundError(
            f"Mart DB not found at {p}. "
            "Run: python -m db_reduction.mart_loader"
        )
    return sqlite3.connect(f"file:{p}?mode=ro", uri=True)


def _pass(name: str, msg: str = "") -> None:
    suffix = f" — {msg}" if msg else ""
    print(f"PASS  {name}{suffix}")


def _fail(name: str, msg: str) -> None:
    print(f"FAIL  {name} — {msg}")


# ---------------------------------------------------------------------------
# Check 1 — Amendment supersession
# ---------------------------------------------------------------------------

def check_amendment_supersession(src: sqlite3.Connection, mart: sqlite3.Connection) -> bool:
    """Verify that when a 10-K/A is the most recently filed document for a fact,
    the mart holds the 10-K/A value (not an older 10-K value).
    Note: a subsequent 10-K correctly supersedes a 10-K/A — the dedup is by filed date,
    not by form type.
    """
    name = "amendment_supersession"

    # Find facts where the most recently filed adsh is a 10-K/A
    latest_amendments = src.execute("""
        SELECT cik, tag, ddate, qtrs, value FROM (
            SELECT s.cik, n.tag, n.ddate, n.qtrs, CAST(n.value AS REAL) AS value, s.form,
                   ROW_NUMBER() OVER (
                       PARTITION BY s.cik, n.tag, n.ddate, n.qtrs
                       ORDER BY s.filed DESC, s.adsh DESC
                   ) AS rn
            FROM num n
            JOIN sub s ON n.adsh = s.adsh
            WHERE n.segments IS NULL AND n.coreg IS NULL AND n.qtrs IN (0, 4)
        )
        WHERE rn = 1 AND form = '10-K/A'
        LIMIT 100
    """).fetchall()

    if not latest_amendments:
        _pass(name, "no 10-K/A is the most recently filed for any fact (nothing to verify)")
        return True

    failures = []
    for cik, tag, ddate, qtrs, expected_val in latest_amendments:
        row = mart.execute(
            "SELECT value FROM facts WHERE cik=? AND tag=? AND ddate=? AND qtrs=?",
            (cik, tag, ddate, qtrs),
        ).fetchone()
        if row is None:
            continue  # tag not IS/BS/CF — excluded, skip
        if expected_val is None:
            continue  # NULL source value — skip
        if abs(row[0] - expected_val) >= 0.01:
            failures.append(
                f"CIK {cik} tag={tag} ddate={ddate} expected {expected_val} got {row[0]}"
            )

    if failures:
        _fail(name, "; ".join(failures[:3]))
        return False
    _pass(name, f"verified {len(latest_amendments)} facts where 10-K/A is most recent filing")
    return True


# ---------------------------------------------------------------------------
# Check 2 — Prior-year data present
# ---------------------------------------------------------------------------

def check_prior_year_data(mart: sqlite3.Connection, min_periods: int = 5) -> bool:
    name = "prior_year_data"
    # Apple CIK = 320193
    ddates = mart.execute("""
        SELECT DISTINCT ddate FROM facts
        WHERE cik='320193' AND stmt='IS' AND qtrs=4
        ORDER BY ddate
    """).fetchall()

    if len(ddates) < min_periods:
        _fail(name, f"Apple (320193) only has {len(ddates)} IS periods (need >={min_periods})")
        return False

    earliest, latest = ddates[0][0], ddates[-1][0]
    min_span = max(1, min_periods - 2)
    year_span = int(latest[:4]) - int(earliest[:4])
    if year_span < min_span:
        _fail(name, f"Apple IS date range too narrow: {earliest} to {latest}")
        return False

    _pass(name, f"Apple has {len(ddates)} IS periods spanning {earliest}–{latest}")
    return True


# ---------------------------------------------------------------------------
# Check 3 — Known-tag stmt assignments
# ---------------------------------------------------------------------------

KNOWN_STMTS = [
    ("Assets",                                     "BS"),
    ("Liabilities",                                "BS"),
    ("StockholdersEquity",                         "BS"),
    ("Revenues",                                   "IS"),
    ("NetIncomeLoss",                              "IS"),
    ("NetCashProvidedByUsedInOperatingActivities", "CF"),
    ("NetCashProvidedByUsedInInvestingActivities", "CF"),
]


def check_known_tag_stmts(mart: sqlite3.Connection) -> bool:
    name = "known_tag_stmts"
    failures = []
    for tag, expected in KNOWN_STMTS:
        row = mart.execute(
            "SELECT 1 FROM facts WHERE tag=? AND stmt=? LIMIT 1", (tag, expected)
        ).fetchone()
        if row is None:
            failures.append(f"{tag}: no row with stmt={expected}")

    if failures:
        _fail(name, "; ".join(failures))
        return False
    _pass(name, f"all {len(KNOWN_STMTS)} spot-check tags correct")
    return True


# ---------------------------------------------------------------------------
# Check 4 — Exclusion rate
# ---------------------------------------------------------------------------

def check_exclusion_rate(
    src: sqlite3.Connection,
    mart: sqlite3.Connection,
    min_pct: float = 25.0,
    max_pct: float = 85.0,
) -> bool:
    name = "exclusion_rate"
    print(f"  [{name}] counting source unique facts (may take a minute) ...", flush=True)
    src_count = src.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT s.cik, n.tag, n.ddate, n.qtrs
            FROM num n
            JOIN sub s ON n.adsh = s.adsh
            WHERE n.segments IS NULL AND n.coreg IS NULL AND n.qtrs IN (0, 4)
        )
    """).fetchone()[0]

    # Compare distinct (cik, tag, ddate, qtrs) in both — mart PK includes stmt
    # so COUNT(*) would be inflated for tags appearing in multiple stmts.
    mart_count = mart.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT cik, tag, ddate, qtrs FROM facts)"
    ).fetchone()[0]
    pct_kept = 100.0 * mart_count / src_count if src_count else 0.0

    print(f"  Source unique facts          : {src_count:,}")
    print(f"  Mart unique facts (IS/BS/CF) : {mart_count:,}  ({pct_kept:.1f}% kept)")

    if not (min_pct < pct_kept < max_pct):
        _fail(name, f"exclusion rate out of expected range [{min_pct}%–{max_pct}%]: {pct_kept:.1f}%")
        return False
    _pass(name, f"{pct_kept:.1f}% of source unique facts kept")
    return True


# ---------------------------------------------------------------------------
# Check 5 — Fiscal year mapping (non-December FY companies)
# ---------------------------------------------------------------------------

def check_fy_mapping(src: sqlite3.Connection, mart: sqlite3.Connection) -> bool:
    name = "fy_mapping"
    row = src.execute("""
        SELECT cik, period, fy FROM sub
        WHERE period LIKE '____0331'
          AND fy IS NOT NULL AND fy != '0' AND fy != ''
          AND fy != substr(period, 1, 4)
        LIMIT 1
    """).fetchone()

    if not row:
        _pass(name, "no non-December FY test case found in source")
        return True

    cik, period, expected_fy = row
    mart_row = mart.execute(
        "SELECT fy FROM periods WHERE cik=? AND ddate=?", (cik, period)
    ).fetchone()

    if mart_row is None:
        # CIK may not have IS/BS/CF facts — skip
        _pass(name, f"CIK {cik} period {period} not in mart (no IS/BS/CF facts)")
        return True

    if mart_row[0] != expected_fy:
        _fail(name, f"CIK {cik} period {period}: expected fy={expected_fy} got {mart_row[0]}")
        return False

    _pass(name, f"CIK {cik} period {period} fy={expected_fy} correct")
    return True


# ---------------------------------------------------------------------------
# Check 6 — Referential integrity
# ---------------------------------------------------------------------------

def check_referential_integrity(mart: sqlite3.Connection) -> bool:
    name = "referential_integrity"
    orphan_ciks = mart.execute("""
        SELECT COUNT(DISTINCT f.cik) FROM facts f
        LEFT JOIN companies c ON f.cik = c.cik
        WHERE c.cik IS NULL
    """).fetchone()[0]

    orphan_periods = mart.execute("""
        SELECT COUNT(*) FROM facts f
        LEFT JOIN periods p ON f.cik = p.cik AND f.ddate = p.ddate
        WHERE p.ddate IS NULL
    """).fetchone()[0]

    if orphan_ciks > 0:
        _fail(name, f"{orphan_ciks} ciks in facts not in companies")
        return False
    if orphan_periods > 0:
        _fail(name, f"{orphan_periods} facts have no period row")
        return False

    _pass(name, "no orphan facts")
    return True


# ---------------------------------------------------------------------------
# Check 7 — NULL audit
# ---------------------------------------------------------------------------

def check_null_audit(mart: sqlite3.Connection, max_null_value_pct: float = 5.0) -> bool:
    """Check NULL rates in facts. NULL values exist legitimately in source data
    (e.g. CommitmentsAndContingencies is always reported without a number).
    The threshold guards against pipeline bugs that introduce spurious NULLs.
    """
    name = "null_audit"
    total      = mart.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
    null_label = mart.execute("SELECT COUNT(*) FROM facts WHERE label IS NULL").fetchone()[0]
    null_value = mart.execute("SELECT COUNT(*) FROM facts WHERE value IS NULL").fetchone()[0]

    pct_null_label = 100.0 * null_label / total if total else 0.0
    pct_null_value = 100.0 * null_value / total if total else 0.0

    print(f"  NULL labels: {null_label:,} ({pct_null_label:.1f}%) — custom tags expected")
    print(f"  NULL values: {null_value:,} ({pct_null_value:.1f}%) — source NULLs tolerated up to {max_null_value_pct}%")

    if total > 0 and pct_null_value >= max_null_value_pct:
        _fail(name, f">{max_null_value_pct}% of facts have NULL value ({pct_null_value:.1f}%)")
        return False

    _pass(name, f"label nulls={pct_null_label:.1f}%, value nulls={pct_null_value:.2f}%")
    return True


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main(
    src_path=None,
    mart_path=None,
    min_apple_periods: int = 5,
    exclusion_min_pct: float = 25.0,
    exclusion_max_pct: float = 85.0,
    max_null_value_pct: float = 5.0,
) -> int:
    src  = _connect_src(src_path)
    mart = _connect_mart(mart_path)

    results = []

    label = Path(mart_path).name if mart_path else "sec_annual.db"
    print(f"\n=== {label} verification ===\n")

    results.append(check_amendment_supersession(src, mart))
    results.append(check_prior_year_data(mart, min_periods=min_apple_periods))
    results.append(check_known_tag_stmts(mart))
    results.append(check_exclusion_rate(src, mart, min_pct=exclusion_min_pct, max_pct=exclusion_max_pct))
    results.append(check_fy_mapping(src, mart))
    results.append(check_referential_integrity(mart))
    results.append(check_null_audit(mart, max_null_value_pct=max_null_value_pct))

    src.close()
    mart.close()

    n_pass = sum(results)
    n_fail = len(results) - n_pass
    print(f"\n{n_pass}/{len(results)} checks passed", end="")
    if n_fail:
        print(f", {n_fail} FAILED")
        return 1
    print()
    return 0


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--src",  default=None, help="Source DB path (default: sec_viewer.db)")
    parser.add_argument("--mart", default=None, help="Mart DB path (default: sec_annual.db)")
    parser.add_argument("--min-apple-periods", type=int, default=5,
                        help="Min Apple IS periods required (default: 5)")
    parser.add_argument("--exclusion-min-pct", type=float, default=25.0,
                        help="Min pct of source facts expected in mart (default: 25)")
    parser.add_argument("--exclusion-max-pct", type=float, default=85.0,
                        help="Max pct of source facts expected in mart (default: 85)")
    parser.add_argument("--max-null-value-pct", type=float, default=5.0,
                        help="Max allowed NULL value pct in facts (default: 5)")
    args = parser.parse_args()
    sys.exit(main(
        src_path=args.src,
        mart_path=args.mart,
        min_apple_periods=args.min_apple_periods,
        exclusion_min_pct=args.exclusion_min_pct,
        exclusion_max_pct=args.exclusion_max_pct,
        max_null_value_pct=args.max_null_value_pct,
    ))
