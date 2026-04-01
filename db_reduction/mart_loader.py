"""Build sec_annual.db — deduplicated annual financial mart.

Reads from sec_viewer.db (raw store) and produces a clean, narrow mart with one
canonical value per (company, tag, stmt, fiscal-date). Each fact carries the stmt
from the pre table for the most recently filed adsh — so a tag that appears in
both IS and CF gets two rows (one per stmt), same value.

Tags that have no IS/BS/CF entry in pre for their winning adsh are excluded.

Usage:
    conda run -n tf python -m db_reduction.mart_loader           # build (skip if exists)
    conda run -n tf python -m db_reduction.mart_loader --force   # rebuild from scratch
"""

import sqlite3
import time
from pathlib import Path

from sec_core.paths import DB_PATH, MART_DB_PATH

BATCH_SIZE = 50_000


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_DDL = """
CREATE TABLE sic_codes (
    sic           TEXT PRIMARY KEY,
    description   TEXT,
    office        TEXT,
    division      TEXT,
    division_name TEXT
);

CREATE TABLE companies (
    cik   TEXT PRIMARY KEY,
    name  TEXT,
    sic   TEXT REFERENCES sic_codes(sic)
);

CREATE TABLE periods (
    cik   TEXT NOT NULL,
    ddate TEXT NOT NULL,
    fy    TEXT,
    PRIMARY KEY (cik, ddate)
);

CREATE TABLE facts (
    cik      TEXT    NOT NULL,
    tag      TEXT    NOT NULL,
    stmt     TEXT    NOT NULL,
    ddate    TEXT    NOT NULL,
    qtrs     INTEGER NOT NULL,
    report   INTEGER,           -- presentation report number (for ordering)
    line     INTEGER,           -- presentation line number (for ordering)
    label    TEXT,              -- tlabel from tag (taxonomy label)
    plabel   TEXT,              -- presentation label from pre (filing-specific)
    negating TEXT,              -- '1' if value should be sign-flipped for display
    inpth    TEXT,              -- '1' if this line is in-parenthesis (subtotal/header)
    uom      TEXT,
    value    REAL,
    PRIMARY KEY (cik, tag, stmt, ddate, qtrs)
);
"""

_INDEX_DDL = """
CREATE INDEX idx_facts_cik_stmt_ddate ON facts(cik, stmt, ddate);
CREATE INDEX idx_facts_cik_ddate      ON facts(cik, ddate);
CREATE INDEX idx_facts_tag            ON facts(tag);
CREATE INDEX idx_periods_cik          ON periods(cik);
"""


def _create_schema(mart_conn: sqlite3.Connection) -> None:
    mart_conn.executescript(_DDL)
    mart_conn.commit()


def _create_mart_indexes(mart_conn: sqlite3.Connection) -> None:
    mart_conn.executescript(_INDEX_DDL)
    mart_conn.commit()


# ---------------------------------------------------------------------------
# Canonical label mapping: tag -> tlabel
# ---------------------------------------------------------------------------

def _build_canonical_label(src_conn: sqlite3.Connection) -> dict[str, str]:
    """Return {tag: tlabel} using the most recent taxonomy version."""
    rows = src_conn.execute("""
        SELECT tag, tlabel FROM (
            SELECT tag, tlabel,
                   ROW_NUMBER() OVER (
                       PARTITION BY tag
                       ORDER BY version DESC, quarter DESC
                   ) AS rn
            FROM tag
            WHERE tlabel IS NOT NULL
        )
        WHERE rn = 1
    """).fetchall()
    return dict(rows)


# ---------------------------------------------------------------------------
# Build facts
# ---------------------------------------------------------------------------

_PRE_SUBQUERY = """
    SELECT adsh, tag, stmt, report, line, plabel, negating, inpth FROM (
        SELECT adsh, tag, stmt, report, line, plabel, negating, inpth,
               ROW_NUMBER() OVER (
                   PARTITION BY adsh, tag, stmt
                   ORDER BY report, line
               ) AS rn
        FROM pre WHERE stmt IN ('IS', 'BS', 'CF')
    ) WHERE rn = 1
"""


def _build_facts(
    src_conn: sqlite3.Connection,
    mart_conn: sqlite3.Connection,
    tag_label: dict[str, str],
) -> int:
    """Stream deduped facts from src → mart. Returns row count inserted.

    Algorithm:
      1. Canonical adsh: pick the most recently filed adsh per (cik, ddate) — one
         filing is authoritative for each fiscal period date. This prevents tag mixing
         when a company renames XBRL tags between filings. Only fiscal year-end dates
         are included (ddate must appear as sub.period for that CIK), which excludes
         prior-year carryforward data, interim BS snapshots, and off-cycle disclosures.
      2. Primary pass: all null-segment num facts from that canonical adsh, joined to
         pre for IS/BS/CF statement membership and display ordering.
      3. Segmented fallback: for tags with no null-segment row, sum all eq_count=1
         (single-dimension) segmented rows per (cik, tag, ddate, qtrs, stmt). This
         captures values that companies only filed in segmented form (e.g. a related-
         party subtotal). INSERT OR IGNORE ensures primary-pass rows are never replaced.
      4. A tag appearing in both IS and CF in pre produces two rows — one per stmt.
      5. Tags with no IS/BS/CF pre entry for their winning adsh are excluded.
    """
    # Temp table of known fiscal year-end dates per CIK.
    src_conn.execute(
        "CREATE TEMP TABLE _annual_periods (cik TEXT, period TEXT, "
        "PRIMARY KEY (cik, period))"
    )
    src_conn.execute(
        "INSERT OR IGNORE INTO _annual_periods SELECT cik, period FROM sub"
    )

    # Canonical adsh per (cik, ddate) — materialised once, used by both passes.
    src_conn.execute(
        "CREATE TEMP TABLE _canonical_adsh "
        "(cik TEXT, ddate TEXT, adsh TEXT, PRIMARY KEY (cik, ddate))"
    )
    src_conn.execute("""
        INSERT INTO _canonical_adsh
        SELECT cik, ddate, adsh FROM (
            SELECT s.cik, n.ddate, n.adsh,
                   ROW_NUMBER() OVER (
                       PARTITION BY s.cik, n.ddate
                       ORDER BY s.filed DESC, s.adsh DESC
                   ) AS rn
            FROM num n
            JOIN sub s ON n.adsh = s.adsh
            WHERE n.segments IS NULL AND n.coreg IS NULL
              AND n.qtrs IN (0, 4)
              AND EXISTS (
                  SELECT 1 FROM temp._annual_periods ap
                  WHERE ap.cik = s.cik AND ap.period = n.ddate
              )
        ) WHERE rn = 1
    """)
    src_conn.commit()

    total = 0
    t0 = time.time()

    def _insert_batch(cursor) -> int:
        count = 0
        while True:
            batch = cursor.fetchmany(BATCH_SIZE)
            if not batch:
                break
            enriched = [
                (cik, tag, stmt, ddate, qtrs, report, line,
                 tag_label.get(tag), plabel, negating, inpth, uom, value)
                for cik, tag, ddate, qtrs, uom, value,
                    stmt, report, line, plabel, negating, inpth in batch
            ]
            mart_conn.executemany(
                "INSERT OR IGNORE INTO facts"
                "(cik, tag, stmt, ddate, qtrs, report, line,"
                " label, plabel, negating, inpth, uom, value) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                enriched,
            )
            mart_conn.commit()
            count += len(batch)
            elapsed = time.time() - t0
            print(f"  facts: {total + count:,} rows  ({elapsed:.0f}s)", end="\r", flush=True)
        return count

    # --- Primary pass: null-segment rows ---
    cursor = src_conn.execute(f"""
        SELECT ca.cik, n.tag, n.ddate, n.qtrs, n.uom,
               COALESCE(CAST(n.value AS REAL), 0.0) AS value,
               p.stmt, p.report, p.line, p.plabel, p.negating, p.inpth
        FROM temp._canonical_adsh ca
        JOIN num n ON n.adsh = ca.adsh AND n.ddate = ca.ddate
                   AND n.segments IS NULL AND n.coreg IS NULL AND n.qtrs IN (0, 4)
        JOIN ({_PRE_SUBQUERY}) p ON p.adsh = ca.adsh AND p.tag = n.tag
    """)
    total += _insert_batch(cursor)

    # --- Segmented fallback: eq_count=1 rows ---
    # Sum all eq_count=1 rows, excluding ConsolidationItems= rows (those are subtotals
    # of the eq_count=2 business segment breakdown, not independent peer values).
    # Fallback: if all eq_count=1 rows for a group are ConsolidationItems=, include
    # them anyway (nothing else available).
    # INSERT OR IGNORE skips any tag already inserted by the primary pass.
    seg_cursor = src_conn.execute(f"""
        WITH seg1 AS (
            SELECT ca.cik, n.tag, n.ddate, n.qtrs, n.uom,
                   COALESCE(CAST(n.value AS REAL), 0.0) AS value,
                   n.segments,
                   p.stmt, p.report, p.line, p.plabel, p.negating, p.inpth,
                   SUM(CASE WHEN n.segments NOT LIKE 'ConsolidationItems=%' THEN 1 ELSE 0 END)
                       OVER (PARTITION BY ca.cik, n.tag, n.ddate, n.qtrs, p.stmt) AS non_consol_count
            FROM temp._canonical_adsh ca
            JOIN num n ON n.adsh = ca.adsh AND n.ddate = ca.ddate
                       AND n.coreg IS NULL AND n.qtrs IN (0, 4)
                       AND n.segments IS NOT NULL
                       AND (length(n.segments) - length(replace(n.segments, '=', ''))) = 1
            JOIN ({_PRE_SUBQUERY}) p ON p.adsh = ca.adsh AND p.tag = n.tag
        )
        SELECT cik, tag, ddate, qtrs, uom, SUM(value) AS value,
               stmt, report, line, plabel, negating, inpth
        FROM seg1
        WHERE (non_consol_count > 0 AND segments NOT LIKE 'ConsolidationItems=%')
           OR  non_consol_count = 0
        GROUP BY cik, tag, ddate, qtrs, uom, stmt, report, line, plabel, negating, inpth
    """)
    seg_added = _insert_batch(seg_cursor)
    total += seg_added

    print(f"  facts: {total:,} rows  ({time.time() - t0:.0f}s)  DONE  "
          f"(+{seg_added:,} from segmented fallback)        ")
    return total


# ---------------------------------------------------------------------------
# Build sic_codes
# ---------------------------------------------------------------------------

def _build_sic_codes(mart_conn: sqlite3.Connection) -> int:
    from sec_core.sic import SIC_CODES
    rows = [
        (sic, info["description"], info["office"], info["division"], info["division_name"])
        for sic, info in SIC_CODES.items()
    ]
    mart_conn.executemany(
        "INSERT INTO sic_codes VALUES (?, ?, ?, ?, ?)", rows
    )
    mart_conn.commit()
    print(f"  sic_codes: {len(rows):,}")
    return len(rows)


# ---------------------------------------------------------------------------
# Build companies
# ---------------------------------------------------------------------------

def _build_companies(src_conn: sqlite3.Connection, mart_conn: sqlite3.Connection) -> int:
    rows = src_conn.execute("""
        SELECT cik, name, sic FROM (
            SELECT cik, name, sic,
                   ROW_NUMBER() OVER (
                       PARTITION BY cik
                       ORDER BY filed DESC, adsh DESC
                   ) AS rn
            FROM sub
        )
        WHERE rn = 1
    """).fetchall()

    mart_conn.executemany("INSERT INTO companies VALUES (?, ?, ?)", rows)
    mart_conn.commit()
    print(f"  companies: {len(rows):,}")
    return len(rows)


# ---------------------------------------------------------------------------
# Build periods
# ---------------------------------------------------------------------------

def _build_periods(src_conn: sqlite3.Connection, mart_conn: sqlite3.Connection) -> int:
    """Build periods table with fy from sub where possible, else substr(ddate,1,4).

    Type 1 collision fix: when two ddates for the same (cik, fy) both have a real
    sub.fy match (not a fallback), at least one sub.fy label is wrong. Reassign any
    row where sub.fy disagrees with substr(ddate,1,4) to use substr instead.
    This handles filers that submitted the wrong fy label (e.g. fy='2024' for a
    period ending 20250930) without touching companies that legitimately use
    start-year naming (where only one side of a collision has a real sub match).
    """
    from collections import defaultdict

    pairs = mart_conn.execute(
        "SELECT DISTINCT cik, ddate FROM facts"
    ).fetchall()

    src_conn.execute(
        "CREATE TEMP TABLE _fact_periods (cik TEXT, ddate TEXT)"
    )
    src_conn.executemany("INSERT INTO _fact_periods VALUES (?, ?)", pairs)
    src_conn.commit()

    # Fetch fy alongside a flag: has_sub=1 if fy came from a real sub row,
    # has_sub=0 if it fell back to substr(ddate,1,4).
    rows = src_conn.execute("""
        SELECT fp.cik, fp.ddate,
               COALESCE(
                   NULLIF(s.fy, '0'),
                   NULLIF(s.fy, ''),
                   substr(fp.ddate, 1, 4)
               ) AS fy,
               CASE WHEN s.fy IS NOT NULL
                         AND s.fy != '0'
                         AND s.fy != '' THEN 1 ELSE 0 END AS has_sub
        FROM _fact_periods fp
        LEFT JOIN (
            SELECT cik, period, fy,
                   ROW_NUMBER() OVER (
                       PARTITION BY cik, period
                       ORDER BY filed DESC, adsh DESC
                   ) AS rn
            FROM sub
        ) s ON s.cik = fp.cik AND s.period = fp.ddate AND s.rn = 1
    """).fetchall()

    # Detect Type 1 collisions: (cik, fy) groups where every member matched a
    # real sub row. In those groups, reassign rows where fy != ddate[:4].
    groups: dict = defaultdict(list)
    for cik, ddate, fy, has_sub in rows:
        groups[(cik, fy)].append((ddate, has_sub))

    type1_collisions = {
        key for key, members in groups.items()
        if len(members) > 1 and all(has_sub for _, has_sub in members)
    }

    cleaned = []
    for cik, ddate, fy, has_sub in rows:
        if (cik, fy) in type1_collisions and fy != ddate[:4]:
            fy = ddate[:4]
        cleaned.append((cik, ddate, fy if fy else ddate[:4]))

    if type1_collisions:
        print(f"  Type 1 fy collisions corrected: {len(type1_collisions)}")

    mart_conn.executemany("INSERT INTO periods VALUES (?, ?, ?)", cleaned)
    mart_conn.commit()
    print(f"  periods: {len(cleaned):,}")
    return len(cleaned)


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

def build_annual_mart(
    force: bool = False,
    src_path: Path | None = None,
    mart_path: Path | None = None,
) -> Path:
    """Build sec_annual.db from sec_viewer.db.

    Args:
        force:     If True, delete and rebuild even if the file already exists.
        src_path:  Override source DB path (default: DB_PATH from paths.py).
        mart_path: Override mart DB path (default: MART_DB_PATH from paths.py).

    Returns:
        Path to the built mart database.
    """
    _src  = Path(src_path)  if src_path  else DB_PATH
    _mart = Path(mart_path) if mart_path else MART_DB_PATH

    if _mart.exists():
        if force:
            print(f"Removing existing {_mart} ...")
            _mart.unlink()
        else:
            print(f"{_mart.name} already exists at {_mart}. Use --force to rebuild.")
            return _mart

    if not _src.exists():
        raise FileNotFoundError(f"Source database not found: {_src}")

    print(f"Building {_mart} from {_src} ...")
    t_start = time.time()

    src_conn = sqlite3.connect(f"file:{_src}?mode=ro", uri=True)
    mart_conn = sqlite3.connect(str(_mart))

    mart_conn.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA cache_size=-262144;
        PRAGMA temp_store=FILE;
    """)
    src_conn.executescript("""
        PRAGMA cache_size=-262144;
        PRAGMA temp_store=FILE;
    """)

    try:
        print("Building schema ...")
        _create_schema(mart_conn)

        print("Building canonical label map ...")
        tag_label = _build_canonical_label(src_conn)
        print(f"  {len(tag_label):,} labels found")

        print("Building sic_codes ...")
        _build_sic_codes(mart_conn)

        print("Building companies ...")
        _build_companies(src_conn, mart_conn)

        print("Building facts (dedup → per-filing pre join) ...")
        n_facts = _build_facts(src_conn, mart_conn, tag_label)

        print("Building periods ...")
        _build_periods(src_conn, mart_conn)

        print("Creating indexes ...")
        _create_mart_indexes(mart_conn)

    except Exception:
        src_conn.close()
        mart_conn.close()
        if _mart.exists():
            _mart.unlink()
        raise

    src_conn.close()
    mart_conn.close()

    elapsed = time.time() - t_start
    size_mb = _mart.stat().st_size / 1_048_576
    print(f"\nDone in {elapsed:.0f}s. {_mart.name}: {size_mb:.0f} MB, {n_facts:,} facts.")
    return _mart


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    force = "--force" in sys.argv
    build_annual_mart(force=force)
