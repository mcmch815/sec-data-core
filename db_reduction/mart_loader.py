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
CREATE TABLE companies (
    cik   TEXT PRIMARY KEY,
    name  TEXT
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

def _build_facts(
    src_conn: sqlite3.Connection,
    mart_conn: sqlite3.Connection,
    tag_label: dict[str, str],
) -> int:
    """Stream deduped facts from src → mart. Returns row count inserted.

    Algorithm:
      1. Dedup: pick the most recently filed adsh per (cik, tag, ddate, qtrs).
      2. Join to pre on (adsh, tag), picking the first pre row per (adsh, tag, stmt)
         ordered by (report, line). This gives the correct display ordering and
         presentation label (plabel) from the winning filing.
      3. A tag appearing in both IS and CF in pre produces two rows — one per stmt.
      4. Tags with no IS/BS/CF pre entry for their winning adsh are excluded.
    """
    # Temp table of known fiscal year-end dates per CIK — every date that appears
    # as sub.period in our source data. Facts are only included if their ddate is
    # in this set, ensuring we keep only complete standalone filing periods and
    # excluding prior-year carryforward data and supplementary disclosures.
    src_conn.execute(
        "CREATE TEMP TABLE _annual_periods (cik TEXT, period TEXT, "
        "PRIMARY KEY (cik, period))"
    )
    src_conn.execute(
        "INSERT OR IGNORE INTO _annual_periods SELECT cik, period FROM sub"
    )
    src_conn.commit()

    cursor = src_conn.execute("""
        SELECT d.cik, d.tag, d.ddate, d.qtrs, d.uom, d.value,
               p.stmt, p.report, p.line, p.plabel, p.negating, p.inpth
        FROM (
            -- Step 1: dedup — best adsh per (cik, tag, ddate, qtrs).
            -- Only include facts where ddate is a known fiscal year-end date for
            -- that CIK (i.e. ddate appears as sub.period somewhere in our data).
            -- This excludes prior-year carryforward data, interim BS snapshots,
            -- and supplementary regulatory disclosures at off-cycle dates.
            SELECT cik, tag, ddate, qtrs, uom, value, adsh FROM (
                SELECT s.cik, n.tag, n.ddate, n.qtrs, n.uom,
                       CAST(n.value AS REAL) AS value, n.adsh,
                       ROW_NUMBER() OVER (
                           PARTITION BY s.cik, n.tag, n.ddate, n.qtrs
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
        ) d
        JOIN (
            -- Step 2: first pre row per (adsh, tag, stmt) by (report, line)
            SELECT adsh, tag, stmt, report, line, plabel, negating, inpth FROM (
                SELECT adsh, tag, stmt, report, line, plabel, negating, inpth,
                       ROW_NUMBER() OVER (
                           PARTITION BY adsh, tag, stmt
                           ORDER BY report, line
                       ) AS rn
                FROM pre
                WHERE stmt IN ('IS', 'BS', 'CF')
            ) WHERE rn = 1
        ) p ON p.adsh = d.adsh AND p.tag = d.tag
    """)

    total = 0
    t0 = time.time()

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
        total += len(batch)

        elapsed = time.time() - t0
        print(f"  facts: {total:,} rows  ({elapsed:.0f}s)", end="\r", flush=True)

    print(f"  facts: {total:,} rows  ({time.time() - t0:.0f}s)  DONE        ")
    return total


# ---------------------------------------------------------------------------
# Build companies
# ---------------------------------------------------------------------------

def _build_companies(src_conn: sqlite3.Connection, mart_conn: sqlite3.Connection) -> int:
    rows = src_conn.execute("""
        SELECT cik, name FROM (
            SELECT cik, name,
                   ROW_NUMBER() OVER (
                       PARTITION BY cik
                       ORDER BY filed DESC, adsh DESC
                   ) AS rn
            FROM sub
        )
        WHERE rn = 1
    """).fetchall()

    mart_conn.executemany("INSERT INTO companies VALUES (?, ?)", rows)
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
