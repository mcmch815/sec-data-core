"""Load SEC Financial Statement Data Set files into SQLite for fast querying.

Only 10-K and 10-K/A filings are loaded. All other form types are excluded.
"""

import os
import sqlite3
import pandas as pd
import glob

from sec_core.paths import DATA_DIR, DB_PATH

ANNUAL_FORMS = {'10-K', '10-K/A'}

SUB_DTYPES = {'cik': str, 'sic': str, 'ein': str,
              'fye': str, 'period': str, 'fy': str, 'filed': str,
              'prevrpt': str, 'detail': str, 'nciks': str, 'zipba': str, 'zipma': str}
TAG_DTYPES = {'custom': str, 'abstract': str}
NUM_DTYPES = {'ddate': str, 'qtrs': 'Int64'}
PRE_DTYPES = {'line': int, 'inpth': str, 'negating': str}

INDEXES = [
    ('idx_sub_cik', 'sub', 'cik'),
    ('idx_sub_name', 'sub', 'name'),
    ('idx_num_adsh', 'num', 'adsh'),
    ('idx_num_adsh_tag', 'num', 'adsh, tag, version'),
    ('idx_num_adsh_ddate', 'num', 'adsh, ddate, qtrs'),
    ('idx_pre_adsh_stmt', 'pre', 'adsh, stmt'),
    ('idx_pre_adsh_report', 'pre', 'adsh, report, line'),
]


def find_quarter_dirs():
    """Find all quarter directories in Data/."""
    dirs = sorted(glob.glob(os.path.join(DATA_DIR, '20*')))
    return [(os.path.basename(d), d) for d in dirs if os.path.isdir(d)]


def load_sub(conn, file_path, quarter):
    """Load sub.txt, filtering to annual forms only. Returns set of valid adsh."""
    print(f"  Loading sub.txt ...", flush=True)
    try:
        df = pd.read_csv(file_path, sep='\t', dtype=SUB_DTYPES, low_memory=False)
    except pd.errors.EmptyDataError:
        print(f"    (empty file, skipped)", flush=True)
        return set()
    if 'form' not in df.columns or 'adsh' not in df.columns:
        print(f"    (missing required columns, skipped)", flush=True)
        return set()
    before = len(df)
    df = df[df['form'].isin(ANNUAL_FORMS)]
    df['quarter'] = quarter
    df.to_sql('sub', conn, if_exists='append', index=False)
    print(f"    {len(df)}/{before} rows kept (10-K/10-K/A only)", flush=True)
    return set(df['adsh'])


def load_tag(conn, file_path, quarter):
    """Load tag.txt in full (reference table, not per-filing)."""
    print(f"  Loading tag.txt ...", flush=True)
    try:
        df = pd.read_csv(file_path, sep='\t', dtype=TAG_DTYPES, low_memory=False)
    except pd.errors.EmptyDataError:
        print(f"    (empty file, skipped)", flush=True)
        return
    df['quarter'] = quarter
    df.to_sql('tag', conn, if_exists='append', index=False)
    print(f"    {len(df)} rows", flush=True)


def load_num(conn, file_path, quarter, valid_adsh, chunksize=500_000):
    """Load num.txt in chunks, filtering to valid adsh only."""
    print(f"  Loading num.txt ...", flush=True)
    total_kept = 0
    try:
        reader = pd.read_csv(file_path, sep='\t',
                             dtype=NUM_DTYPES, chunksize=chunksize,
                             low_memory=False)
    except pd.errors.EmptyDataError:
        print(f"    (empty file, skipped)", flush=True)
        return
    try:
        for i, chunk in enumerate(reader):
            if 'adsh' not in chunk.columns:
                print(f"    Chunk {i+1}: missing 'adsh' column, skipped", flush=True)
                continue
            chunk = chunk[chunk['adsh'].isin(valid_adsh)].copy()
            chunk['quarter'] = quarter
            chunk.to_sql('num', conn, if_exists='append', index=False)
            total_kept += len(chunk)
            print(f"    Chunk {i+1}: {len(chunk)} rows kept", flush=True)
    except pd.errors.EmptyDataError:
        pass
    print(f"    Total num rows: {total_kept}", flush=True)


def load_pre(conn, file_path, quarter, valid_adsh):
    """Load pre.txt, filtering to valid adsh only."""
    print(f"  Loading pre.txt ...", flush=True)
    try:
        df = pd.read_csv(file_path, sep='\t', dtype=PRE_DTYPES, low_memory=False)
    except pd.errors.EmptyDataError:
        print(f"    (empty file, skipped)", flush=True)
        return
    if 'adsh' not in df.columns:
        print(f"    (missing 'adsh' column, skipped)", flush=True)
        return
    before = len(df)
    df = df[df['adsh'].isin(valid_adsh)]
    df['quarter'] = quarter
    df.to_sql('pre', conn, if_exists='append', index=False)
    print(f"    {len(df)}/{before} rows kept", flush=True)


def create_indexes(conn):
    """Create indexes for fast querying."""
    print("Creating indexes...", flush=True)
    for idx_name, table, cols in INDEXES:
        sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} ({cols})"
        conn.execute(sql)
    conn.commit()


def load_all(force=False):
    """Load all quarter data into SQLite (10-K/10-K/A filings only)."""
    if os.path.exists(DB_PATH) and not force:
        print(f"Database already exists at {DB_PATH}. Use force=True to reload.")
        return DB_PATH

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    quarters = find_quarter_dirs()
    if not quarters:
        raise FileNotFoundError(f"No quarter directories found in {DATA_DIR}")

    print(f"Found quarters: {[q[0] for q in quarters]}")
    print(f"Filtering to forms: {ANNUAL_FORMS}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    for quarter_name, quarter_path in quarters:
        print(f"\nLoading {quarter_name}...")

        sub_path = os.path.join(quarter_path, 'sub.txt')
        tag_path = os.path.join(quarter_path, 'tag.txt')
        num_path = os.path.join(quarter_path, 'num.txt')
        pre_path = os.path.join(quarter_path, 'pre.txt')

        if not os.path.exists(sub_path):
            print(f"  WARNING: sub.txt not found, skipping quarter")
            continue
        valid_adsh = load_sub(conn, sub_path, quarter_name)

        if os.path.exists(tag_path):
            load_tag(conn, tag_path, quarter_name)
        else:
            print(f"  WARNING: tag.txt not found")

        if os.path.exists(num_path):
            load_num(conn, num_path, quarter_name, valid_adsh)
        else:
            print(f"  WARNING: num.txt not found")

        if os.path.exists(pre_path):
            load_pre(conn, pre_path, quarter_name, valid_adsh)
        else:
            print(f"  WARNING: pre.txt not found")

    create_indexes(conn)
    conn.close()
    print(f"\nDatabase created at {DB_PATH}")
    return DB_PATH


def reload_quarter(quarter_name):
    """Delete and reload a single quarter's data without rebuilding the whole DB."""
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}. Run load_all() first.")
        return

    quarter_path = os.path.join(DATA_DIR, quarter_name)
    if not os.path.isdir(quarter_path):
        raise FileNotFoundError(f"Quarter directory not found: {quarter_path}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    print(f"Deleting existing rows for quarter '{quarter_name}'...")
    for table in ('sub', 'tag', 'num', 'pre'):
        cur = conn.execute(f"DELETE FROM {table} WHERE quarter = ?", (quarter_name,))
        print(f"  {table}: deleted {cur.rowcount} rows")
    conn.commit()

    print(f"\nReloading {quarter_name}...")
    sub_path = os.path.join(quarter_path, 'sub.txt')
    tag_path = os.path.join(quarter_path, 'tag.txt')
    num_path = os.path.join(quarter_path, 'num.txt')
    pre_path = os.path.join(quarter_path, 'pre.txt')

    valid_adsh = load_sub(conn, sub_path, quarter_name)
    if os.path.exists(tag_path):
        load_tag(conn, tag_path, quarter_name)
    if os.path.exists(num_path):
        load_num(conn, num_path, quarter_name, valid_adsh)
    if os.path.exists(pre_path):
        load_pre(conn, pre_path, quarter_name, valid_adsh)

    conn.commit()
    conn.close()
    print(f"\nDone reloading {quarter_name}.")


if __name__ == '__main__':
    import sys
    if len(sys.argv) == 2:
        reload_quarter(sys.argv[1])
    else:
        load_all(force=True)
