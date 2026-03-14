"""sec_core — shared SEC data access layer.

Provides database connection, path resolution, and taxonomy loading
for all projects consuming the SEC XBRL structured dataset.
"""

import sqlite3
from sec_core.paths import DB_PATH, MART_DB_PATH
from sec_core.taxonomy_loader import load_taxonomy, load_presentation_hierarchy

__all__ = [
    'get_connection',
    'get_connection_mart',
    'DB_PATH',
    'MART_DB_PATH',
    'load_taxonomy',
    'load_presentation_hierarchy',
]


def get_connection(**kwargs):
    """Return a read-only sqlite3 connection to sec_viewer.db.

    Uses SQLite URI mode with mode=ro — any attempt to write will raise
    an OperationalError. Passes any kwargs to sqlite3.connect
    (e.g. check_same_thread=False).
    """
    uri = f"file:{DB_PATH}?mode=ro"
    return sqlite3.connect(uri, uri=True, **kwargs)


def get_connection_mart(**kwargs):
    """Return a read-only sqlite3 connection to sec_annual.db.

    Raises FileNotFoundError if the mart has not been built yet.
    Run: python -m db_reduction.mart_loader
    """
    if not MART_DB_PATH.exists():
        raise FileNotFoundError(
            f"sec_annual.db not found at {MART_DB_PATH}. "
            "Run: python -m db_reduction.mart_loader"
        )
    uri = f"file:{MART_DB_PATH}?mode=ro"
    return sqlite3.connect(uri, uri=True, **kwargs)
