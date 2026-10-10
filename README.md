# sec-data-core

Shared SEC XBRL data layer consumed by `sec-structured` and `sec-llm` (and any
future project). It turns the SEC's raw quarterly filings into two queryable
SQLite databases:

| Database | Size | Contents |
|---|---|---|
| `sec_viewer.db` | ~16 GB | Raw 10-K/10-K/A store (`sub`, `num`, `pre`, `tag`) |
| `sec_annual.db` | ~1 GB | Deduplicated annual mart, IS/BS/CF only (`sic_codes`, `companies`, `periods`, `facts`) |

The Python code and the GAAP taxonomy are in git. The **raw SEC data and the
built databases are not** (they total tens of gigabytes), so a fresh clone needs
a one-time data download and build. This README walks through that.

## TL;DR

```bash
git clone https://github.com/mcmch815/sec-data-core.git
cd sec-data-core

conda env create -f environment.yml        # Python 3.11 + pandas/pyarrow/odfpy/flask
conda run -n tf pip install -e .           # install the sec_core package

# Download Data/2009q1 … Data/2025q4 from the SEC (see the download loop below),
# then build both databases:
conda run -n tf python -m sec_core.db_loader
conda run -n tf python -m db_reduction.mart_loader
conda run -n tf python -m db_reduction.verify_mart
```

> **Disk:** budget **~50 GB free** (raw `Data/` ≈27 GB, `sec_viewer.db` ≈16 GB,
> `sec_annual.db` ≈1 GB).

---

## What is and isn't in git

| Path | In git? | Where it comes from |
|------|:-------:|---------------------|
| `sec_core/`, `db_reduction/`, `viewer/`, `checks/` | ✅ | `git clone` |
| `pyproject.toml`, `requirements.txt`, `environment.yml` | ✅ | `git clone` |
| `2025_GAAP_Taxonomy_Small.ods` + `..._Calculation.parquet` + `..._Presentation.parquet` | ✅ | `git clone` (no download) |
| `Data/<quarter>/{sub,tag,num,pre}.txt` (~27 GB) | ❌ | download from the SEC |
| `sec_viewer.db` (~16 GB) | ❌ | built from `Data/` |
| `sec_annual.db` (~1 GB) | ❌ | built from `sec_viewer.db` |
| `test_db/test_*.db` | ❌ | built from 4 quarters (fast smoke test) |

The GAAP taxonomy is committed, so `load_taxonomy()` and
`load_presentation_hierarchy()` work with no extra setup.

---

## 1. Environment

```bash
conda env create -f environment.yml          # first time
conda env update -f environment.yml --prune  # to refresh an existing env
conda run -n tf pip install -e .             # install the sec_core package into `tf`
```

`requirements.txt` holds the pinned versions and is the single source of truth;
`environment.yml` and `pyproject.toml` both draw from it.

---

## 2. Download the raw data

The raw files are the SEC **Financial Statement Data Sets**, published as one
zip per quarter:

```
https://www.sec.gov/files/dera/data/financial-statement-data-sets/<quarter>.zip
```

Each zip contains `sub.txt`, `tag.txt`, `num.txt`, `pre.txt` and `readme.htm` at
its root. They must be extracted into `Data/<quarter>/` — exactly the layout
`sec_core/db_loader.py` expects.

### Scope: the exact snapshot

To match this project as it stands, fetch **`2009q1` through `2025q4`** (68
quarters) and nothing newer. The SEC has published quarters after `2025q4`;
leave them out unless you deliberately want a newer dataset.

### Download loop

SEC's access policy requires a descriptive `User-Agent` with a contact — set it
first, then run the loop. It skips quarters already present, extracts only the
five files, and pauses between requests.

```bash
cd /path/to/sec-data-core
UA="Your Name you@example.com"          # <-- change this

for y in $(seq 2009 2025); do
  for q in 1 2 3 4; do
    quarter="${y}q${q}"
    dir="Data/${quarter}"
    if [ -d "$dir" ]; then echo "$quarter: present, skipping"; continue; fi
    echo "== ${quarter} =="
    curl -fSL -A "$UA" -o "/tmp/${quarter}.zip" \
      "https://www.sec.gov/files/dera/data/financial-statement-data-sets/${quarter}.zip"
    mkdir -p "$dir"
    conda run -n tf python -m zipfile -e "/tmp/${quarter}.zip" "$dir"
    rm -f "/tmp/${quarter}.zip"
    sleep 1
  done
done
```

Notes:

- `python -m zipfile -e` extracts the archive (no `unzip` needed). It also drops
  in `readme.htm`, which is harmless.
- To fetch a single quarter, e.g. `2025q4`: replace the two loops with
  `quarter="2025q4"` and run the body once.
- If a quarter is not yet published, `curl -f` fails and the loop moves on.

**Sanity check:** the four `.txt` files should be non-empty. Truncated downloads
are the usual failure mode — e.g. `2014q3`, `2014q4`, `2015q1`, `2015q4` should
each have a `num.txt` of roughly 350–425 MB. If one is far smaller, delete that
`Data/<quarter>/` directory and re-run to re-download it.

### Data provenance

If you keep the downloaded zips, record their SHA-256 alongside the quarter and
date; SEC revises past quarters over time, so this is how you remember exactly
which snapshot a database was built from.

---

## 3. Build the databases

```bash
# Raw store: reads every Data/<quarter>/ and (re)builds sec_viewer.db
conda run -n tf python -m sec_core.db_loader

# Annual mart: reads sec_viewer.db → sec_annual.db
conda run -n tf python -m db_reduction.mart_loader          # build (skips if it exists)
conda run -n tf python -m db_reduction.mart_loader --force  # rebuild from scratch

# Cross-checks (7 checks)
conda run -n tf python -m db_reduction.verify_mart
```

Reload a single quarter without a full rebuild:

```bash
conda run -n tf python -m sec_core.db_loader 2025q4
```

---

## 4. Quick smoke test (no 27 GB download)

Build small databases from four quarters instead of the full history:

```bash
conda run -n tf python -m db_reduction.build_test_dbs
```

This produces `test_db/test_viewer.db` and `test_db/test_annual.db`, and runs
the mart verification against them. See `readme/test_annual_db_readme.md` for how
the mart is built.

---

## 5. Viewer

```bash
conda run -n tf python viewer/app.py                                  # sec_annual.db
MART_DB=test_db/test_annual.db conda run -n tf python viewer/app.py   # test DB
```

Serves on `http://<host>:5050`. `viewer/restart.sh` stops any existing instance
and (re)starts it in the background, logging to `viewer/viewer.log`.

---

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `SEC_DATA_ROOT` | the repo root | Where `sec_core.paths` looks for `Data/`, the `*.db` files, and the taxonomy ODS. Set this if you install the data elsewhere. |
| `MART_DB` | `sec_annual.db` | Mart the viewer reads. |

---

## Copying a database between machines

SQLite runs in WAL mode, so a database may have `-wal` and `-shm` sidecars.
Before copying a `.db` elsewhere, fold the sidecars in so you don't ship a stale
or incomplete file:

```bash
conda run -n tf python -c "import sqlite3; sqlite3.connect('sec_annual.db').execute('PRAGMA wal_checkpoint(TRUNCATE)')"
```

Then copy `sec_annual.db` (and any remaining `-wal`/`-shm`).

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `HTTP 403` from sec.gov | Set a real `User-Agent` (`-A "Name email"`); don't use a browser UA. |
| A quarter won't download | It may not be published; skip it or check the SEC page. |
| `No quarter directories found in Data/` | Download and extract the data (step 2) before building. |
| A database looks short on facts | Re-check the `Data/<quarter>/num.txt` sizes (see the sanity check in step 2); re-download any that are small. |
| Out of disk | The downloaded zips can be deleted once extracted; `Data/` itself can be removed once `sec_viewer.db` is built. |
| Mart verification fails | Re-run with `--force`; read the failing check's message. |

---

## Quick facts

- **Install root:** repo root, or `$SEC_DATA_ROOT`.
- **Env:** conda env `tf`, Python 3.11 — `conda env create -f environment.yml`, then `conda run -n tf pip install -e .`.
- **Raw data needed:** `Data/<YYYY>q<Q>/{sub,tag,num,pre}.txt`, from `https://www.sec.gov/files/dera/data/financial-statement-data-sets/<YYYY>q<Q>.zip` (files at the zip root).
- **Scope:** `2009q1`–`2025q4` only.
- **Committed, do not download:** `2025_GAAP_Taxonomy_Small.ods`, `..._Calculation.parquet`, `..._Presentation.parquet`.
- **Build order:** `Data/` → `sec_viewer.db` (`python -m sec_core.db_loader`) → `sec_annual.db` (`python -m db_reduction.mart_loader`) → verify (`python -m db_reduction.verify_mart`).
- **Query:** `sec_annual.db` tables `sic_codes`, `companies`, `periods`, `facts`. Schema and query patterns are in `CLAUDE.md`; mart construction is in `readme/test_annual_db_readme.md`.
