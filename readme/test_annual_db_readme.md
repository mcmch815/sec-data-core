# How `test_annual.db` is built from `test_viewer.db`

This document explains in plain English the full pipeline from raw SEC filing data
to the deduplicated annual mart used in tests.

---

## 0. What the test databases are

`test_viewer.db` is a miniature version of the full `sec_viewer.db`. Instead of all
quarters from 2009 to present, it contains only two quarters of 10-K filings:
**2024q4 and 2025q4**. It has the same four-table schema (`sub`, `num`, `pre`, `tag`)
and is built the same way — just pointed at a subset of the raw data.

`test_annual.db` is the deduplicated annual mart derived from `test_viewer.db`. It is
structurally identical to the full `sec_annual.db` and is used so tests run fast
without touching the 16 GB production database.

---

## 1. Load raw SEC data into `test_viewer.db`

For each test quarter (`2024q4`, `2025q4`), the loader reads four pipe-delimited text
files from `Data/<quarter>/`:

| File | What it contains |
|------|-----------------|
| `sub.txt` | One row per filing: company name, CIK, form type, period, fiscal year, date filed |
| `tag.txt` | XBRL tag definitions and taxonomy labels |
| `num.txt` | Numeric values for each tag in each filing |
| `pre.txt` | Presentation info: which statement (IS/BS/CF) each tag appears in, and in what order |

**Only 10-K and 10-K/A filings are kept** — the loader filters `sub.txt` to these
form types and discards everything else. The resulting set of valid accession numbers
(`adsh`) is used as a whitelist when loading `num.txt` and `pre.txt`, so no quarterly
or registration-statement data leaks in.

---

## 2. Build `test_annual.db` — the annual mart

`test_annual.db` has four tables: `sic_codes`, `companies`, `periods`, and `facts`.
Here is how each is populated.

### 2a. `sic_codes`

A static lookup table of 444 SEC-published SIC codes. It is copied in directly from
a hardcoded Python dict (`sec_core/sic.py`) — no filtering, no dedup logic needed.
Each row has: SIC code, description, SEC office, and division (A–J).

### 2b. `companies`

One row per CIK (company). When a company has multiple filings (e.g. a 10-K/A
amendment after the original 10-K), we take the name and SIC code from the
**most recently filed** submission. This means the company table always reflects the
latest known name, even if the company changed its name mid-history.

### 2c. `facts` — the core of the mart

This is where most of the work happens. The goal is to produce **one canonical value
per (company, tag, statement, fiscal-period)** — no duplicates.

#### Step 1: Identify fiscal year-end dates

We first build a list of known fiscal year-end dates per company by reading all
`sub.period` values. These are the dates a company actually ended a fiscal year.
This whitelist is used later to exclude carryforward data — values that appear in a
10-K for a *prior* period date (e.g. Apple's FY2024 10-K also reports the FY2023
balance sheet for comparison). We only want the authoritative period from the filing
that originally reported it.

#### Step 2: Choose one canonical filing per (company, period-date)

For each combination of company + period date, we pick a single **canonical `adsh`**
(accession number). The rule is: **most recently filed wins**.

This handles the common case where a company files a 10-K and later files a 10-K/A
to correct it. The amendment supersedes the original. By picking the latest `adsh`,
we automatically use the amended values everywhere.

At this stage we also enforce two filters on the raw `num` rows considered:
- **`segments IS NULL` and `coreg IS NULL`** — primary entity only (no segment
  breakdowns, no co-registrant sub-entities)
- **`qtrs IN (0, 4)`** — balance sheet instant values (`qtrs=0`) and full-year
  flow values (`qtrs=4`); quarterly interim values are excluded

#### Step 3: Primary pass — null-segment rows

Using the canonical `adsh` for each (company, period), we pull all numeric values
that have no segment dimension (`segments IS NULL`, `coreg IS NULL`). These are the
consolidated, entity-level values — exactly what you'd read off the face of the
financial statements.

Each value is joined to the `pre` table to find out which statement it belongs to
(IS = Income Statement, BS = Balance Sheet, CF = Cash Flow). The `pre` table also
gives us presentation order (`report`, `line`), the filing-specific label (`plabel`),
and display flags like `negating` (flip the sign for display) and `inpth`
(footnote — the line appears in-parenthesis in the filing, not on the face of the statement).

**A tag that appears in both IS and CF** (e.g. `NetIncomeLoss`) produces **two rows**
— one for each statement. This is intentional: the same value plays different roles
in each statement.

Tags with no IS/BS/CF `pre` entry for their canonical filing are dropped entirely
(we can't assign them to a statement).

#### Step 4: Segmented fallback

Some companies only file certain values in segmented form — they never supply a
consolidated null-segment row. To capture these, we run a second pass over rows where
`segments IS NOT NULL`, restricted to rows with exactly one dimension
(detected by counting `=` signs in the segments string).

These single-dimension rows are summed across all dimension members to reconstruct
a consolidated value. However, rows tagged `ConsolidationItems=...` are excluded from
this sum — those are subtotals of the segment breakdown itself, not independent peer
values, so including them would double-count. If *all* rows for a group happen to be
`ConsolidationItems=` (nothing else available), they are included as a last resort.

**`INSERT OR IGNORE`** is used for the fallback insert, so primary-pass rows are
never overwritten. The fallback only fills gaps.

### 2d. `periods`

After `facts` is populated, we build the `periods` table mapping each (company,
period-date) pair to a fiscal year label (`fy`). The `fy` comes from `sub.fy` where
available, falling back to the calendar year of the period date.

**Edge case — `fy` label collisions:** Some companies (particularly those that
changed their fiscal year-end) end up with two different period dates assigned the
same `fy` label. When this happens because *both* were matched to a real `sub.fy`
row (not a fallback), it means one of the `fy` labels in the SEC data is wrong. In
that case, we reassign the conflicting row to use the calendar year of its period
date instead. This is called a "Type 1 collision fix."

---

## 3. Indexes

After all data is loaded, four indexes are created on `facts`:

- `(cik, stmt, ddate)` — fast lookup by company + statement + period (main query pattern)
- `(cik, ddate)` — lookup by company + period across all statements
- `(tag)` — lookup all companies that reported a given tag
- `(cik)` on `periods` — fast period lookup per company

---

## 4. Verification

After the mart is built, seven cross-checks are run against the source:

1. Apple (`CIK=320193`) has at least 2 fiscal periods in the mart.
2. Apple's revenue tag `RevenueFromContractWithCustomerExcludingAssessedTax` is present.
3. Every `facts.cik` exists in `companies`.
4. `facts` contains rows for all three statement types (IS, BS, CF).
5. The IS/BS/CF filter excluded at least 10% but no more than 99% of raw `num` rows
   (sanity-checks that the statement filter is working but not over-filtering).
6. No (cik, tag, stmt, ddate, qtrs) duplicates exist in `facts`.
7. The fraction of facts with a NULL value is below 5%.

---

## Summary

```
test_viewer.db (2024q4 + 2025q4 10-K filings only)
    │
    ├─ sub + num + pre + tag tables
    │
    ▼
mart_loader.py
    │
    ├─ sic_codes:  static lookup, copied from sec_core/sic.py
    ├─ companies:  one row per CIK, most-recently-filed name/SIC
    ├─ facts:      canonical adsh per (cik, ddate)
    │              → null-segment primary pass
    │              → segmented fallback (eq_count=1, summed)
    │              → INSERT OR IGNORE keeps primary rows safe
    └─ periods:    fy from sub.fy, with Type 1 collision correction
    │
    ▼
test_annual.db (~36 MB)
```
