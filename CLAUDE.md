# CLAUDE.md — sec-data-core

Shared SEC data layer consumed by `sec-structured` and `sec-llm` (and any future projects).

## Location

`E:\SEC_projects\sec-data-core\`

Install into the shared `sec` venv (also used by `sec-llm` and `sec_structured_project`):
```powershell
E:\SEC_projects\.venv-sec\Scripts\pip.exe install -e E:\SEC_projects\sec-data-core
```

## What lives here

| Path | Contents |
|------|----------|
| `sec_core/` | Python package |
| `sec_core/paths.py` | Resolves `DB_PATH`, `MART_DB_PATH`, `DATA_DIR`, `ODS_PATH` |
| `sec_core/db_loader.py` | Builds `sec_viewer.db` from raw SEC quarterly files |
| `sec_core/taxonomy_loader.py` | Parses GAAP taxonomy ODS → calculation & presentation hierarchies |
| `sec_core/__init__.py` | Exports `get_connection()`, `get_connection_mart()`, `load_taxonomy()`, `load_presentation_hierarchy()` |
| `db_reduction/` | Scripts package (not installed); builds and verifies `sec_annual.db` |
| `db_reduction/mart_loader.py` | Builds `sec_annual.db` from `sec_viewer.db` |
| `db_reduction/verify_mart.py` | Cross-checks mart vs source (7 checks) |
| `Data/` | Raw SEC quarterly files (2009q1–2025q4), ~27GB |
| `sec_viewer.db` | SQLite built from `Data/`, ~16GB |
| `sec_annual.db` | Deduplicated annual mart, IS/BS/CF only, ~200–400MB |
| `2025_GAAP_Taxonomy_Small.ods` | GAAP 2025 taxonomy (Calculation + Presentation sheets) |
| `2025_GAAP_Taxonomy_Small_Calculation.parquet` | Parquet cache of Calculation sheet |
| `2025_GAAP_Taxonomy_Small_Presentation.parquet` | Parquet cache of Presentation sheet |

## Working Assumptions

**Never assume SEC filing data is wrong.** When data looks unexpected — duplicate periods, unusual `fy` labels, values that seem incorrect — assume our understanding of the SEC data format or our pipeline logic is incomplete, not that the filer made an error. Investigate the data first. Known legitimate edge cases include:

- Companies with non-December fiscal year-ends where `sub.fy` differs from `substr(period, 1, 4)`
- Companies that changed their fiscal year-end mid-history (two different year-end dates in the same calendar year)
- Tags that appear in multiple statements (e.g. `NetIncomeLoss` in both IS and CF)
- `qtrs=0` facts for interim dates within a fiscal year included as supplementary data in 10-K filings

## Path resolution

`sec_core/paths.py` defaults to the `sec-data-core/` root. Override with env var:
```powershell
$env:SEC_DATA_ROOT = "E:\path\to\sec-data-core"
```

## SQLite Schema (`sec_viewer.db`)

Only 10-K and 10-K/A filings are loaded. Four tables:

- **`sub`** — Filing submissions: `adsh` (accession), `cik`, `name`, `form`, `period`, `fy`, `fp`, `filed`, `quarter`
- **`num`** — Numeric values: `adsh`, `tag`, `version`, `ddate`, `qtrs`, `uom`, `value`, `segments`, `coreg`
- **`pre`** — Presentation order: `adsh`, `stmt`, `report`, `line`, `tag`, `version`, `plabel`, `negating`, `inpth`
- **`tag`** — Tag metadata: `tag`, `version`, `label`, `custom`, `abstract`

Key filtering rules for correct value extraction:
- `segments IS NULL AND coreg IS NULL` — primary entity only (no segment breakdowns)
- `qtrs = 0` — Balance Sheet (instant values)
- `qtrs = 4` — Income Statement / Cash Flow (full-year values)

## SIC Office → Taxonomy Presentation Role

A company's `office` in `sic_codes` determines which taxonomy presentation role applies to its statements. **Not yet implemented in code** — mapping is planned.

The 2025 GAAP presentation parquet contains multiple role variants per statement type identified by the `definition` column. The `office` field is the primary signal for selecting the correct role.

**Balance Sheet** (documented; see also `sec-llm/CLAUDE.md`):

| Office / SIC | Role |
|---|---|
| `"Office of Finance"` (SIC 6000–6411: banks, insurance, savings) | Unclassified (`108000`/`108200`/`112000`) |
| SIC 1531 `OPERATIVE BUILDERS` | Unclassified (multi-year inventory cycles) |
| Most others | Classified (`104000`) |

**Income Statement / Cash Flows:** industry-specific variants exist (Interest Based, Insurance Based, Securities Based, Real Estate, REITs, Direct Method) but the `office` → role mapping is **not yet designed**.

## Taxonomy Module

`load_taxonomy()` returns:
- `calc_map` — `{parent_tag: [(child_tag, weight), ...]}`
- `calc_by_role` — `{(role, parent_tag): [(child_tag, weight), ...]}`
- `tag_info` — `{tag_name: {label, roles}}`
- `parent_map` — `{(role, child_tag): (parent_tag, weight)}`
- `all_parents` — `{child_tag: [(parent_tag, weight), ...]}` (cross-role)

`load_presentation_hierarchy()` returns:
- `pres_descendants` — `{abstract_tag: set of value-bearing descendant tags}`
- `pres_parent` — `{tag: parent_tag}`

## Rebuilding the DB

```powershell
cd E:\SEC_projects\sec-data-core
E:\SEC_projects\.venv-sec\Scripts\python.exe -m sec_core.db_loader          # full rebuild
E:\SEC_projects\.venv-sec\Scripts\python.exe -m sec_core.db_loader 2025q4   # reload one quarter
```

## Building / Rebuilding the Annual Mart (`sec_annual.db`)

```powershell
cd E:\SEC_projects\sec-data-core
E:\SEC_projects\.venv-sec\Scripts\python.exe -m db_reduction.mart_loader           # build (skip if exists)
E:\SEC_projects\.venv-sec\Scripts\python.exe -m db_reduction.mart_loader --force   # rebuild from scratch
E:\SEC_projects\.venv-sec\Scripts\python.exe -m db_reduction.verify_mart           # cross-check 7 checks
```

## Annual Mart Schema (`sec_annual.db`)

One canonical value per `(company, tag, fiscal-date)`, IS/BS/CF only.

```sql
-- sic_codes: 444 SEC-published SIC codes with description, office, division (A–J), division_name
-- companies: most recent name and SIC code per CIK (sic FK → sic_codes)
-- periods:   (cik, ddate) pairs with fy label
-- facts:     (cik, tag, label, stmt, ddate, qtrs, report, line, plabel, negating, inpth, version, uom, value, display_value, filed, crdr, iord, datatype)
--            crdr: 'C'=credit, 'D'=debit; iord: 'I'=instant, 'D'=duration; NULL for custom/unknown tags
--            version: taxonomy version string from pre (e.g. 'us-gaap/2023')
--            inpth: '1' if the line is a footnote (appears in-parenthesis in the filing, not on the face of the statement)
--            filed: date the original 10-K was submitted to SEC (YYYYMMDD) — use for point-in-time filtering
```

**`display_value`** is `value` with its sign flipped when `negating='1'` (presentation-context sign convention applied). Use `display_value` for rendering; use `value` for arithmetic.

**LLM query pattern** (CIK + year + statement):
```sql
SELECT f.label, f.display_value, f.uom
FROM facts f JOIN periods p ON f.cik = p.cik AND f.ddate = p.ddate
WHERE f.cik = '320193' AND p.fy = '2024' AND f.stmt = 'IS'
ORDER BY f.report, f.line;
```

**Dedup rule:** earliest filed `adsh` wins per `(cik, tag, ddate, qtrs)` — original 10-K is preferred over 10-K/A amendments.
`filed` on each fact row records when that value became publicly available (YYYYMMDD); use `WHERE filed <= :as_of_date` for point-in-time ML queries.

**Access:** `from sec_core import get_connection_mart` (raises `FileNotFoundError` if not built).
