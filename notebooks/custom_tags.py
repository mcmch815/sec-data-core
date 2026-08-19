# %% [markdown]
# # Taxonomy Gap Analysis
# 
# How many tags in `test_annual.db` (facts table) do **not** appear anywhere in the 2025 GAAP taxonomy?
# 
# We check against all tags known to the taxonomy:
# - `tag_info` from the Calculation sheet (all parent + child tags)
# - `pres_parent` from the Presentation sheet (all tags in hierarchy)
# 
# A tag that appears in neither is a **custom** or **deprecated** XBRL tag that filers invented or carried over from older taxonomy versions.

# %%
import os
import sqlite3
from pathlib import Path

import pandas as pd

# Point sec_core at the test mart DB
ROOT = Path("E:/SEC_projects/sec-data-core")
TEST_MART = ROOT / "test_db" / "test_annual.db"
assert TEST_MART.exists(), f"Test mart not found: {TEST_MART}"

os.environ["SEC_DATA_ROOT"] = str(ROOT)

# %%
# Load taxonomy — uses project-root ODS / parquet cache
from sec_core.taxonomy_loader import load_taxonomy, load_presentation_hierarchy

calc_map, calc_by_role, tag_info, parent_map, all_parents = load_taxonomy()
pres_descendants, pres_parent = load_presentation_hierarchy()

# All tags known to the taxonomy (Calculation + Presentation sheets combined)""
taxonomy_tags = set(tag_info.keys()) | set(pres_parent.keys())
print(f"Taxonomy tag universe: {len(taxonomy_tags):,} unique tags")
print(f"  From Calculation sheet (tag_info): {len(tag_info):,}")
print(f"  From Presentation sheet (pres_parent): {len(pres_parent):,}")

# %%
# Pull every distinct tag that appears in the mart facts table
conn = sqlite3.connect(f"file:{TEST_MART}?mode=ro", uri=True)

facts_tags_df = pd.read_sql_query(
    "SELECT tag, label, stmt, COUNT(*) AS n_rows, COUNT(DISTINCT cik) AS n_companies "
    "FROM facts GROUP BY tag, label, stmt ORDER BY tag",
    conn,
)
conn.close()

print(f"Distinct (tag, stmt) combos in mart: {len(facts_tags_df):,}")
print(f"Distinct tags in mart:               {facts_tags_df['tag'].nunique():,}")

# %%
# Classify each tag
facts_tags_df["in_taxonomy"] = facts_tags_df["tag"].isin(taxonomy_tags)
facts_tags_df["in_calc"]     = facts_tags_df["tag"].isin(tag_info.keys())
facts_tags_df["in_pres"]     = facts_tags_df["tag"].isin(pres_parent.keys())

summary = facts_tags_df.groupby("in_taxonomy")[["tag"]].nunique().rename(columns={"tag": "unique_tags"})
summary.index = summary.index.map({True: "In taxonomy", False: "NOT in taxonomy"})
summary["pct"] = (summary["unique_tags"] / summary["unique_tags"].sum() * 100).round(1)
print(summary.to_string())

# %% [markdown]
# ## Tags missing from the taxonomy — detail

# %%
missing = (
    facts_tags_df[~facts_tags_df["in_taxonomy"]]
    .groupby("tag")
    .agg(
        label=("label", "first"),
        stmts=("stmt", lambda s: ", ".join(sorted(s.unique()))),
        total_rows=("n_rows", "sum"),
        n_companies=("n_companies", "max"),
    )
    .sort_values("n_companies", ascending=False)
    .reset_index()
)

print(f"{len(missing):,} tags in the mart have NO match in the 2025 GAAP taxonomy")
missing

# %% [markdown]
# ## Breakdown by statement

# %%
stmt_breakdown = (
    facts_tags_df.groupby(["stmt", "in_taxonomy"])["tag"]
    .nunique()
    .unstack(fill_value=0)
    .rename(columns={True: "in_taxonomy", False: "missing"})
)
stmt_breakdown["total"] = stmt_breakdown.sum(axis=1)
stmt_breakdown["pct_missing"] = (
    stmt_breakdown["missing"] / stmt_breakdown["total"] * 100
).round(1)
stmt_breakdown

# %% [markdown]
# ## Are the missing tags custom extensions?
# 
# Custom XBRL tags (invented by filers) are typically lowercase or contain a company-specific prefix. Standard `us-gaap` tags are PascalCase. A quick heuristic: does the tag start with a lowercase letter?

# %%
missing["likely_custom"] = missing["tag"].str[0].str.islower()

print("Missing tags — likely custom (starts lowercase):")
print(missing["likely_custom"].value_counts().to_string())
print()

# Show the non-custom ones (PascalCase, should be standard but missing from taxonomy)
non_custom = missing[~missing["likely_custom"]].copy()
print(f"\nPascalCase tags NOT in taxonomy ({len(non_custom)} tags — likely deprecated or removed):")
non_custom[["tag", "label", "stmts", "n_companies", "total_rows"]]

# %% [markdown]
# ## Coverage summary

# %%
total_facts = facts_tags_df["n_rows"].sum()
missing_facts = facts_tags_df.loc[~facts_tags_df["in_taxonomy"], "n_rows"].sum()

print("=" * 50)
print("COVERAGE SUMMARY")
print("=" * 50)
print(f"Total fact rows in mart:          {total_facts:>10,}")
print(f"Rows with non-taxonomy tags:      {missing_facts:>10,}  ({missing_facts/total_facts*100:.1f}%)")
print()
total_tags = facts_tags_df['tag'].nunique()
missing_tags = missing['tag'].nunique()
print(f"Total distinct tags in mart:      {total_tags:>10,}")
print(f"Tags not in taxonomy:             {missing_tags:>10,}  ({missing_tags/total_tags*100:.1f}%)")


