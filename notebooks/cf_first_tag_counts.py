# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
# ---

# %%

# %% [markdown]
# # CF Statement — First Tag Frequency
#
# For each Cash Flow statement in `test_annual.db`, find the tag with the lowest `line` value (i.e. the first tag presented), then count how often each tag appears in that role.

# %%
import sqlite3
import pandas as pd

con = sqlite3.connect('../test_db/test_annual.db')

# %%
query = """
WITH cf_min_line AS (
    SELECT cik, ddate, MIN(line) AS min_line
    FROM facts
    WHERE stmt = 'CF'
    GROUP BY cik, ddate
),
first_tags AS (
    SELECT f.tag, f.label, f.cik, f.ddate, f.version, f.datatype, f.inpth
    FROM facts f
    JOIN cf_min_line m
      ON f.cik = m.cik
     AND f.ddate = m.ddate
     AND f.line = m.min_line
    WHERE f.stmt = 'CF'
),
counts AS (
    SELECT tag, label, COUNT(*) AS filing_count
    FROM first_tags
    GROUP BY tag, label
),
examples AS (
    SELECT tag, cik AS example_cik, ddate AS example_ddate, version AS example_version, datatype AS example_datatype, inpth AS example_inpth,
           ROW_NUMBER() OVER (PARTITION BY tag ORDER BY cik) AS rn
    FROM first_tags
)
SELECT c.tag, c.label, c.filing_count,
       e.example_cik, e.example_ddate, e.example_version, e.example_datatype, e.example_inpth
FROM counts c
JOIN examples e ON c.tag = e.tag AND e.rn = 1
ORDER BY c.filing_count DESC
"""

df = pd.read_sql_query(query, con)
con.close()
df

# %%



