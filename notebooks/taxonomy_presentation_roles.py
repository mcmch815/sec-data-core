# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: tf
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Taxonomy Presentation — Role Exploration
#
# Explore the unique `definition` values in the 2025 GAAP Taxonomy presentation parquet,
# with a focus on Statement-type roles.

# %%
import pandas as pd
from sec_core.paths import PRES_PARQUET

pres = pd.read_parquet(PRES_PARQUET)
print("Shape:", pres.shape)
pres.head(3)

# %% [markdown]
# ## All unique definitions

# %%
all_defs = sorted(pres['definition'].unique())
for d in all_defs:
    print(d)

# %% [markdown]
# ## Statement-type roles only

# %%
stmt_defs = [d for d in all_defs if ' - Statement - ' in d]
for d in stmt_defs:
    print(d)

# %% [markdown]
# ## Counts per definition type

# %%
pres['def_type'] = pres['definition'].str.extract(r' - (\w+) - ')
pres['def_type'].value_counts()

# %% [markdown]
# ## Row counts per statement role

# %%
stmt_rows = pres[pres['definition'].isin(stmt_defs)]
stmt_rows.groupby('definition').size().rename('row_count').reset_index()

# %%

# %%

# %%
