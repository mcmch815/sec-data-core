"""Parse the GAAP taxonomy ODS file and build calculation relationships."""

import os
from collections import defaultdict
import pandas as pd

from sec_core.paths import ODS_PATH, CALC_PARQUET, PRES_PARQUET

_PARQUET = {
    'Calculation':  str(CALC_PARQUET),
    'Presentation': str(PRES_PARQUET),
}


def _read_sheet(sheet_name):
    """Read ODS sheet, using a Parquet cache after the first parse."""
    path = _PARQUET[sheet_name]
    if os.path.exists(path):
        return pd.read_parquet(path)
    print(f"  [taxonomy] First run — parsing ODS '{sheet_name}' sheet (one-time ~28s)...")
    df = pd.read_excel(str(ODS_PATH), sheet_name=sheet_name, engine='odf')
    df.to_parquet(path, index=False)
    print(f"  [taxonomy] Saved {os.path.basename(path)} — future loads will be fast")
    return df


# Primary roles we care about for balance checking
ROLE_MAP = {
    'StatementOfFinancialPositionClassified': 'BS',
    'StatementOfIncome': 'IS',
    'StatementOfCashFlowsIndirect': 'CF',
    # Alternatives (fallbacks)
    'StatementOfFinancialPositionClassifiedFirstAlternative': 'BS',
    'StatementOfFinancialPositionClassifiedSecondAlternative': 'BS',
    'StatementOfFinancialPositionUnclassified-DepositBasedOperations': 'BS',
    'StatementOfIncomeFirstAlternative': 'IS',
    'StatementOfIncomeDiscontinuedOperationsAlternate': 'IS',
    'StatementOfCashFlowsDirect': 'CF',
    'StatementOfOtherComprehensiveIncome': 'CI',
    'StatementOfShareholdersEquityAndOtherComprehensiveIncome': 'EQ',
}


def load_taxonomy():
    """Load the Calculation sheet and build parent-child relationships.

    Returns:
        calc_map: dict {parent_tag: [(child_tag, weight), ...]}
            keyed by parent tag name, values are lists of (child, weight) tuples.
            Covers ALL roles so we can match against any filing.
        tag_info: dict {tag_name: {label, roles: set}}
    """
    df = _read_sheet('Calculation')

    # Extract role short name from URL
    df['role'] = df['extended link role'].str.extract(
        r'role/statement/(.+)$')[0]

    # Clean parent: strip 'us-gaap:' prefix
    df['parent_clean'] = df['parent'].str.replace('us-gaap:', '', regex=False)

    # Split roots (depth=0, no parent/weight) from valid child rows
    mask = df['parent_clean'].notna() & df['weight'].notna()
    df_roots = df[~mask]
    df_valid = df[mask].copy()
    df_valid['weight'] = df_valid['weight'].astype(int)

    # calc_map: global all-roles deduped — iterate over groups (O(n_groups))
    pairs = df_valid[['parent_clean', 'name', 'weight']].drop_duplicates()
    calc_map = {}
    for parent, grp in pairs.groupby('parent_clean', sort=False):
        calc_map[parent] = list(zip(grp['name'], grp['weight']))

    # calc_by_role: per (role, parent)
    calc_by_role = {}
    for (role, parent), grp in df_valid.groupby(['role', 'parent_clean'], sort=False):
        calc_by_role[(role, parent)] = list(zip(grp['name'], grp['weight']))

    # tag_info — vectorized aggregations, no iterrows
    child_labels = df_valid.drop_duplicates('name').set_index('name')['label'].to_dict()
    child_roles  = df_valid.groupby('name')['role'].apply(set).to_dict()
    parent_roles = df_valid.groupby('parent_clean')['role'].apply(set).to_dict()
    tag_info = {}
    for name, roles in child_roles.items():
        tag_info[name] = {'label': child_labels.get(name, name),
                          'roles': {r for r in roles if isinstance(r, str)}}
    for name, roles in parent_roles.items():
        if name not in tag_info:
            tag_info[name] = {'label': name, 'roles': set()}
        tag_info[name]['roles'].update(r for r in roles if isinstance(r, str))
    # roots: small itertuples loop (few rows)
    for row in df_roots.itertuples(index=False):
        name = row.name
        if name not in tag_info:
            tag_info[name] = {'label': getattr(row, 'label', name), 'roles': set()}
        role = getattr(row, 'role', None)
        if isinstance(role, str):
            tag_info[name]['roles'].add(role)

    # Build parent_map: {(role, child_tag): (parent_tag, weight)}
    parent_map = {}
    for (role, parent_tag), children in calc_by_role.items():
        for child_tag, weight in children:
            parent_map[(role, child_tag)] = (parent_tag, weight)

    # all_parents: {child_tag: [(parent_tag, weight), ...]} — role-agnostic
    import math
    all_parents = {}
    seen = set()
    for (role, child), (parent, weight) in parent_map.items():
        if isinstance(role, float) and math.isnan(role):
            continue
        key = (child, parent, weight)
        if key not in seen:
            seen.add(key)
            all_parents.setdefault(child, []).append((parent, weight))

    return calc_map, calc_by_role, tag_info, parent_map, all_parents


def load_presentation_hierarchy():
    """Load the Presentation sheet and build abstract tag descendant maps.

    Returns:
        pres_descendants: dict {abstract_tag_name: set of all descendant tag names}
            Only includes non-structural descendants (excludes Abstract, Table, Axis,
            Domain, Member suffixes). These are the potential value-bearing tags.
        pres_parent: dict {tag_name: parent_tag_name}
            Parent relationship for every tag in the presentation hierarchy.
    """
    df = _read_sheet('Presentation')

    # Clean parent: strip namespace prefix (e.g. 'us-gaap:', 'dei:')
    df['parent_clean'] = df['parent'].str.replace(r'^[a-z-]+:', '', regex=True)

    # pres_parent dict — vectorized one-liner (exclude rows with null name or parent)
    valid = df[df['parent_clean'].notna() & df['name'].notna()][['name', 'parent_clean']]
    pres_parent = dict(zip(valid['name'], valid['parent_clean']))

    # children_map — vectorized groupby
    children_map = defaultdict(list,
        valid.groupby('parent_clean')['name'].apply(list).to_dict()
    )

    # Suffixes that indicate structural/dimensional nodes (not value tags)
    _non_value_suffixes = ('Abstract', 'Table', 'Axis', 'Domain', 'Member',
                           'LineItems', 'Roll')

    def _is_value_tag(name):
        if not isinstance(name, str):
            return False
        return not any(name.endswith(s) for s in _non_value_suffixes)

    def _all_descendants(tag):
        """BFS to collect all descendants of a tag."""
        result = set()
        stack = list(children_map.get(tag, []))
        while stack:
            current = stack.pop()
            if current not in result:
                result.add(current)
                stack.extend(children_map.get(current, []))
        return result

    # For each abstract tag, build set of value-bearing descendants
    pres_descendants = {}
    abstract_tags = [name for name in df['name'].dropna() if name.endswith('Abstract')]
    for tag in abstract_tags:
        all_desc = _all_descendants(tag)
        pres_descendants[tag] = {t for t in all_desc if _is_value_tag(t)}

    return pres_descendants, pres_parent


def get_children(calc_map, parent_tag):
    """Get direct children and their weights for a parent tag."""
    return calc_map.get(parent_tag, [])


def is_parent(calc_map, tag):
    """Check if a tag is a parent (has children in calculation relationships)."""
    return tag in calc_map


if __name__ == '__main__':
    calc_map, calc_by_role, tag_info, parent_map, all_parents = load_taxonomy()
    print(f"Total parent tags: {len(calc_map)}")
    print(f"Total tags: {len(tag_info)}")

    print("\nBalance Sheet - Assets children:")
    for child, weight in get_children(calc_map, 'Assets'):
        print(f"  {child} (w={weight:+d})")

    print("\nBalance Sheet - LiabilitiesAndStockholdersEquity children:")
    for child, weight in get_children(calc_map, 'LiabilitiesAndStockholdersEquity'):
        print(f"  {child} (w={weight:+d})")

    print("\nIncome Statement - ProfitLoss children:")
    for child, weight in get_children(calc_map, 'ProfitLoss'):
        print(f"  {child} (w={weight:+d})")
