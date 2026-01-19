# src/profiling.py

import pandas as pd

def profile_table(df, table_name):
    """
    Generates profiling information for a single table.
    """

    profile_rows = []

    for column in df.columns:
        profile_rows.append({
            "table_name": table_name,
            "column_name": column,
            "null_count": df[column].isna().sum(),
            "distinct_count": df[column].nunique()
        })

    return pd.DataFrame(profile_rows)


def generate_profiling_report(tables):
    """
    Generates profiling report for all tables.
    """

    all_profiles = []

    for table_name, df in tables.items():
        table_profile = profile_table(df, table_name)
        all_profiles.append(table_profile)

    return pd.concat(all_profiles, ignore_index=True)
