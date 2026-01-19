# src/clean_data.py

def clean_tables(tables):
    """
    Cleans all tables by:
    - Removing duplicate rows
    - Removing fully empty rows
    """

    for table_name, df in tables.items():
        # Remove duplicate rows
        df.drop_duplicates(inplace=True)

        # Remove rows where all columns are null
        df.dropna(how="all", inplace=True)

    return tables
