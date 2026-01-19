# src/load_data.py

import pandas as pd
from config import RAW_DATA_PATH

def load_all_tables():
    """
    Loads all CSV files into pandas DataFrames
    and returns them as a dictionary.
    """

    tables = {}

    tables["lead_logs"] = pd.read_csv(RAW_DATA_PATH + "lead_log.csv")
    tables["user_referrals"] = pd.read_csv(RAW_DATA_PATH + "user_referrals.csv")
    tables["user_referral_logs"] = pd.read_csv(RAW_DATA_PATH + "user_referral_logs.csv")
    tables["user_logs"] = pd.read_csv(RAW_DATA_PATH + "user_logs.csv")
    tables["user_referral_statuses"] = pd.read_csv(RAW_DATA_PATH + "user_referral_statuses.csv")
    tables["referral_rewards"] = pd.read_csv(RAW_DATA_PATH + "referral_rewards.csv")
    tables["paid_transactions"] = pd.read_csv(RAW_DATA_PATH + "paid_transactions.csv")

    return tables
if __name__ == "__main__":
    data_tables = load_all_tables()
    for name, df in data_tables.items():
        print(f"Loaded {name} with shape {df.shape}") 
        