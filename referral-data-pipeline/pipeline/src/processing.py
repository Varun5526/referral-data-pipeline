# src/processing.py

import pandas as pd

def process_data(tables):
    """
    Joins all tables and prepares dataset for business logic.
    """

    referrals = tables["user_referrals"]
    referral_logs = tables["user_referral_logs"]
    statuses = tables["user_referral_statuses"]
    rewards = tables["referral_rewards"]
    transactions = tables["paid_transactions"]
    leads = tables["lead_logs"]
    users = tables["user_logs"]

    # 1️⃣ Join referral logs
    df = referrals.merge(
        referral_logs,
        left_on="referral_id",
        right_on="user_referral_id",
        how="left"
    )

    # 2️⃣ Join referral status
    df = df.merge(
        statuses,
        left_on="user_referral_status_id",
        right_on="id",
        how="left",
        suffixes=("", "_status")
    )

    # 3️⃣ Join referral rewards
    df = df.merge(
        rewards,
        left_on="referral_reward_id",
        right_on="id",
        how="left",
        suffixes=("", "_reward")
    )

    # 4️⃣ Join transactions
    df = df.merge(
        transactions,
        on="transaction_id",
        how="left"
    )

    # 5️⃣ Join leads (for source category)
    df = df.merge(
        leads,
        left_on="referee_id",
        right_on="lead_id",
        how="left",
        suffixes=("", "_lead")
    )

    # 6️⃣ JOIN USER LOGS (FIXED ❗)
    df = df.merge(
        users,
        left_on="referrer_id",
        right_on="user_id",
        how="left",
        suffixes=("", "_referrer")
    )

    # 7️⃣ Create referral_source_category
    df["referral_source_category"] = df.apply(
        lambda row:
            "Online" if row["referral_source"] == "User Sign Up"
            else "Offline" if row["referral_source"] == "Draft Transaction"
            else row["source_category"],
        axis=1
    )

    return df
