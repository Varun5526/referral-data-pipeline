# src/business_logic.py

import pandas as pd


def apply_business_logic(df):
    """
    Applies business rules to detect valid / invalid referral rewards.
    """

    # Convert dates to datetime
    date_columns = [
        "referral_at",
        "transaction_at",
        "membership_expired_date",
        "created_at"
    ]

    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    def _strip_tz(series):
        try:
            return series.dt.tz_localize(None)
        except Exception:
            return series

    # Convenience handles when optional columns are missing and strip timezone info for comparisons
    referral_at = _strip_tz(df["referral_at"]) if "referral_at" in df else pd.Series(pd.NaT, index=df.index)
    transaction_at = _strip_tz(df["transaction_at"]) if "transaction_at" in df else pd.Series(pd.NaT, index=df.index)
    membership_expired_date = _strip_tz(df["membership_expired_date"]) if "membership_expired_date" in df else pd.Series(pd.NaT, index=df.index)

    # Ensure numeric comparison on reward_value (strip text like "20 days")
    reward_value_num = pd.Series(0, index=df.index)
    if "reward_value" in df.columns:
        extracted = df["reward_value"].astype(str).str.extract(r"([-\d\.]+)")[0]
        reward_value_num = pd.to_numeric(extracted, errors="coerce")
        df["reward_value"] = reward_value_num

    # -------------------------------
    # CONDITION 1: VALID - Successful
    # -------------------------------
    is_deleted = df["is_deleted"] if "is_deleted" in df else pd.Series(False, index=df.index)
    is_reward_granted = df["is_reward_granted"] if "is_reward_granted" in df else pd.Series(False, index=df.index)

    condition_valid_success = (
        (reward_value_num > 0) &
        (df["description"] == "Berhasil") &
        (df["transaction_id"].notna()) &
        (df["transaction_status"] == "PAID") &
        (df["transaction_type"] == "NEW") &
        (transaction_at > referral_at) &
        (transaction_at.dt.to_period("M") == referral_at.dt.to_period("M")) &
        (membership_expired_date > referral_at) &
        (is_deleted == False) &
        (is_reward_granted == True)
    )

    # ---------------------------------
    # CONDITION 2: VALID - Pending/Fail
    # ---------------------------------
    condition_valid_pending = (
        df["description"].isin(["Menunggu", "Tidak Berhasil"]) &
        (df["reward_value"].isna())
    )

    # Final decision
    df["is_business_logic_valid"] = (
        condition_valid_success | condition_valid_pending
    )

    return df
