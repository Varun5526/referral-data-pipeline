# src/final_output.py

import pandas as pd

def prepare_final_output(df):
    """
    Deduplicates referrals, fixes schema, removes NULLs, ensures 22 columns.
    """

    # Rename status column
    df = df.rename(columns={
        "description": "referral_status",
        "name": "referrer_name",
        "phone_number": "referrer_phone_number",
        "homeclub": "referrer_homeclub"
    })

    # Sort and deduplicate (one row per referral)
    df = df.sort_values(
        by=["referral_id", "created_at"],
        ascending=[True, False]
    )

    df = df.drop_duplicates(
        subset=["referral_id"],
        keep="first"
    )

    # Create required columns
    df["referral_details_id"] = range(1, len(df) + 1)
    df["num_reward_days"] = 30
    df["reward_granted_at"] = df["created_at"]
    
    # Add missing updated_at column (22-column requirement)
    if "updated_at" not in df.columns:
        df["updated_at"] = df["referral_at"]

    # Final column order (EXACT 22-column schema)
    final_columns = [
        "referral_details_id",
        "referral_id",
        "referral_source",
        "referral_source_category",
        "referral_at",
        "referrer_id",
        "referrer_name",
        "referrer_phone_number",
        "referrer_homeclub",
        "referee_id",
        "referee_name",
        "referee_phone",
        "referral_status",
        "num_reward_days",
        "transaction_id",
        "transaction_status",
        "transaction_at",
        "transaction_location",
        "transaction_type",
        "updated_at",
        "reward_granted_at",
        "is_business_logic_valid"
    ]

    final_df = df[final_columns].copy()

    # Clean timestamps (Remove timezone offsets and microseconds)
    timestamp_cols = ['referral_at', 'transaction_at', 'updated_at', 'reward_granted_at']
    for col in timestamp_cols:
        if col in final_df.columns:
            final_df[col] = pd.to_datetime(final_df[col], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')

    # 🔥 NO NULL VALUES RULE - Comprehensive fill
    fill_values = {
        'referral_source': 'Not Available',
        'referral_source_category': 'Online',
        'referrer_id': 'Not Available',
        'referrer_name': 'Unknown User',
        'referrer_phone_number': 'Not Available',
        'referrer_homeclub': 'Not Available',
        'referee_id': 'Not Available',
        'referee_name': 'Unknown Lead',
        'referee_phone': 'Not Available',
        'referral_status': 'Unknown',
        'transaction_id': 'Not Available',
        'transaction_status': 'Not Available',
        'transaction_at': '1900-01-01 00:00:00',
        'transaction_location': 'Not Available',
        'transaction_type': 'Not Available',
        'referral_at': '1900-01-01 00:00:00',
        'updated_at': '1900-01-01 00:00:00',
        'reward_granted_at': '1900-01-01 00:00:00'
    }
    
    final_df = final_df.fillna(value=fill_values)
    
    # Additional sweep: convert all remaining NaN to appropriate strings
    for col in final_df.columns:
        if final_df[col].dtype == 'object':
            final_df[col] = final_df[col].fillna('Not Available')
            # Convert to string to ensure no NaN leaks through
            final_df[col] = final_df[col].astype(str).replace('nan', 'Not Available')
    
    # Ensure numeric columns are properly filled
    final_df['referral_details_id'] = final_df['referral_details_id'].fillna(0).astype(int)
    final_df['num_reward_days'] = final_df['num_reward_days'].fillna(0).astype(int)
    
    # Ensure boolean column
    final_df['is_business_logic_valid'] = final_df['is_business_logic_valid'].fillna(False)

    return final_df


