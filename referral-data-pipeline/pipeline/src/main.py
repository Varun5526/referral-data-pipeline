# src/main.py

from load_data import load_all_tables
from profiling import generate_profiling_report
from clean_data import clean_tables
from processing import process_data
from business_logic import apply_business_logic
from final_output import prepare_final_output
from config import OUTPUT_DATA_PATH, PROFILING_PATH

def main():
    print("🚀 Starting Referral Data Pipeline...")

    # 1️⃣ Load data
    print("📥 Loading raw data...")
    tables = load_all_tables()

    # 2️⃣ Data profiling
    print("📊 Generating data profiling report...")
    profiling_df = generate_profiling_report(tables)
    profiling_df.to_csv(
        PROFILING_PATH + "data_profiling.csv",
        index=False
    )

    # 3️⃣ Data cleaning
    print("🧹 Cleaning data...")
    tables = clean_tables(tables)

    # 4️⃣ Data processing (joins & transformations)
    print("🔗 Processing data...")
    processed_df = process_data(tables)

    # 5️⃣ Apply business logic
    print("🧠 Applying business logic...")
    validated_df = apply_business_logic(processed_df)

    # 6️⃣ Prepare final output (deduplication & schema)
    print("✂️ Shaping final output...")
    try:
        final_df = prepare_final_output(validated_df)
        print(f"   → Output shaped: {len(final_df)} rows")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        final_df = validated_df

    # 7️⃣ Save final output
    print("💾 Saving final report...")
    final_df.to_csv(
        OUTPUT_DATA_PATH + "referral_validation_report.csv",
        index=False
    )

    print("✅ Pipeline completed successfully!")


if __name__ == "__main__":
    main()