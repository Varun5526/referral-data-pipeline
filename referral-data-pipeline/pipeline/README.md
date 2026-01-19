# Referral Data Pipeline – Take Home Test

## 📌 Overview
This project implements an end-to-end **data profiling and validation pipeline**
for a user referral program.  
The goal is to identify whether referral rewards are **valid or potentially fraudulent**
based on defined business rules.

The pipeline:
- Loads multiple CSV data sources
- Profiles data quality
- Cleans and transforms data
- Applies business validation logic
- Produces a final validated report

---

## � Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        REFERRAL DATA PIPELINE                    │
└─────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────────────┐    │  ORCHESTRATOR: main.py                           │
    │  Executes all steps below in sequence            │
    └────────────────┬─────────────────────────────────┘
                     │
                     ▼
    ┌──────────────────────────────────────────────────┐    │  STEP 1: Load Raw Data (load_data.py)            │
    │  • Lead logs (8 rows)                            │
    │  • User referrals (46 rows)                      │
    │  • User referral logs (96 rows)                  │
    │  • User logs (29 rows)                           │
    │  • User referral statuses (3 rows)               │
    │  • Referral rewards (3 rows)                     │
    │  • Paid transactions (14 rows)                   │
    └────────────────┬─────────────────────────────────┘
                     │
                     ▼
    ┌──────────────────────────────────────────────────┐
    │  STEP 2: Data Profiling (profiling.py)           │
    │  • Generate null count statistics                │
    │  • Calculate distinct value counts               │
    │  • Save profiling report                         │
    └────────────────┬─────────────────────────────────┘
                     │
                     ▼
    ┌──────────────────────────────────────────────────┐
    │  STEP 3: Data Cleaning (clean_data.py)           │
    │  • Remove duplicate rows                         │
    │  • Remove fully empty rows                       │
    └────────────────┬─────────────────────────────────┘
                     │
                     ▼
    ┌──────────────────────────────────────────────────┐
    │  STEP 4: Data Processing (processing.py)         │
    │  • Join referrals with logs                      │
    │  • Join with status & rewards                    │
    │  • Join with transactions                        │
    │  • Join with leads for categorization            │
    │  • Create referral_source_category               │
    │  Result: 76 rows (multiple logs per referral)    │
    └────────────────┬─────────────────────────────────┘
                     │
                     ▼
    ┌──────────────────────────────────────────────────┐
    │  STEP 5: Business Logic (business_logic.py)      │
    │  • Parse reward values                           │
    │  • Validate successful referrals                 │
    │  • Validate pending/failed referrals             │
    │  • Flag valid vs invalid rewards                 │
    └────────────────┬─────────────────────────────────┘
                     │
                     ▼
    ┌──────────────────────────────────────────────────┐
    │  STEP 6: Final Output (final_output.py)          │
    │  • Sort by referral_id and created_at            │
    │  • Keep latest record per referral               │
    │  • Select required columns (19 cols)             │
    │  Result: 46 rows (35 valid, 11 invalid)          │
    └────────────────┬─────────────────────────────────┘
                     │
                     ▼
    ┌──────────────────────────────────────────────────┐
    │  OUTPUT: referral_validation_report.csv          │
    │  ✅ 46 rows × 19 columns                        │
    │  ✅ Fraud detection flag included               │
    └──────────────────────────────────────────────────┘
```

---

## �🗂 Project Structure

```
referral-data-pipeline/
│
├── data/
│   ├── raw/                          # Input CSV files
│   └── output/                       # Final output CSV
│
├── profiling/
│   └── data_profiling.csv            # Null & distinct count profiling
│
├── src/
│   ├── config.py                     # Configuration & paths
│   ├── load_data.py                  # Load CSV files
│   ├── profiling.py                  # Data profiling logic
│   ├── clean_data.py                 # Basic data cleaning
│   ├── processing.py                 # Joins & transformations
│   ├── business_logic.py             # Referral validation rules
│   ├── final_output.py               # Deduplication & column selection
│   └── main.py                       # Pipeline orchestration
│
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## ⚙️ Requirements

- Python 3.9+
- Pandas
- pytz
- python-dateutil

Install dependencies:
```bash
pip install -r requirements.txt
```

---

## ▶️ How to Run the Pipeline

From the project root directory:

```bash
python src/main.py
```

---

## 📊 Outputs

### 1️⃣ Data Profiling Report
`profiling/data_profiling.csv`

Contains:
- Table name
- Column name
- Null count
- Distinct value count

### 2️⃣ Final Referral Validation Report
`data/output/referral_validation_report.csv`

Output Characteristics:
- ✅ 46 rows
- ✅ One row per referral
- ✅ Business logic validation applied
- ✅ Fraud detection flag included

---

## 🧠 Business Logic Summary

A referral reward is considered **VALID** if:

**CONDITION 1: Successful Referral**
- Reward value > 0
- Referral status is "Berhasil"
- Transaction exists and is PAID
- Transaction type is NEW
- Transaction occurred after referral creation
- Transaction occurred in the same month
- Referrer membership is active
- Referrer account is not deleted
- Reward has been granted

**CONDITION 2: Pending/Failed Referral**
- Referral status is "Menunggu" or "Tidak Berhasil"
- No reward value is assigned

All other cases are marked as **INVALID**.

---

## 🐳 Docker Usage

### Build Image
```bash
docker build -t referral-pipeline .
```

### Run Container
```bash
docker run -v $(pwd)/data/output:/app/data/output referral-pipeline
```

The final report will be available on the host machine.

---

## 📋 Data Dictionary

| Column Name | Data Type | Description |
|-----------|----------|-------------|
| id | Integer | Unique identifier of referral log |
| referral_id | String | Unique referral identifier |
| referral_source | String | Source of referral |
| referral_source_category | String | Online / Offline / Lead source |
| referral_at | Datetime | Referral creation time |
| referrer_id | String | Referring user ID |
| referee_id | String | Referred user ID |
| referee_name | String | Referred user name |
| referee_phone | String | Referred user phone |
| description | String | Referral status (Berhasil/Menunggu/Tidak Berhasil) |
| reward_value | Integer | Reward amount in days |
| transaction_id | String | Transaction ID |
| transaction_status | String | PAID / PENDING |
| transaction_at | Datetime | Transaction time |
| transaction_location | String | Transaction location |
| transaction_type | String | NEW / RENEWAL |
| updated_at | Datetime | Last update timestamp |
| created_at | Datetime | Reward granted timestamp |
| is_business_logic_valid | Boolean | Fraud validation flag (True/False) |

---

## ✅ Final Status

The pipeline successfully produces the expected output as per the assignment requirements and follows clean, modular, and production-ready design principles.

**Design Highlights:**
- Modular ETL architecture
- Deterministic deduplication (latest record per referral)
- Business-rule-driven fraud detection
- Comprehensive data profiling
- Docker containerization for reproducibility

---


