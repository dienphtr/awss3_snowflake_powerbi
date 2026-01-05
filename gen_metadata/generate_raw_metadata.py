import os
import csv
import yaml
import pandas as pd
from pandas.errors import EmptyDataError

# ================= CONFIG =================
LOCAL_ROOT = r"C:\Users\no1pr\Documents\000_Code\awss3_snowflake_powerbi\csv_export\adventureworks_data\raw"
DATABASE_NAME = "ADVENTUREWORKS_RAW"
SNOWFLAKE_STAGE = "@ADVENTUREWORKS_RAW.CONTROL.AW_RAW_S3_STAGE"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "RAW_TABLE_METADATA.csv")
PK_CONFIG_FILE = os.path.join(SCRIPT_DIR, "manual_pk_config.yml")

SAMPLE_ROWS = 5
# =========================================


# ---------- Helpers ----------
def infer_datatype(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "NUMBER"
    if pd.api.types.is_float_dtype(series):
        return "FLOAT"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP_NTZ"
    return "VARCHAR"


def print_sample(df: pd.DataFrame, n=SAMPLE_ROWS):
    print("\n📊 Sample data:")
    print(df.sample(min(n, len(df)), random_state=42).to_string(index=False))


# ---------- Load manual PK config ----------
if os.path.exists(PK_CONFIG_FILE):
    with open(PK_CONFIG_FILE, "r", encoding="utf-8") as f:
        manual_pk_config = yaml.safe_load(f) or {}
else:
    manual_pk_config = {}


rows = []

print("🔍 Scanning root:", LOCAL_ROOT)

# ============================================================
# MAIN SCAN
# ============================================================
for root, _, files in os.walk(LOCAL_ROOT):
    for file in files:
        if not file.lower().endswith(".csv"):
            continue

        full_path = os.path.join(root, file)

        # -------- Skip empty file --------
        if os.path.getsize(full_path) == 0:
            print(f"⚠️  Skipping empty file: {file}")
            continue

        try:
            df = pd.read_csv(full_path)
        except EmptyDataError:
            print(f"⚠️  No columns in file: {file}")
            continue
        except Exception as e:
            print(f"❌ Error reading {file}: {e}")
            continue

        if df.columns.empty:
            print(f"⚠️  No header in file: {file}")
            continue

        # -------- Resolve schema / table --------
        relative_path = os.path.relpath(full_path, LOCAL_ROOT).replace("\\", "/")
        schema_name = relative_path.split("/")[0].upper()
        table_name = os.path.splitext(file)[0].upper()
        stage_path = f"{SNOWFLAKE_STAGE}/{schema_name.lower()}"

        table_key = f"{schema_name}.{table_name}"
        print(f"\n✅ Processing {table_key}")

        # ========================================================
        # 1. AUTO PK DETECTION (column name contains ID)
        # ========================================================
        auto_pk_cols = [c for c in df.columns if "ID" in c.upper()]
        pk_cols = []
        pk_source = "AUTO"

        # ========================================================
        # 2. MANUAL PK FALLBACK
        # ========================================================
        if not auto_pk_cols:
            if table_key in manual_pk_config:
                pk_cols = manual_pk_config[table_key]
                pk_source = "MANUAL_CONFIG"
                print(f"ℹ️  Using manual PK from config: {pk_cols}")
            else:
                print("\n" + "=" * 80)
                print(f"⚠️  NO ID COLUMN FOUND: {table_key}")
                print(f"📄 Columns: {list(df.columns)}")
                print_sample(df)

                user_input = input("👉 Enter PK columns (comma-separated, ENTER to skip): ").strip()
                if user_input:
                    pk_cols = [c.strip() for c in user_input.split(",")]
                    pk_source = "MANUAL_INPUT"
                    manual_pk_config[table_key] = pk_cols
                    print(f"✅ Manual PK accepted: {pk_cols}")
                else:
                    print("🚫 No PK defined.")
        else:
            pk_cols = auto_pk_cols

        # ========================================================
        # 3. PK DATA QUALITY CHECK
        # ========================================================
        pk_quality = "OK"
        has_pk = bool(pk_cols)

        if not has_pk:
            pk_quality = "NO_PK"
        else:
            # --- PK column existence ---
            missing_cols = [c for c in pk_cols if c not in df.columns]
            if missing_cols:
                pk_quality = "INVALID_PK_COLUMN"
                print(f"❌ Invalid PK columns: {missing_cols}")
            else:
                # --- NULL check (IMPORTANT) ---
                null_cnt = df[pk_cols].isnull().any(axis=1).sum()
                if null_cnt > 0:
                    pk_quality = "PK_CONTAINS_NULL"
                    print(f"❌ PK contains NULL values ({null_cnt} rows)")
                else:
                    # --- Duplicate check ---
                    dup = (
                        df[pk_cols]
                        .groupby(pk_cols)
                        .size()
                        .reset_index(name="cnt")
                        .query("cnt > 1")
                    )
                    if not dup.empty:
                        pk_quality = "DUPLICATE_PK"
                        print(f"❌ PK not distinct ({len(dup)} duplicate keys)")
                    else:
                        print("✅ PK quality check passed")

        # ========================================================
        # 4. WRITE COLUMN-LEVEL METADATA
        # ========================================================
        pk_counter = 0
        for idx, col in enumerate(df.columns, start=1):
            col_upper = col.upper()
            is_pk = col in pk_cols and pk_quality == "OK"

            pk_ordinal_position = None
            if is_pk:
                pk_counter += 1
                pk_ordinal_position = pk_counter

            rows.append({
                "database_name": DATABASE_NAME,
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": col_upper,
                "data_type": infer_datatype(df[col]),
                "ordinal_position": idx,
                "is_primary_key": is_pk,
                "pk_ordinal_position": pk_ordinal_position,
                "pk_source": pk_source,
                "pk_quality": pk_quality,
                "file_name": file,
                "relative_path": relative_path,
                "stage_path": stage_path
            })


# ============================================================
# SAVE OUTPUTS
# ============================================================
if not rows:
    raise RuntimeError("❌ No metadata generated.")

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"\n🎉 Metadata written to {OUTPUT_FILE}")

with open(PK_CONFIG_FILE, "w", encoding="utf-8") as f:
    yaml.dump(manual_pk_config, f, sort_keys=True)

print(f"💾 Manual PK config saved to {PK_CONFIG_FILE}")
