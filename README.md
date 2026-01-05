# awss3_snowflake_powerbi

A small project that demonstrates a complete flow for the AdventureWorks sample dataset:

- Source CSV files stored on Amazon S3
- Ingestion and processing with Snowflake
- Visualization with Power BI

---

## Prerequisites ✅

- **Python** 3.9 or newer (`python --version`)
- **pipx** (optional but recommended):

```powershell
pip install pipx
pipx ensurepath
```

- **AWS account** and an IAM user or role with S3 access
- **AWS CLI** installed and configured (`aws --version`, `aws configure`)
- **Snowflake** account with privileges to create storage integrations and stages

> The `csv_export/` folder contains PowerShell scripts to export and organize CSVs used by this project.

## Uploading CSVs to S3 (example, Windows)

Use the AWS CLI to sync local folders to S3. Example commands (PowerShell or CMD):

```powershell
aws s3 sync "C:\Users\<USER>\Documents\000_Code\awss3_snowflake_powerbi\csv_export\AdventureWorks_CSV_SAFE\dim" s3://adventureworks-mockup-dienpt7/raw/dim
aws s3 sync "C:\Users\<USER>\Documents\000_Code\awss3_snowflake_powerbi\csv_export\AdventureWorks_CSV_SAFE\fact" s3://adventureworks-mockup-dienpt7/raw/fact
aws s3 sync "C:\Users\<USER>\Documents\000_Code\awss3_snowflake_powerbi\csv_export\AdventureWorks_CSV_SAFE\lookup" s3://adventureworks-mockup-dienpt7/raw/lookup
```

Quick checks:

```powershell
aws s3 ls s3://adventureworks-mockup-dienpt7/adventureworks-data/raw/
aws s3 ls s3://adventureworks-mockup-dienpt7/adventureworks-data/raw/dim/
```

```powershell
aws s3 cp s3://adventureworks-mockup-dienpt7/adventureworks-data/raw/dim/Production_UnitMeasure.csv C:\temp\test.csv
```

## Snowflake: create a STORAGE INTEGRATION for S3 🔒

Create an external storage integration in Snowflake (example):

```sql
CREATE OR REPLACE STORAGE INTEGRATION s3_adventureworks_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<REDACTED_ACCOUNT_ID>:role/REDACTED'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://adventureworks-mockup-dienpt7/adventureworks-data/raw'
  );
```

Notes:
- Prefer using an **IAM role** for Snowflake (more secure) and set the trust policy to allow Snowflake's account to assume the role.
- After creating the integration, obtain Snowflake's `STORAGE_AWS_EXTERNAL_ID` (if applicable) and configure the role's trust relationship accordingly.

---

## Tips & Notes 💡

- Keep S3 buckets private and follow least-privilege principles. Do not store AWS credentials in source control.
- Use `upload to bucket/upload_to_s3.py` for Python-based uploads or the `csv_export/` PowerShell scripts for Windows.






