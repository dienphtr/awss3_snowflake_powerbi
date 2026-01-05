# awss3\_snowflake\_powerbi

\- Build a full flow for sample AdventureWork database from archieve on S3 , data process by snowflake and data visual by powerbi
- python --version > 3.9
- pip install pipx
- pipx ensurepath

\- use csv\_export folder powershell to export and organize db csv

\- create bucket s3: bucket name - aws region - bucket type (general purpose) - object ownership (ACLs disabled) - Block public Access (All) - Encryption (...SSE-S3) - Create bucket - Create folder

\- IAM - Users - Create user - user name - AmazonS3FullAccess policy - security credentials - create access key - CLI - save csv key

\- Download AWS CLI - check by (cmd: aws --version) - cmd: aws configure (2 key + default region of s3 bucket + output = json) - test by cmd: aws s3 ls - Upload folder: 

&nbsp;    - aws s3 sync C:\\Users\\no1pr\\Documents\\000\_Code\\awss3\_snowflake\_powerbi\\csv\_export\\AdventureWorks\_CSV\_SAFE\\dim s3://adventureworks-mockup-dienpt7/raw/dim

&nbsp;    - aws s3 sync C:\\Users\\no1pr\\Documents\\000\_Code\\awss3\_snowflake\_powerbi\\csv\_export\\AdventureWorks\_CSV\_SAFE\\fact s3://adventureworks-mockup-dienpt7/raw/fact

&nbsp;    - aws s3 sync C:\\Users\\no1pr\\Documents\\000\_Code\\awss3\_snowflake\_powerbi\\csv\_export\\AdventureWorks\_CSV\_SAFE\\lookup s3://adventureworks-mockup-dienpt7/raw/lookup

\- C:\\Users\\no1pr>aws s3 ls s3://adventureworks-mockup-dienpt7/adventureworks-data/raw/

\- C:\\Users\\no1pr>aws s3 ls s3://adventureworks-mockup-dienpt7/adventureworks-data/raw/dim/

\- C:\\Users\\no1pr>aws s3 cp s3://adventureworks-mockup-dienpt7/adventureworks-data/raw/dim/Production\_UnitMeasure.csv C:\\temp\\test.csv

download: s3://adventureworks-mockup-dienpt7/adventureworks-data/raw/dim/Production\_UnitMeasure.csv to ..\\..\\temp\\test.csv

\- GitHub - ACtion - create ....
- Snowflake query create integration
 CREATE OR REPLACE STORAGE INTEGRATION s3_adventureworks_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::593857178186:role/snowflake_s3_role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://adventureworks-mockup-dienpt7/adventureworks-data/raw'
  );
- STORAGE_AWS_IAM_USER_ARN	String	arn:aws:iam::194317476460:user/6i9d1000-s
STORAGE_AWS_ROLE_ARN	String	arn:aws:iam::593857178186:role/snowflake_s3_role
STORAGE_AWS_EXTERNAL_ID	String	PQB81735_SFCRole=2_5T/4y0t1/G2AM0G5VaxhTJF+Tho=

- 





