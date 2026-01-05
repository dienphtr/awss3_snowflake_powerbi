import os
import yaml
import boto3
from datetime import datetime

# ========== LOAD CONFIG ==========
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "..", "config", "upload_config.yml")

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

LOCAL_ROOT = config["local"]["raw_root"]
BUCKET = config["s3"]["bucket"]
S3_RAW_PREFIX = config["s3"]["raw_prefix"]

ADD_TIMESTAMP = config["upload"]["add_timestamp_if_exists"]
TS_FORMAT = config["upload"]["timestamp_format"]
EXT = config["upload"]["file_extension"]

# ========== AWS CLIENT ==========
s3 = boto3.client("s3")

# ========== HELPERS ==========
def s3_key_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False


def upload_file(local_file, s3_key):
    s3.upload_file(local_file, BUCKET, s3_key)
    print(f"✅ Uploaded: s3://{BUCKET}/{s3_key}")


# ========== MAIN LOGIC ==========
def run_upload():
    for layer in os.listdir(LOCAL_ROOT):
        layer_path = os.path.join(LOCAL_ROOT, layer)

        if not os.path.isdir(layer_path):
            continue

        for file in os.listdir(layer_path):
            if not file.lower().endswith(EXT):
                continue

            local_file_path = os.path.join(layer_path, file)
            base_name = file.replace(EXT, "")

            s3_key = f"{S3_RAW_PREFIX}/{layer}/{file}"

            if ADD_TIMESTAMP and s3_key_exists(BUCKET, s3_key):
                ts = datetime.now().strftime(TS_FORMAT)
                new_file = f"{base_name}_{ts}{EXT}"
                s3_key = f"{S3_RAW_PREFIX}/{layer}/{new_file}"

            upload_file(local_file_path, s3_key)


if __name__ == "__main__":
    run_upload()
