import os
import io
import csv
import json
import pandas as pd
from datetime import datetime

base_dir = os.getcwd()

input_folder = "DEV_cleaned_json_files"
output_folder = "dev_cleaned_csv_files"
log_folder = "dev_csv_validation_logs"
skipped_folder = os.path.join(output_folder, "csv_skipped_records")

os.makedirs(output_folder, exist_ok=True)
os.makedirs(log_folder, exist_ok=True)
os.makedirs(skipped_folder, exist_ok=True)

# Target column order
ordered_columns = [
    "portalNm_ssoId", "created_ts", "dob", "emp_id", "frst_nm", "last_login",
    "last_nm", "last_trigger_ts", "last_updated_by", "last_updated_ts",
    "mfaEnrolledStatus", "okta_uid", "portalNm_regId", "prv_login", "role",
    "security_ans_plain", "security_answer", "security_question",
    "ssoId_status", "usr_agrmnt_status", "usr_agrmnt_vs"
]
# Load and filter records
combined_records = []
skipped_records = []

for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        path = os.path.join(input_folder, filename)
        with open(path, 'r') as f:
            for line in f:
                try:
                    obj = json.loads(line.strip())
                    item = obj.get("Item", {})
                    record = {}
                    for col in ordered_columns:
                        val = item.get(col, {}).get("S", "").strip().strip('"').strip("'")
                        if col == "usr_agrmnt_vs" and val == "":
                            val = '""'  # Preserve as quoted empty string in CSV
                        record[col] = val
                    # Check if portalNm_ssoId is valid
                    if not (record["portalNm_ssoId"].startswith("mc_") or record["portalNm_ssoId"].startswith("evn_")):
                        skipped_records.append(record)
                    else:
                        combined_records.append(record)
                except Exception as e:
                    print(f"Failed to process line in {filename}: {e}")
                    print(f"Finished reading all records. Total valid: {len(combined_records)}, Skipped: {len(skipped_records)}")
# Sanitize and normalize all values
for record in combined_records + skipped_records:
    for key, val in record.items():
        if val is None:
            record[key] = ""
        elif isinstance(val, (int, float)):
            record[key] = str(val)
        elif isinstance(val, str):
            record[key] = val.strip()
# Create cleaned CSV
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_filename = f"IBOR_MIG_LEAD_InitLd_{timestamp}.csv"
csv_path = os.path.join(output_folder, csv_filename)
df = pd.DataFrame(combined_records, columns=ordered_columns)
df.to_csv(csv_path, index=False, quoting=csv.QUOTE_ALL)
print(f"Combined CSV created: {csv_path}")

# Write skipped records to CSV
if skipped_records:
    skipped_csv_filename = f"skipped_portalNm_ssoId_{timestamp}.csv"
    skipped_path = os.path.join(skipped_folder, skipped_csv_filename)
    skipped_df = pd.DataFrame(skipped_records, columns=ordered_columns)
    skipped_df.to_csv(skipped_path, index=False, quoting=csv.QUOTE_ALL)
    print(f"Skipped rows written to: {skipped_path}")

# Validation Function
def validate_csv_structure(csv_path, expected_columns):
    errors = []
    invalid_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        lines = list(reader)
    header = lines[0]
    if header != expected_columns:
        errors.append("Column headers are not in the correct order.")

    for idx, cols in enumerate(lines[1:], start=2):
        if len(cols) != len(expected_columns):
            errors.append(f"Line {idx}: Expected {len(expected_columns)} columns, found {len(cols)}")
            continue
    
        portalNm_ssoId = cols[0].strip()
        if not (portalNm_ssoId.startswith("mc_") or portalNm_ssoId.startswith("evn_")):
            msg = f"Line {idx}: portalNm_ssoId does not start with 'mc_' or 'evn_' â†’ {portalNm_ssoId}"
            errors.append(msg)
            invalid_rows.append(cols)
        elif cols[-1] == "":
            errors.append(f"Line {idx}: 'usr_agrmnt_vs' is empty")

    # Write skipped invalid rows (if any new ones are found)
    if invalid_rows:
        skipped_path = os.path.join(skipped_folder, f"skipped_portalNm_ssoId_{timestamp}_validation.csv")
        with open(skipped_path, 'w', encoding='utf-8', newline='') as skipped_file:
            writer = csv.writer(skipped_file)
            writer.writerow(expected_columns)
            for row in invalid_rows:
                writer.writerow(row)
        print(f"Additional skipped rows from validation written to: {skipped_path}")

    # Print results
    if not errors:
        print("CSV is well-structured and clean.")
    else:
        print("CSV validation failed:")
        for err in errors[:10]:
            print(err)
        print(f"Total issues found: {len(errors)}")

        # Save validation log
        log_filename = csv_filename.replace(".csv", ".log")
        log_path = os.path.join(log_folder, log_filename)
        with open(log_path, 'w', encoding='utf-8') as log_file:
            for err in errors:
                log_file.write(err + "\n")
        print(f"Validation errors saved to: {log_path}")

    # Check for duplicate portalNm_ssoId
    df = pd.read_csv(csv_path)
    duplicates = df[df.duplicated('portalNm_ssoId', keep=False)]
    if not duplicates.empty:
        print(f"Found duplicate portalNm_ssoId values:")
        print(duplicates[['portalNm_ssoId']].drop_duplicates())
        print(f"Total duplicates: {len(duplicates)}")
    else:
        print("All portalNm_ssoId values are unique.")

# Final structural check
def final_line_integrity_check(csv_path, expected_columns):
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        lines = list(reader)
    issues = []
    for idx, cols in enumerate(lines[1:], start=2):  # Skip header
        if len(cols) != len(expected_columns):
            issues.append(f"Line {idx}: Incorrect number of columns ({len(cols)})")
        elif not (cols[0].startswith("mc_") or cols[0].startswith("evn_")):
                issues.append(f"Line {idx}: Invalid 'portalNm_ssoId': {cols[0]}")
    if not issues:
        print("Final integrity check passed. Each line is correctly structured.")
    else:
        print("Final integrity check failed:")
        for issue in issues[:10]:
            print(issue)
        print(f"Total structure issues found: {len(issues)}")

# Run validation
validate_csv_structure(csv_path, ordered_columns)
# Final structural check
final_line_integrity_check(csv_path, ordered_columns)