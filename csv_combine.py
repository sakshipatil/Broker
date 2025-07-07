import os
import pandas as pd
from datetime import datetime
# Input/output paths
input_dir = 'PVS CSV FILES'
# Generate timestamped filename
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = f'PVS CSV FILES/IBOR_MIG_LEAD_InitLd_output_{timestamp}.csv'
# List to hold DataFrames
all_dataframes = []
# Read and append each CSV
for filename in os.listdir(input_dir):
    if filename.endswith('.csv') and not filename.startswith('IBOR_MIG_LEAD_InitLd_output'):
        file_path = os.path.join(input_dir, filename)
        df = pd.read_csv(file_path, dtype=str)  # Read as string to preserve formatting
        all_dataframes.append(df)
# Combine all CSVs
combined_df = pd.concat(all_dataframes, ignore_index=True)

# Add 'last_trigger_ts' column if missing
if 'last_trigger_ts' not in combined_df.columns:
    combined_df['last_trigger_ts'] = ''
# Desired column order (based on your image)
ordered_columns = [
    "portalNm_ssoId",
    "created_ts",
    "dob",
    "emp_id",
    "frst_nm",
    "last_login",
    "last_nm",
    "last_trigger_ts",
    "last_updated_by",
    "last_updated_ts",
    "mfaEnrolledStatus",
    "okta_uid",
    "portalNm_regId",
    "prv_login",
    "role",
    "security_ans_plain",
    "security_answer",
    "security_question",
    "ssoId_status",
    "usr_agrmnt_status",
    "usr_agrmnt_vs"
]
# Columns to remove
columns_to_drop = [
    'dateOfBirth',
    'firstName',
    'frst_nm;',
    'lastName',
    'last_updated',
    'regId',
    'requestedBy',
    'sso_Id',
    'user_agreement_status',
    'user_agreement_vs',
    'usr_agrmnt_ts', 
    'dup_email',
    'lastUpdateBy',
    'security_ans',
    'claimedEmailAddress',
    'sharedSsoId',
    'trigger'
]
# Drop the columns if they exist
combined_df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

# Reorder columns (and keep any remaining at the end)
existing_cols = [col for col in ordered_columns if col in combined_df.columns]
remaining_cols = [col for col in combined_df.columns if col not in ordered_columns]
combined_df = combined_df[existing_cols + remaining_cols]

# Save the final CSV
combined_df.to_csv(output_file, index=False)

print(f"✅ Combined CSV written to: {output_file}")