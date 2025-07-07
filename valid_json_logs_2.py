import os
import json

json_folder = 'TEST JSON FILES'
output_folder = 'test_cleaned_json_files'
log_folder = 'test_skipped_json_logs'

os.makedirs(output_folder, exist_ok=True)
os.makedirs(log_folder, exist_ok=True)
def is_suspicious_value(val):
    return any(word in val for word in ["return", "Math.", "function", "{", "}"])
def clean_item(item):
    result = {}
    for k, v in item.items():
        if not isinstance(v, dict):
            continue
        if "S" in v:
            if is_suspicious_value(v["S"]):
                return None, f"Suspicious code in value of '{k}'"
            result[k] = {"S": v["S"]}
        elif "N" in v:
            result[k] = {"S": v["N"]}
        elif "BOOL" in v:
            result[k] = {"S": str(v["BOOL"])}
        elif "NULL" in v:
            result[k] = {"S": ""}
        elif "L" in v:
            if isinstance(v["L"], list):
                values = [elem.get("S", "") for elem in v["L"] if isinstance(elem, dict)]
                result[k] = {"S": ",".join(values)}
            else:
                return None, f"Unsupported list format in field '{k}'"
        else:
            return None, f"Unsupported field type in '{k}': {v}"
    return {"Item": result}, None
# Process each file
for filename in os.listdir(json_folder):
    if filename.endswith(".json"):
        input_path = os.path.join(json_folder, filename)
        output_path = os.path.join(output_folder, filename)
        log_path = os.path.join(log_folder, filename.replace('.json', '.log'))
        skipped_count = 0
        valid_count = 0

        with open(input_path, 'r') as infile, \
            open(output_path, 'w') as outfile, \
            open(log_path, 'w') as logfile:

            for i, line in enumerate(infile, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if "Item" not in obj or not isinstance(obj["Item"], dict):
                        logfile.write(f"Line {i}: Missing or invalid 'Item' key\n")
                        skipped_count += 1
                        continue

                    cleaned, reason = clean_item(obj["Item"])
                    if cleaned:
                        json.dump(cleaned, outfile)
                        outfile.write("\n")
                        valid_count += 1
                    else:
                        logfile.write(f"Line {i}: {reason}\n")
                        skipped_count += 1

                except json.JSONDecodeError as e:
                    logfile.write(f"Line {i}: JSON decode error → {e}\n")
                    skipped_count += 1

        print(f"🧹 Cleaned: {filename} → {output_path}")
        print(f"✅ Valid records: {valid_count}")
        print(f"❌ Skipped records: {skipped_count} (see: {log_path})\n")
