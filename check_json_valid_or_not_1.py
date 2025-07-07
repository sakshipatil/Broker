import os
import json
json_folder = "TEST JSON FILES"  # folder containing your .json files

def is_valid_dynamodb_item_verbose(obj, line_num, filename):
    if "Item" not in obj:
        print(f"❌ Line {line_num} in {filename}: Missing 'Item' key")
        return False
    item = obj["Item"]
    if not isinstance(item, dict):
        print(f"❌ Line {line_num} in {filename}: 'Item' is not a dictionary")
        return False

    for k, v in item.items():
        if "(" in k or ")" in k:
            print(f"❌ Line {line_num} in {filename}: Suspicious key with parentheses: '{k}'")
            return False
        if not isinstance(v, dict):
            print(f"❌ Line {line_num} in {filename}: Field '{k}' is not a dict (got: {type(v)})")
            return False
        if not any(t in v for t in ["S", "N", "BOOL", "NULL"]):
            print(f"❌ Line {line_num} in {filename}: Field '{k}' missing expected type key (got: {v})")
            return False
        # Optional: detect JS code-like values
        if "S" in v and any(word in v["S"] for word in ["return", "Math.", "function", "{", "}"]):
            print(f"❌ Line {line_num} in {filename}: Suspicious value (code?): '{v['S']}'")
            return False
    return True

def validate_json_file(filepath):
    all_valid = True
    with open(filepath, 'r') as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if not is_valid_dynamodb_item_verbose(obj, i, os.path.basename(filepath)):
                    all_valid = False
            except json.JSONDecodeError as e:
                print(f"❌ JSON decode error at line {i} in {os.path.basename(filepath)}: {e}")
                all_valid = False
    if all_valid:
        print(f"✅ {os.path.basename(filepath)} is valid and clean\n")
    else:
        print(f"❌ {os.path.basename(filepath)} has structural issues\n")

# Run validator on all .json files in the folder
for filename in os.listdir(json_folder):
    if filename.endswith(".json"):
        full_path = os.path.join(json_folder, filename)
        validate_json_file(full_path)