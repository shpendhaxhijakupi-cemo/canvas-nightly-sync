import argparse
import csv
import requests

def upsert_to_airtable(base_id, table_name, token, csv_path, unique_field):
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        records = [{"fields": row} for row in reader]

    url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Batch upload 10 at a time
    for i in range(0, len(records), 10):
        batch = {"records": records[i:i+10]}
        resp = requests.patch(url, headers=headers, json=batch)
        if not resp.ok:
            print(f"Error uploading batch {i//10 + 1}: {resp.status_code} {resp.text}")
        else:
            print(f"Uploaded batch {i//10 + 1}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upsert CSV to Airtable table.")
    parser.add_argument("--base", required=True, help="Airtable Base ID (starts with 'app')")
    parser.add_argument("--table", required=True, help="Airtable Table Name")
    parser.add_argument("--token", required=True, help="Airtable Personal Access Token (PAT)")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--unique-field", required=True, help="Unique field in Airtable to match")
    args = parser.parse_args()

    upsert_to_airtable(args.base, args.table, args.token, args.csv, args.unique_field)

