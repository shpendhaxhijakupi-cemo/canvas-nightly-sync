#!/usr/bin/env python3
import argparse, csv, json, os, sys, time
from typing import List, Dict
import requests
from urllib.parse import quote

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def load_csv(path: str) -> List[Dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert empty strings to None so Airtable can clear/ignore
            cleaned = {k: (v if v not in ("", None) else None) for k, v in row.items()}
            rows.append(cleaned)
    return rows

def upsert_to_airtable(base_id: str, table_name: str, token: str,
                       csv_path: str, unique_field: str, typecast: bool):
    rows = load_csv(csv_path)
    if not rows:
        print(f"[warn] No rows found in {csv_path}; nothing to upsert.")
        return

    url = f"https://api.airtable.com/v0/{base_id}/{quote(table_name)}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    batch_size = 10
    total = len(rows)
    sent = 0
    errors = 0

    for i, batch_rows in enumerate(chunked(rows, batch_size), start=1):
        payload = {
            "records": [{"fields": r} for r in batch_rows],
            "performUpsert": {"fieldsToMergeOn": [unique_field]},
            "typecast": typecast,
        }
        resp = requests.patch(url, headers=headers, data=json.dumps(payload))
        if not resp.ok:
            errors += 1
            snippet = resp.text[:500]
            print(f"[error] Batch {i} failed: {resp.status_code} {snippet}")
        else:
            data = resp.json()
            count = len(data.get("records", []))
            sent += count
            print(f"[ok] Batch {i}: upserted {count} record(s)")

        # small backoff for rate limits
        time.sleep(0.2)

    print(f"[done] Upserted {sent}/{total} rows. Batches with errors: {errors}")
    if errors:
        sys.exit(1)

def main():
    p = argparse.ArgumentParser(description="Upsert CSV rows into Airtable using performUpsert.")
    p.add_argument("--base", required=True, help="Airtable Base ID (app...)")
    p.add_argument("--table", required=True, help="Airtable Table name (can contain spaces)")
    p.add_argument("--token", required=True, help="Airtable Personal Access Token (PAT)")
    p.add_argument("--csv", required=True, help="Path to CSV file to upsert")
    p.add_argument("--unique-field", required=True, help="Field to merge on (e.g., 'Enrollment Course Key')")
    p.add_argument("--typecast", action="store_true", help="Let Airtable coerce values to field types")
    args = p.parse_args()

    upsert_to_airtable(
        base_id=args.base,
        table_name=args.table,
        token=args.token,
        csv_path=args.csv,
        unique_field=args.unique_field,
        typecast=bool(args.typecast),
    )

if __name__ == "__main__":
    main()
