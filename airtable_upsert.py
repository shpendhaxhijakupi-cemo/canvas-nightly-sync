#!/usr/bin/env python3
import argparse, csv, json, sys, time
from typing import List, Dict
import requests
from urllib.parse import quote

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def load_csv(path: str) -> List[Dict]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [ {k: (v if v != "" else None) for k, v in row.items()} for row in reader ]
        print("[info] CSV headers:", list(reader.fieldnames or []))
    return rows

def ensure_unique_key(rows: List[Dict], unique_field: str) -> None:
    """If unique_field missing, try to synthesize Enrollment Course Key from 3 columns."""
    if not rows:
        return
    have_field = unique_field in rows[0]
    if have_field and any(r.get(unique_field) for r in rows):
        return  # fine

    # Try to build it from Student Canvas ID + School Year + Course ID
    needed = ["Student Canvas ID", "School Year", "Course ID"]
    if all(needed[0] in rows[0] for _ in [None]) and all(k in rows[0] for k in needed):
        for r in rows:
            sid = (r.get("Student Canvas ID") or "").strip()
            year = (r.get("School Year") or "").strip()
            cid = (r.get("Course ID") or "").strip()
            if sid and year and cid:
                r[unique_field] = f"{sid}-{year}-{cid}"
        print(f"[warn] '{unique_field}' was missing/empty; synthesized from {needed}.")
    else:
        # If we cannot synthesize, fail with a clear message
        hdrs = list(rows[0].keys())
        raise SystemExit(
            f"[fatal] Unique field '{unique_field}' not found and cannot synthesize.\n"
            f"CSV columns: {hdrs}\n"
            f"Either add '{unique_field}' to your CSV or ensure the CSV has columns "
            f"'Student Canvas ID', 'School Year', and 'Course ID' so I can build it."
        )

def upsert_to_airtable(base_id: str, table_name: str, token: str,
                       csv_path: str, unique_field: str, typecast: bool):
    rows = load_csv(csv_path)
    if not rows:
        print(f"[warn] No rows in {csv_path}; nothing to upsert.")
        return

    ensure_unique_key(rows, unique_field)

    url = f"https://api.airtable.com/v0/{base_id}/{quote(table_name)}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    total, sent, errors = len(rows), 0, 0
    for i, batch in enumerate(chunked(rows, 10), start=1):
        payload = {
            "records": [{"fields": r} for r in batch],
            "performUpsert": {"fieldsToMergeOn": [unique_field]},
            "typecast": typecast,
        }
        resp = requests.patch(url, headers=headers, data=json.dumps(payload))
        if not resp.ok:
            errors += 1
            print(f"[error] Batch {i} failed: {resp.status_code} {resp.text[:500]}")
        else:
            up = len(resp.json().get("records", []))
            sent += up
            print(f"[ok] Batch {i}: upserted {up}")

        time.sleep(0.2)

    print(f"[done] Upserted {sent}/{total}. Batches with errors: {errors}")
    if errors:
        sys.exit(1)

def main():
    p = argparse.ArgumentParser(description="Upsert CSV rows to Airtable using performUpsert.")
    p.add_argument("--base", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--token", required=True)
    p.add_argument("--csv", required=True)
    p.add_argument("--unique-field", required=True)
    p.add_argument("--typecast", action="store_true")
    args = p.parse_args()

    upsert_to_airtable(
        base_id=args.base,
        table_name=args.table,
        token=args.token,
        csv_path=args.csv,
        unique_field=args.unique_field,
        typecast=args.typecast,
    )

if __name__ == "__main__":
    main()
