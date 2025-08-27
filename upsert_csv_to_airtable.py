#!/usr/bin/env python3
import os, csv, requests, time, sys, json
from urllib.parse import quote

"""
Generic CSV → Airtable upsert
- Uses a unique key column present in the CSV to decide update vs create.
- Optional soft-delete: mark missing records as Active=False if AIRTABLE_SOFT_DELETE=true
  and the table has an 'Active' checkbox.

Env:
  AIRTABLE_PAT
  AIRTABLE_BASE_ID
  AIRTABLE_TABLE_NAME
  CSV_PATH
  UNIQUE_KEY
  AIRTABLE_TYPECAST     (optional: 'true'/'false'; default true)
  AIRTABLE_SOFT_DELETE  (optional: 'true'/'false'; default false)
"""

def batched(lst, n=10):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def main():
    base  = os.environ["AIRTABLE_BASE_ID"]
    table = os.environ["AIRTABLE_TABLE_NAME"]
    token = os.environ["AIRTABLE_PAT"]
    csv_path   = os.environ["CSV_PATH"]
    unique_key = os.environ["UNIQUE_KEY"]
    typecast   = (os.environ.get("AIRTABLE_TYPECAST","true").lower() == "true")
    do_softdel = (os.environ.get("AIRTABLE_SOFT_DELETE","false").lower() == "true")

    api  = f"https://api.airtable.com/v0/{base}/{quote(table, safe='')}"
    hdr  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # 1) Read CSV
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # 2) Pull existing [unique_key -> id]
    existing = {}
    offset = None
    sess = requests.Session()
    while True:
        params = {"pageSize": 100}
        if offset: params["offset"] = offset
        r = sess.get(api, headers=hdr, params=params, timeout=60)
        if not r.ok:
            print(f"[error] Read existing failed {r.status_code}: {r.text[:300]}")
            r.raise_for_status()
        data = r.json()
        for rec in data.get("records", []):
            k = (rec.get("fields",{}).get(unique_key) or "").strip()
            if k:
                existing[k] = rec["id"]
        offset = data.get("offset")
        if not offset:
            break

    # 3) Split updates vs creates
    updates, creates = [], []
    current_keys = set()
    for row in rows:
        k = (row.get(unique_key) or "").strip()
        if not k:
            continue
        current_keys.add(k)
        if k in existing:
            updates.append({"id": existing[k], "fields": row})
        else:
            creates.append({"fields": row})

    # 4) PATCH updates
    total_upd = total_new = 0
    for batch in batched(updates, 10):
        r = sess.patch(api, headers=hdr, json={"records": batch, "typecast": typecast}, timeout=60)
        if r.status_code in (429,500,502,503,504):
            time.sleep(2)
            r = sess.patch(api, headers=hdr, json={"records": batch, "typecast": typecast}, timeout=60)
        if not r.ok:
            print(f"[error] PATCH failed {r.status_code}: {r.text[:300]}")
            r.raise_for_status()
        total_upd += len(batch)

    # 5) POST creates
    for batch in batched(creates, 10):
        r = sess.post(api, headers=hdr, json={"records": batch, "typecast": typecast}, timeout=60)
        if r.status_code in (429,500,502,503,504):
            time.sleep(2)
            r = sess.post(api, headers=hdr, json={"records": batch, "typecast": typecast}, timeout=60)
        if not r.ok:
            print(f"[error] POST failed {r.status_code}: {r.text[:300]}")
            r.raise_for_status()
        total_new += len(batch)

    print(f"[ok] Upsert complete. Updated={total_upd}, Created={total_new}")

    # 6) Optional soft-delete
    if do_softdel:
        try:
            # Re-fetch all to get [id, key]
            pairs = []
            offset = None
            while True:
                params = {"pageSize": 100}
                if offset: params["offset"] = offset
                r = sess.get(api, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=60)
                r.raise_for_status()
                j = r.json()
                for rec in j.get("records", []):
                    fields = rec.get("fields", {})
                    k = (fields.get(unique_key) or "").strip()
                    if k:
                        pairs.append((rec["id"], k))
                offset = j.get("offset")
                if not offset:
                    break

            to_mark = [rid for rid, k in pairs if k and (k not in current_keys)]
            for batch_ids in batched(to_mark, 10):
                batch = [{"id": rid, "fields": {"Active": False}} for rid in batch_ids]
                r = sess.patch(api,
                               headers={"Authorization": f"Bearer {token}", "Content-Type":"application/json"},
                               json={"records": batch, "typecast": True},
                               timeout=60)
                if r.status_code in (429,500,502,503,504):
                    time.sleep(2)
                    r = sess.patch(api,
                                   headers={"Authorization": f"Bearer {token}", "Content-Type":"application/json"},
                                   json={"records": batch, "typecast": True},
                                   timeout=60)
                r.raise_for_status()
            print("[ok] Soft-delete complete (Active=False).")
        except requests.HTTPError as e:
            print(f"[warn] Soft-delete skipped: {e}. Add an 'Active' checkbox to '{table}'.")
            # don’t fail the job

if __name__ == "__main__":
    main()
