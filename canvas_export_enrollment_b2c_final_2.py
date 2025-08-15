#!/usr/bin/env python3
"""
All-in-One Student Summary Export (Canvas → Single CSV) with Progress Logs
---------------------------------------------------------------------------
Creates ONE CSV with these columns per student:

- Student First Name
- Student Last Name
- Student ID nese ka (per SCS)           [Canvas SIS user id]
- Grade Level                             [blank unless you add custom-data fetch]
- Student Date of Birth                   [blank unless you add custom-data fetch]
- Email
- Parent First Name                       [from Observer enrollments]
- Parent Last Name
- Parent Email
- Parent Phone Number                     [Canvas doesn't store phone by default]
- Parent Date of Birth                    [Canvas doesn't store DOB by default]
- Address                                 [blank unless you add custom-data fetch]
- Enrollment Date                         [earliest student enrollment created_at]
- Observer Account linked with student    [Yes/No]
- Total number of assignments             [if --include-assignments]
- Completed ones                          [if --include-assignments]
- Total courses enrolled
- Progress                                [% if --include-assignments]
- Enrollment status                       [active if any active; else completed if all completed; else mixed]
- Course Names                            [semicolon-separated]

Example:
  python3 canvas_export_enrollments_b2c_final.py \
    --api-url https://cornerstoneeducation.instructure.com/api/v1 \
    --api-key "YOUR_API_KEY" \
    --account-id self \
    --out Students_AllInOne.csv \
    --include-canvas-ids "851,2220,951" \
    --include-assignments
"""

import argparse
import csv
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple, Set
from datetime import datetime
import requests

DEFAULT_PER_PAGE = 100
RETRY_STATUS = {429, 500, 502, 503, 504}

def parse_link_header(link_header: str) -> Dict[str, str]:
    links = {}
    if not link_header:
        return links
    parts = link_header.split(",")
    for part in parts:
        section = part.strip().split(";")
        if len(section) < 2:
            continue
        url_part = section[0].strip()
        if url_part.startswith("<") and url_part.endswith(">"):
            url = url_part[1:-1]
        else:
            continue
        rel = None
        for seg in section[1:]:
            seg = seg.strip()
            if seg.startswith('rel='):
                rel_val = seg.split("=", 1)[1].strip().strip('"')
                rel = rel_val
        if rel and url:
            links[rel] = url
    return links

def robust_get(session: requests.Session, url: str, headers: Dict[str, str], params: Optional[Dict]=None, max_retries: int=5) -> requests.Response:
    delay = 1.0
    for attempt in range(1, max_retries+1):
        resp = session.get(url, headers=headers, params=params, timeout=120)
        if resp.status_code in RETRY_STATUS:
            retry_after = resp.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = max(delay, float(retry_after))
                except Exception:
                    pass
            print(f"[WARN] HTTP {resp.status_code} on attempt {attempt}; retrying in {delay:.1f}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp

def iso_parse(dt: Optional[str]) -> Optional[datetime]:
    if not dt:
        return None
    try:
        return datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except Exception:
        return None

class CanvasClient:
    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.session = requests.Session()

    def paged_get(self, url: str, params: Optional[Dict]=None) -> Iterable[Dict]:
        params_local = dict(params or {})
        while True:
            resp = robust_get(self.session, url, self.headers, params=params_local)
            params_local = None
            data = resp.json()
            if isinstance(data, list):
                for item in data:
                    yield item
            else:
                yield data
            links = parse_link_header(resp.headers.get("Link", ""))
            next_url = links.get("next")
            if not next_url:
                break
            url = next_url

    def list_students(self, account_id: str, per_page: int=DEFAULT_PER_PAGE) -> Iterable[Dict]:
        url = f"{self.api_url}/accounts/{account_id}/users"
        params = {"enrollment_type[]": "student", "per_page": per_page}
        yield from self.paged_get(url, params)

    def list_student_enrollments(self, user_id: int, per_page: int=DEFAULT_PER_PAGE) -> Iterable[Dict]:
        url = f"{self.api_url}/users/{user_id}/enrollments"
        params = {"type[]": "StudentEnrollment", "include[]": "grades", "per_page": per_page}
        yield from self.paged_get(url, params)

    def get_course(self, course_id: int) -> Dict:
        url = f"{self.api_url}/courses/{course_id}"
        resp = robust_get(self.session, url, self.headers)
        return resp.json()

    def list_course_assignments(self, course_id: int, per_page: int=DEFAULT_PER_PAGE) -> Iterable[Dict]:
        url = f"{self.api_url}/courses/{course_id}/assignments"
        params = {"per_page": per_page}
        yield from self.paged_get(url, params)

    def list_student_submissions(self, course_id: int, user_id: int, per_page: int=DEFAULT_PER_PAGE) -> Iterable[Dict]:
        url = f"{self.api_url}/courses/{course_id}/students/submissions"
        params = {"student_ids[]": user_id, "per_page": per_page, "include[]": "submission_history"}
        yield from self.paged_get(url, params)

    def list_course_observer_enrollments(self, course_id: int, per_page: int=DEFAULT_PER_PAGE) -> Iterable[Dict]:
        url = f"{self.api_url}/courses/{course_id}/enrollments"
        params = {"type[]": "ObserverEnrollment", "per_page": per_page, "include[]": "user"}
        yield from self.paged_get(url, params)

def split_first_last(name: Optional[str], sortable_name: Optional[str]) -> Tuple[str, str]:
    sname = (sortable_name or "").strip()
    n = (name or "").strip()
    if sname and "," in sname:
        last, first = [x.strip() for x in sname.split(",", 1)]
        return first, last
    if n:
        parts = n.split()
        if len(parts) == 1:
            return parts[0], ""
        return " ".join(parts[:-1]), parts[-1]
    return "", ""

def pick_email(user: Dict) -> str:
    return (user.get("email") or user.get("login_id") or "").strip()

def build_summary_for_student(client: CanvasClient, user: Dict, include_assignments: bool, per_page: int) -> Dict:
    user_id = int(user["id"])
    first, last = split_first_last(user.get("name"), user.get("sortable_name"))
    email = pick_email(user)
    sis_user_id = (user.get("sis_user_id") or "").strip()

    print(f"[INFO] Processing student {user_id} ({first} {last})...", flush=True)

    total_courses = 0
    statuses = []
    earliest_enrollment = None
    course_ids = set()
    course_names = []
    total_assignments = 0
    completed_assignments = 0

    enrollments = list(client.list_student_enrollments(user_id, per_page=per_page))
    print(f"[INFO]  Found {len(enrollments)} enrollments for student {user_id}", flush=True)

    for idx, enr in enumerate(enrollments, start=1):
        cid = enr.get("course_id")
        if not cid:
            continue
        course_ids.add(cid)
        statuses.append(enr.get("enrollment_state") or "")
        created_at = iso_parse(enr.get("created_at"))
        if created_at and (earliest_enrollment is None or created_at < earliest_enrollment):
            earliest_enrollment = created_at

        try:
            course = client.get_course(cid)
            cname = course.get("name") or str(cid)
            course_names.append(cname)
            print(f"[INFO]   [{idx}/{len(enrollments)}] Course: {cname} (ID {cid})", flush=True)
        except Exception as e:
            print(f"[WARN]   [{idx}/{len(enrollments)}] Failed to fetch course {cid}: {e}", flush=True)

        if include_assignments:
            try:
                assignments = list(client.list_course_assignments(cid, per_page=per_page))
                total_assignments += len(assignments)
                print(f"[INFO]     Assignments found: {len(assignments)}", flush=True)
                submitted = 0
                for sub in client.list_student_submissions(cid, user_id, per_page=per_page):
                    if sub.get("submitted_at") or sub.get("workflow_state") in ("submitted", "graded"):
                        submitted += 1
                completed_assignments += submitted
                print(f"[INFO]     Completed assignments for this course: {submitted}", flush=True)
            except Exception as e:
                print(f"[WARN]     Could not fetch assignments/submissions for course {cid}: {e}", flush=True)

    total_courses = len(course_ids)
    overall_status = ""
    if statuses:
        if any(s == "active" for s in statuses):
            overall_status = "active"
        elif all(s in ("completed", "inactive", "deleted") for s in statuses):
            overall_status = "completed"
        else:
            overall_status = ",".join(sorted(set([s for s in statuses if s])))

    # Observer → Parent fields
    observer_linked = "No"
    parent_first = parent_last = parent_email = ""
    try:
        for cid in list(course_ids)[:10]:  # limit to reduce API calls
            for obs in client.list_course_observer_enrollments(cid, per_page=per_page):
                if int(obs.get("associated_user_id") or 0) != user_id:
                    continue
                observer_linked = "Yes"
                u = obs.get("user") or {}
                pf, pl = split_first_last(u.get("name"), u.get("sortable_name"))
                parent_first = parent_first or pf
                parent_last = parent_last or pl
                parent_email = parent_email or pick_email(u)
    except Exception as e:
        print(f"[WARN]   Observer lookup error for student {user_id}: {e}", flush=True)

    progress_pct = ""
    if include_assignments and total_assignments > 0:
        progress_pct = round(100.0 * completed_assignments / total_assignments, 2)

    print(f"[INFO] Finished student {user_id} ({first} {last})", flush=True)

    return {
        "Student First Name": first,
        "Student Last Name": last,
        "Student ID nese ka (per SCS)": sis_user_id,
        "Grade Level": "",
        "Student Date of Birth": "",
        "Email": email,
        "Parent First Name": parent_first,
        "Parent Last Name": parent_last,
        "Parent Email": parent_email,
        "Parent Phone Number": "",
        "Parent Date of Birth": "",
        "Address": "",
        "Enrollment Date": earliest_enrollment.isoformat() if earliest_enrollment else "",
        "Observer Account linked with student": observer_linked,
        "Total number of assignments": total_assignments if include_assignments else "",
        "Completed ones": completed_assignments if include_assignments else "",
        "Total courses enrolled": total_courses,
        "Progress": progress_pct,
        "Enrollment status": overall_status,
        "Course Names": "; ".join(sorted(course_names)),
    }

def parse_id_set(s: Optional[str], caster) -> Optional[Set]:
    if not s:
        return None
    out: Set = set()
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(caster(part))
        except Exception:
            out.add(part)
    return out if out else None

def main():
    parser = argparse.ArgumentParser(description="Export one CSV per student with profile + parent + aggregated course stats (with progress logs).")
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--account-id", default="self")
    parser.add_argument("--out", default="Students_AllInOne.csv")
    parser.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    parser.add_argument("--include-canvas-ids", default=None, help='Comma-separated Canvas user IDs, e.g., "851,2220,951"')
    parser.add_argument("--include-sis-ids", default=None, help='Comma-separated SIS user IDs, e.g., "S1234,S2345"')
    parser.add_argument("--include-assignments", action="store_true", help="Fetch assignments & submissions to compute totals/progress")
    args = parser.parse_args()

    client = CanvasClient(args.api_url, args.api_key)
    include_canvas_ids = parse_id_set(args.include_canvas_ids, int) if args.include_canvas_ids else None
    include_sis_ids = parse_id_set(args.include_sis_ids, str) if args.include_sis_ids else None

    fieldnames = [
        "Student First Name", "Student Last Name", "Student ID nese ka (per SCS)",
        "Grade Level", "Student Date of Birth", "Email",
        "Parent First Name", "Parent Last Name", "Parent Email",
        "Parent Phone Number", "Parent Date of Birth", "Address",
        "Enrollment Date", "Observer Account linked with student",
        "Total number of assignments", "Completed ones",
        "Total courses enrolled", "Progress", "Enrollment status", "Course Names",
    ]

    rows = []
    total_students = 0
    matched_students = 0

    print("[INFO] Starting export...", flush=True)
    for user in client.list_students(args.account_id, per_page=args.per_page):
        total_students += 1
        uid_raw = user.get("id")
        if not uid_raw:
            continue
        uid = int(uid_raw)
        sis = (user.get("sis_user_id") or "").strip()

        # Apply filters (if any)
        if include_canvas_ids and uid not in include_canvas_ids:
            continue
        if include_sis_ids and ((sis == "") or (sis not in include_sis_ids)):
            continue

        matched_students += 1
        row = build_summary_for_student(client, user, include_assignments=args.include_assignments, per_page=args.per_page)
        rows.append(row)
        print(f"[INFO] Wrote row for student {uid} ({row['Student First Name']} {row['Student Last Name']})", flush=True)

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"[INFO] Export complete. Matched {matched_students} students (scanned {total_students}). Wrote: {args.out}", flush=True)

if __name__ == "__main__":
    main()

