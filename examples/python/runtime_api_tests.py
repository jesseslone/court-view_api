#!/usr/bin/env python3
"""Runtime integration checks with dynamically discovered defendants.

This script intentionally avoids storing personal data. It discovers test subjects at
runtime from recent backfilled Anchorage criminal cases and prints only redacted IDs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from courtview_api_client import APIError, CourtViewAPIClient
from courtview_criminal_analysis import build_criminal_defendant_report


@dataclass
class Subject:
    first: str
    last: str
    dob: str
    source_case_number: str
    atn: str


def normalize_space(value: Any) -> str:
    return " ".join(str(value or "").split())


def split_party_name(full_name: str) -> Tuple[str, str]:
    cleaned = normalize_space(full_name)
    if not cleaned:
        return "", ""
    if "," in cleaned:
        last, first = cleaned.split(",", 1)
        return normalize_space(first).split(" ")[0], normalize_space(last)
    parts = cleaned.split(" ")
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]


def redact_subject(subject: Subject) -> str:
    token = f"{subject.last}|{subject.first}|{subject.dob}|{subject.atn}".encode("utf-8")
    return hashlib.sha256(token).hexdigest()[:12]


def extract_defendant_subjects(case_payload: Dict[str, Any], case_number: str) -> List[Subject]:
    out: List[Subject] = []
    for row in (case_payload.get("results") or {}).get("rows") or []:
        row_case = normalize_space((row or {}).get("case_number", "")).upper()
        if row_case != normalize_space(case_number).upper():
            continue
        values = (row or {}).get("values") or {}
        role = normalize_space(values.get("Party Type") or values.get("Role") or "")
        if not re.search(r"defendant", role, re.I):
            continue
        full_name = normalize_space(values.get("Party/Company") or values.get("Party") or "")
        first, last = split_party_name(full_name)
        if not (first and last):
            continue
        dob = normalize_space(values.get("Date of Birth") or values.get("DOB") or "")
        atn = ""
        for k, v in values.items():
            if "atn" in normalize_space(k).lower() and normalize_space(v):
                atn = normalize_space(v)
                break
        out.append(Subject(first=first, last=last, dob=dob, source_case_number=case_number, atn=atn))
    return out


def discover_subjects(client: CourtViewAPIClient, year: int, backfill_count: int, subject_count: int) -> List[Subject]:
    backfill = client.backfill_anchorage_criminal(
        count=backfill_count,
        year=year,
        max_attempts=max(200, backfill_count * 30),
        timeout_seconds=900,
        concurrency=2,
        include_defendant_network=False,
    )
    case_numbers = (backfill.get("summary") or {}).get("unique_case_numbers_captured") or []
    subjects: List[Subject] = []
    seen = set()

    for case_number in case_numbers:
        case_payload = client.search_case(
            case_number=case_number,
            include_cases=True,
            max_cases=100,
            max_pages=20,
            include_defendant_network=False,
        )
        for subject in extract_defendant_subjects(case_payload, case_number):
            sig = (subject.first.lower(), subject.last.lower(), subject.dob, subject.atn)
            if sig in seen:
                continue
            seen.add(sig)
            subjects.append(subject)
            if len(subjects) >= subject_count:
                return subjects

    return subjects


def run_checks(client: CourtViewAPIClient, subjects: List[Subject]) -> Dict[str, Any]:
    checks = []
    failures = []

    for idx, subject in enumerate(subjects, start=1):
        redacted = redact_subject(subject)
        try:
            payload = client.search_name(
                first=subject.first,
                last=subject.last,
                dob=subject.dob,
                include_cases=True,
                max_cases=150,
                max_pages=20,
            )
        except APIError as exc:
            failures.append(f"subject#{idx}:{redacted} name-search failed: {exc}")
            continue

        report = build_criminal_defendant_report(payload, atn_filter=subject.atn)
        records = report.get("records") or []
        source_case_present = any(
            normalize_space(r.get("case", {}).get("case_number", "")).upper()
            == normalize_space(subject.source_case_number).upper()
            for r in records
        )
        non_dismissed_ok = True
        for rec in records:
            for ch in rec.get("non_dismissed_charges") or []:
                if re.search(r"dismiss", normalize_space(ch.get("disposition", "")), re.I):
                    non_dismissed_ok = False
                    break
            if not non_dismissed_ok:
                break

        check = {
            "subject_id": redacted,
            "records_found": len(records),
            "source_case_present": source_case_present,
            "non_dismissed_filter_valid": non_dismissed_ok,
            "conviction_flag_present": any(bool(r.get("case", {}).get("conviction_on_case")) for r in records),
        }
        checks.append(check)

        if len(records) == 0:
            failures.append(f"subject#{idx}:{redacted} returned zero report records")
        if not source_case_present:
            failures.append(f"subject#{idx}:{redacted} did not include known source case")
        if not non_dismissed_ok:
            failures.append(f"subject#{idx}:{redacted} included dismissed charge in non-dismissed set")

    return {
        "checked_subjects": len(subjects),
        "checks": checks,
        "failures": failures,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run runtime integration checks with discovered subjects.")
    parser.add_argument("--base-url", default="http://localhost:8088")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--backfill-count", type=int, default=25)
    parser.add_argument("--subject-count", type=int, default=3)
    parser.add_argument("--json-out", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = CourtViewAPIClient(base_url=args.base_url, timeout_seconds=args.timeout)

    try:
        subjects = discover_subjects(
            client=client,
            year=args.year,
            backfill_count=args.backfill_count,
            subject_count=args.subject_count,
        )
    except APIError as exc:
        print(f"error: test subject discovery failed: {exc}", file=sys.stderr)
        return 1

    if not subjects:
        print("error: no test subjects were discovered", file=sys.stderr)
        return 1

    result = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "requested_subject_count": args.subject_count,
        "discovered_subject_count": len(subjects),
    }
    check_result = run_checks(client, subjects)
    result.update(check_result)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
            f.write("\n")

    json.dump(result, sys.stdout, indent=2)
    sys.stdout.write("\n")

    return 1 if result.get("failures") else 0


if __name__ == "__main__":
    raise SystemExit(main())
