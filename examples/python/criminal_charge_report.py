#!/usr/bin/env python3
"""Generate criminal defendant charge report from CourtView API."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from typing import Any, Dict, List

from courtview_api_client import APIError, CourtViewAPIClient
from courtview_criminal_analysis import build_criminal_defendant_report, flatten_report_rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Lookup by case number or by name(+DOB/ATN), then return criminal-case defendant "
            "records with non-dismissed charges, amended/downgraded/original state, and conviction flags."
        )
    )
    parser.add_argument("--base-url", default="http://localhost:8088", help="API base URL")
    parser.add_argument("--timeout", type=int, default=120, help="HTTP timeout seconds")

    parser.add_argument("--case-number", default="", help="Case number (supports API normalization)")
    parser.add_argument("--first", default="", help="First name")
    parser.add_argument("--last", default="", help="Last name")
    parser.add_argument("--dob", default="", help="DOB for name search (MM/DD/YYYY)")
    parser.add_argument("--atn", default="", help="ATN filter (if available in source rows)")

    parser.add_argument("--max-cases", type=int, default=200)
    parser.add_argument("--max-pages", type=int, default=20)
    parser.add_argument("--include-defendant-network", action="store_true", help="For case-number lookups")

    parser.add_argument("--json-out", default="", help="Write report JSON to file")
    parser.add_argument("--csv-out", default="", help="Write flattened rows CSV to file")
    parser.add_argument("--summary-only", action="store_true", help="Print only totals summary JSON")
    return parser.parse_args()


def write_csv(path: str, rows: List[Dict[str, Any]]) -> None:
    columns = [
        "person_name",
        "person_dob",
        "person_atn",
        "case_number",
        "case_url",
        "case_disposition",
        "highest_charge_level",
        "offense_date",
        "arrest_date",
        "first_appearance_date",
        "conviction_on_case",
        "charge",
        "statute",
        "degree",
        "disposition",
        "charge_state",
        "is_conviction_disposition",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in columns})


def main() -> int:
    args = parse_args()

    if not args.case_number and not (args.first and args.last):
        print("error: provide --case-number OR --first and --last", file=sys.stderr)
        return 2

    client = CourtViewAPIClient(base_url=args.base_url, timeout_seconds=args.timeout)

    try:
        if args.case_number:
            payload = client.search_case(
                case_number=args.case_number,
                include_cases=True,
                max_cases=args.max_cases,
                max_pages=args.max_pages,
                include_defendant_network=args.include_defendant_network,
            )
            query = {
                "mode": "case",
                "case_number": args.case_number,
                "atn": args.atn,
            }
        else:
            payload = client.search_name(
                first=args.first,
                last=args.last,
                dob=args.dob,
                include_cases=True,
                max_cases=args.max_cases,
                max_pages=args.max_pages,
            )
            query = {
                "mode": "name",
                "first": args.first,
                "last": args.last,
                "dob": args.dob,
                "atn": args.atn,
            }
    except APIError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    report = build_criminal_defendant_report(payload, atn_filter=args.atn)
    report["query"] = query
    report["api_response_stats"] = {
        "cases_returned": len(payload.get("cases") or []),
        "result_rows": len((payload.get("results") or {}).get("rows") or []),
    }

    rows = flatten_report_rows(report)

    if args.csv_out:
        write_csv(args.csv_out, rows)

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
            f.write("\n")

    if args.summary_only:
        out = {
            "query": report.get("query"),
            "totals": report.get("totals"),
            "api_response_stats": report.get("api_response_stats"),
        }
    else:
        out = report

    json.dump(out, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
