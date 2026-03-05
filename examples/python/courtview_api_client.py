#!/usr/bin/env python3
"""Minimal client for the CourtView Lookup API."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib import error as urlerror
from urllib import parse, request


class APIError(RuntimeError):
    """Raised when the CourtView API returns an error."""


@dataclass
class CourtViewAPIClient:
    base_url: str = "http://localhost:8088"
    timeout_seconds: int = 120

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        query = parse.urlencode({k: v for k, v in (params or {}).items() if v is not None})
        if query:
            path = f"{path}?{query}"
        url = parse.urljoin(self.base_url.rstrip("/") + "/", path.lstrip("/"))
        req = request.Request(url=url, method="GET")
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                payload = resp.read().decode("utf-8", errors="replace")
                data = json.loads(payload or "{}")
                if resp.status >= 400:
                    raise APIError(str(data.get("error") or data.get("message") or payload))
                return data
        except urlerror.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            try:
                data = json.loads(body or "{}")
                msg = str(data.get("error") or data.get("message") or body or str(exc))
            except json.JSONDecodeError:
                msg = body or str(exc)
            raise APIError(msg) from exc
        except urlerror.URLError as exc:
            raise APIError(f"request failed: {exc}") from exc

    def health(self) -> Dict[str, Any]:
        return self._get("/healthz")

    def search_name(
        self,
        first: str,
        last: str,
        dob: str = "",
        include_cases: bool = True,
        max_cases: int = 100,
        all_pages: bool = True,
        max_pages: int = 20,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "first": first,
            "last": last,
            "dob": dob or None,
            "include_cases": str(include_cases).lower(),
            "max_cases": max_cases,
            "all_pages": str(all_pages).lower(),
            "max_pages": max_pages,
        }
        return self._get("/v1/search/name", params)

    def search_case(
        self,
        case_number: str,
        include_cases: bool = True,
        max_cases: int = 200,
        all_pages: bool = True,
        max_pages: int = 20,
        include_defendant_network: bool = True,
        max_related_parties: int = 10,
        max_related_cases: int = 100,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "case_number": case_number,
            "include_cases": str(include_cases).lower(),
            "max_cases": max_cases,
            "all_pages": str(all_pages).lower(),
            "max_pages": max_pages,
            "include_defendant_network": str(include_defendant_network).lower(),
            "max_related_parties": max_related_parties,
            "max_related_cases": max_related_cases,
        }
        return self._get("/v1/search/case", params)

    def backfill_anchorage_criminal(
        self,
        count: int = 100,
        year: Optional[int] = None,
        start_seq: int = 1,
        max_attempts: int = 5000,
        timeout_seconds: int = 900,
        concurrency: int = 1,
        include_defendant_network: bool = False,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "count": count,
            "year": year,
            "start_seq": start_seq,
            "max_attempts": max_attempts,
            "timeout_seconds": timeout_seconds,
            "concurrency": concurrency,
            "include_defendant_network": str(include_defendant_network).lower(),
        }
        return self._get("/v1/admin/backfill/anchorage-criminal", params)


def _main() -> int:
    parser = argparse.ArgumentParser(description="Call CourtView API endpoints.")
    parser.add_argument("--base-url", default="http://localhost:8088", help="API base URL")
    parser.add_argument("--timeout", type=int, default=120, help="HTTP timeout seconds")

    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("health", help="GET /healthz")

    p_name = sub.add_parser("name", help="GET /v1/search/name")
    p_name.add_argument("--first", required=True)
    p_name.add_argument("--last", required=True)
    p_name.add_argument("--dob", default="")
    p_name.add_argument("--max-cases", type=int, default=100)
    p_name.add_argument("--max-pages", type=int, default=20)

    p_case = sub.add_parser("case", help="GET /v1/search/case")
    p_case.add_argument("--case-number", required=True)
    p_case.add_argument("--max-cases", type=int, default=200)
    p_case.add_argument("--max-pages", type=int, default=20)
    p_case.add_argument("--include-defendant-network", action="store_true")

    p_backfill = sub.add_parser("backfill", help="GET /v1/admin/backfill/anchorage-criminal")
    p_backfill.add_argument("--count", type=int, default=100)
    p_backfill.add_argument("--year", type=int)
    p_backfill.add_argument("--concurrency", type=int, default=1)

    args = parser.parse_args()
    client = CourtViewAPIClient(base_url=args.base_url, timeout_seconds=args.timeout)

    if args.cmd == "health":
        out = client.health()
    elif args.cmd == "name":
        out = client.search_name(
            first=args.first,
            last=args.last,
            dob=args.dob,
            max_cases=args.max_cases,
            max_pages=args.max_pages,
        )
    elif args.cmd == "case":
        out = client.search_case(
            case_number=args.case_number,
            max_cases=args.max_cases,
            max_pages=args.max_pages,
            include_defendant_network=args.include_defendant_network,
        )
    else:
        out = client.backfill_anchorage_criminal(
            count=args.count,
            year=args.year,
            concurrency=args.concurrency,
        )

    json.dump(out, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
