#!/usr/bin/env python3
"""Criminal-case extraction helpers for CourtView API payloads."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

_DISMISS_RE = re.compile(r"\b(dismiss|dismissed|dismissal|nolle\s+prosequi|nol\.?\s+pros\.?|no\s+file)\b", re.I)
_CONVICT_RE = re.compile(
    r"\b(convict|convicted|conviction|guilty|found\s+guilty|adjudicated|plea\s+accepted|no\s+contest|nolo)\b",
    re.I,
)
_AMEND_RE = re.compile(r"\bamend|amended|supersed|modify|modified\b", re.I)
_DOWNGRADE_RE = re.compile(r"\bdowngrad|reduc(ed|tion)?\s+to|lesser\s+(included|offense)|lower\s+class\b", re.I)
_DATE_RE = re.compile(r"([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")

_CHARGE_LEVELS: List[Tuple[str, re.Pattern[str]]] = [
    ("Unclassified Felony", re.compile(r"\bunclassified\s+felony\b", re.I)),
    ("Felony A", re.compile(r"\b(felony\s*a|class\s*a\s+felony)\b", re.I)),
    ("Felony B", re.compile(r"\b(felony\s*b|class\s*b\s+felony)\b", re.I)),
    ("Felony C", re.compile(r"\b(felony\s*c|class\s*c\s+felony)\b", re.I)),
    ("Misdemeanor A", re.compile(r"\b(misdemeanor\s*a|class\s*a\s+misdemeanor)\b", re.I)),
    ("Misdemeanor B", re.compile(r"\b(misdemeanor\s*b|class\s*b\s+misdemeanor)\b", re.I)),
    ("Violation", re.compile(r"\bviolation\b", re.I)),
]


def normalize_space(value: Any) -> str:
    return " ".join(str(value or "").split())


def normalize_key(value: Any) -> str:
    return normalize_space(value).lower()


def first_value(row: Dict[str, Any], *candidates: str) -> str:
    if not row:
        return ""
    lowered = {normalize_key(k): normalize_space(v) for k, v in row.items()}
    for cand in candidates:
        key = normalize_key(cand)
        if key in lowered and lowered[key]:
            return lowered[key]
    return ""


def first_value_contains(row: Dict[str, Any], *snippets: str) -> str:
    if not row:
        return ""
    snippets_l = [normalize_key(s) for s in snippets]
    for k, v in row.items():
        k_l = normalize_key(k)
        if any(s in k_l for s in snippets_l):
            v_n = normalize_space(v)
            if v_n:
                return v_n
    return ""


def case_text(case: Dict[str, Any]) -> str:
    parts: List[str] = []
    current = normalize_space(case.get("current", {}).get("main_text_excerpt", ""))
    if current and not current.lower().startswith("tab fetch failed:"):
        parts.append(current)
    for tab in (case.get("tabs") or {}).values():
        excerpt = normalize_space((tab or {}).get("main_text_excerpt", ""))
        if excerpt and not excerpt.lower().startswith("tab fetch failed:"):
            parts.append(excerpt)
    return " ".join(parts)


def extract_date_after_label(text: str, label: str) -> str:
    pattern = re.compile(rf"{re.escape(label)}\s*:??\s*([0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}})", re.I)
    m = pattern.search(text or "")
    return normalize_space(m.group(1) if m else "")


def extract_date_from_text(text: str) -> str:
    m = _DATE_RE.search(text or "")
    return normalize_space(m.group(1) if m else "")


def highest_charge_level(text: str) -> str:
    for label, regex in _CHARGE_LEVELS:
        if regex.search(text or ""):
            return label
    return ""


def iter_pages(case: Dict[str, Any]) -> Iterable[Tuple[str, Dict[str, Any]]]:
    current = case.get("current") or {}
    if current:
        yield "current", current
    for tab_name, tab in (case.get("tabs") or {}).items():
        if tab:
            yield normalize_space(tab_name), tab


def iter_row_maps(case: Dict[str, Any]) -> Iterable[Tuple[str, Dict[str, str], Dict[str, str], int]]:
    for source_tab, page in iter_pages(case):
        for table in (page.get("tables") or []):
            headers_raw = [normalize_space(h) for h in (table.get("headers") or [])]
            headers = [h.lower() for h in headers_raw]
            if not headers:
                continue
            for idx, row in enumerate(table.get("rows") or [], start=1):
                mapped: Dict[str, str] = {}
                for i, value in enumerate(row or []):
                    if i >= len(headers):
                        continue
                    mapped[headers[i]] = normalize_space(value)
                if mapped:
                    yield source_tab, mapped, {"headers": "|".join(headers)}, idx


def infer_disposition(case_status: str, case_full_text: str, docket_rows: List[Dict[str, str]]) -> str:
    explicit = re.search(
        r"(?:Current\s+)?Disposition[:\s]+(.+?)(?:\s+Case Judge:|\s+Next Event:|\s+Events|\s+Docket Information|$)",
        case_full_text,
        re.I,
    )
    if explicit:
        return normalize_space(explicit.group(1))

    docket_blob = " ".join((r.get("docket text", "") or "") for r in docket_rows)
    if re.search(r"\bnot guilty\b", docket_blob, re.I):
        return "Not Guilty"
    if _DISMISS_RE.search(docket_blob):
        return "Dismissed"
    if re.search(r"\bacquitt(ed|al)\b", docket_blob, re.I):
        return "Acquitted"
    if _CONVICT_RE.search(docket_blob):
        return "Convicted"
    if re.search(r"\bsenten(c|t)e\b", docket_blob, re.I):
        return "Sentenced"
    if re.search(r"closed", case_status or "", re.I):
        return "Closed"
    if re.search(r"open|active|pending", case_status or "", re.I):
        return "Pending/Open"
    return normalize_space(case_status)


def extract_case_facts(case: Dict[str, Any]) -> Dict[str, Any]:
    labels = case.get("current", {}).get("label_values") or {}
    text = case_text(case)

    docket_rows: List[Dict[str, str]] = []
    event_rows: List[Dict[str, str]] = []
    for _, row, meta, _ in iter_row_maps(case):
        headers_blob = meta.get("headers", "")
        if "docket text" in headers_blob and "date" in headers_blob:
            docket_rows.append(row)
        if "date/time" in headers_blob and "type" in headers_blob:
            event_rows.append(row)

    case_type = normalize_space(labels.get("Case Type") or "")
    if not case_type:
        m = re.search(r"Case Type:\s*(.+?)\s+Case Status:", text, re.I)
        case_type = normalize_space(m.group(1) if m else "")

    case_status = normalize_space(labels.get("Case Status") or "")
    if not case_status:
        m = re.search(r"Case Status:\s*(.+?)\s+File Date:", text, re.I)
        case_status = normalize_space(m.group(1) if m else "")

    file_date = normalize_space(labels.get("File Date") or "") or extract_date_after_label(text, "File Date")
    case_judge = normalize_space(labels.get("Case Judge") or "")
    if not case_judge:
        m = re.search(
            r"Case Judge:\s*(.+?)(?:\s+Next Event:|\s+All Information|\s+Party Information|\s+Charge Information|\s+Docket Information|$)",
            text,
            re.I,
        )
        case_judge = normalize_space(m.group(1) if m else "")

    offense_date = extract_date_after_label(text, "Date of Offense") or extract_date_after_label(text, "Offense Date")

    arrest_date = (
        extract_date_after_label(text, "Arrest Date")
        or extract_date_after_label(text, "Date of Arrest")
        or extract_date_after_label(text, "Stage Date")
    )

    first_appearance_date = ""
    for row in event_rows:
        t = normalize_space(row.get("type", ""))
        if re.search(r"arraignment|initial\s+appearance|first\s+appearance", t, re.I):
            dt = normalize_space(row.get("date/time", ""))
            first_appearance_date = extract_date_from_text(dt)
            if first_appearance_date and not arrest_date:
                arrest_date = first_appearance_date
            break

    if not arrest_date:
        for row in docket_rows:
            text_value = normalize_space(row.get("docket text", ""))
            if re.search(r"arrest|booking|arraignment|initial\s+appearance", text_value, re.I):
                arrest_date = normalize_space(row.get("date", ""))
                break

    case_disposition = infer_disposition(case_status, text, docket_rows)

    amended = "No"
    between = re.search(r"Amended Charge\s*(.*?)\s*DV Related\?", text, re.I)
    if between:
        value = normalize_space(between.group(1))
        amended = "No" if value == "" or re.search(r"^(none|no|n/a)$", value, re.I) else "Yes"
    elif re.search(r"amended charge", text, re.I):
        amended = "Yes"
    else:
        for row in docket_rows:
            if _AMEND_RE.search(normalize_space(row.get("docket text", ""))):
                amended = "Yes"
                break

    docket_blob = " ".join(normalize_space(r.get("docket text", "")) for r in docket_rows)
    conviction_hint = bool(_CONVICT_RE.search(docket_blob) or _CONVICT_RE.search(case_disposition))

    return {
        "case_type": case_type,
        "case_status": case_status,
        "file_date": file_date,
        "case_judge": case_judge,
        "highest_charge_level": highest_charge_level(text),
        "offense_date": offense_date,
        "arrest_date": arrest_date,
        "first_appearance_date": first_appearance_date,
        "case_disposition": case_disposition,
        "amended_charges": amended,
        "conviction_hint": conviction_hint,
        "docket_blob": docket_blob,
    }


def extract_case_atn(text: str) -> str:
    m = re.search(r"\bATN\s*#\s*([A-Za-z0-9-]+)\b", text or "", re.I)
    return normalize_space(m.group(1) if m else "")


def extract_defendant_parties(case: Dict[str, Any], results_rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    case_number = normalize_space(case.get("case_number", ""))
    key = case_number.upper()
    found: List[Dict[str, str]] = []
    fallback_atn = extract_case_atn(case_text(case))

    for row in results_rows or []:
        row_case = normalize_space((row or {}).get("case_number", "")).upper()
        if key and row_case != key:
            continue
        values = (row or {}).get("values") or {}
        role = first_value(values, "Party Type", "Role")
        if not re.search(r"defendant", role, re.I):
            continue
        atn = first_value_contains(values, "atn")
        found.append(
            {
                "party_name": first_value(values, "Party/Company", "Party"),
                "party_role": role,
                "party_dob": first_value(values, "Date of Birth", "DOB"),
                "party_atn": atn or fallback_atn,
            }
        )

    for _, row_map, _, _ in iter_row_maps(case):
        role = first_value(row_map, "party type", "role")
        if not role or not re.search(r"defendant", role, re.I):
            continue
        name = first_value(row_map, "party/company", "party")
        if not name:
            continue
        found.append(
            {
                "party_name": name,
                "party_role": role,
                "party_dob": first_value(row_map, "date of birth", "dob"),
                "party_atn": first_value_contains(row_map, "atn") or fallback_atn,
            }
        )

    dedup: Dict[str, Dict[str, str]] = {}
    for p in found:
        sig = "|".join(
            [
                normalize_space(p.get("party_name", "")).lower(),
                normalize_space(p.get("party_dob", "")).lower(),
                normalize_space(p.get("party_atn", "")).lower(),
            ]
        )
        if sig.strip("|"):
            dedup[sig] = p

    if dedup:
        return list(dedup.values())
    return [{"party_name": "", "party_role": "Defendant", "party_dob": "", "party_atn": fallback_atn}]


def extract_text_charge_rows(case: Dict[str, Any]) -> List[Dict[str, Any]]:
    text = case_text(case)
    if not re.search(r"Charge\s*#\s*\d+\s*:", text or "", re.I):
        return []

    out: List[Dict[str, Any]] = []
    pattern = re.compile(
        r"Charge\s*#\s*(\d+)\s*:\s*(.*?)(?=Charge\s*#\s*\d+\s*:|\bEvents\b\s+Date/Time|\bDocket Information\b|$)",
        re.I | re.S,
    )
    for match in pattern.finditer(text):
        seq = int(match.group(1))
        block = normalize_space(match.group(2))
        if not block:
            continue

        header = re.split(r"\bOriginal Charge\b", block, maxsplit=1, flags=re.I)[0]
        header = normalize_space(header)

        statute = ""
        statute_match = re.search(r"\b([A-Z]{2,}[0-9][A-Z0-9.-]*(?:-[A-Z0-9]+)?)\b", header, re.I)
        if statute_match:
            statute = normalize_space(statute_match.group(1))

        degree = ""
        degree_match = re.search(
            r"\b(Unclassified\s+Felony|Class\s+[A-Z]\s+Felony(?:\s*\([^)]*\))?|Class\s+[A-Z]\s+Misdemeanor(?:\s*\([^)]*\))?|Violation)\b",
            header,
            re.I,
        )
        if degree_match:
            degree = normalize_space(degree_match.group(1))

        charge_text = re.sub(r"^[A-Z]{2,}[0-9][A-Z0-9.-]*(?:-[A-Z0-9]+)?\s*-\s*", "", header, flags=re.I)
        charge_text = normalize_space(charge_text)

        disposition = ""
        disp_match = re.search(
            r"\bDisposition\s*:?\s*(.*?)\s*(?:Original Charge|Indicted Charge|Amended Charge|DV Related\?|Modifiers|Stage Date|ATN\s*#|Tracking\s*#|Offense Location|Date of Offense|$)",
            block,
            re.I,
        )
        if disp_match:
            disposition = normalize_space(disp_match.group(1))

        amended_field = ""
        amend_match = re.search(
            r"\bAmended Charge\s*(.*?)\s*(?:DV Related\?|Modifiers|Stage Date|ATN\s*#|Tracking\s*#|Offense Location|Date of Offense|$)",
            block,
            re.I,
        )
        if amend_match:
            amended_field = normalize_space(amend_match.group(1))

        out.append(
            {
                "source_tab": "CurrentTextFallback",
                "source_row_index": seq,
                "charge": charge_text,
                "statute": statute,
                "degree": degree,
                "disposition": disposition,
                "amended_field": amended_field,
            }
        )

    return out


def extract_charges(case: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for source_tab, row, meta, row_index in iter_row_maps(case):
        headers_blob = meta.get("headers", "")
        if "charge" not in headers_blob:
            continue
        if not any(token in headers_blob for token in ("disposition", "description", "statute", "count", "status", "degree", "level")):
            continue

        charge_text = (
            first_value(row, "charge", "charge description", "description", "offense", "count description")
            or first_value_contains(row, "charge")
        )
        disposition = first_value(row, "disposition", "result", "judgment", "status")
        statute = first_value(row, "statute", "citation", "as")
        degree = first_value(row, "charge level", "level", "class", "degree", "severity")
        amended_field = first_value_contains(row, "amended")
        if not any([charge_text, disposition, statute, degree, amended_field]):
            continue

        out.append(
            {
                "source_tab": source_tab,
                "source_row_index": row_index,
                "charge": charge_text,
                "statute": statute,
                "degree": degree,
                "disposition": disposition,
                "amended_field": amended_field,
            }
        )

    dedup: Dict[str, Dict[str, Any]] = {}
    for row in out:
        sig = "|".join(
            [
                normalize_space(row.get("charge", "")).lower(),
                normalize_space(row.get("statute", "")).lower(),
                normalize_space(row.get("degree", "")).lower(),
                normalize_space(row.get("disposition", "")).lower(),
            ]
        )
        dedup[sig] = row

    # CourtView occasionally returns 500 when loading charge/docket tabs.
    # Fallback to parsing charge blocks from the Current/All Information text.
    if not dedup:
        for row in extract_text_charge_rows(case):
            sig = "|".join(
                [
                    normalize_space(row.get("charge", "")).lower(),
                    normalize_space(row.get("statute", "")).lower(),
                    normalize_space(row.get("degree", "")).lower(),
                    normalize_space(row.get("disposition", "")).lower(),
                ]
            )
            dedup[sig] = row

    return list(dedup.values())


def is_dismissed(disposition: str) -> bool:
    return bool(_DISMISS_RE.search(normalize_space(disposition)))


def is_conviction(disposition: str) -> bool:
    return bool(_CONVICT_RE.search(normalize_space(disposition)))


def charge_state(charge_row: Dict[str, Any], docket_blob: str, case_amended_flag: str) -> str:
    row_blob = " ".join(
        [
            normalize_space(charge_row.get("charge", "")),
            normalize_space(charge_row.get("disposition", "")),
            normalize_space(charge_row.get("amended_field", "")),
        ]
    )
    amended = bool(_AMEND_RE.search(row_blob) or _AMEND_RE.search(docket_blob) or case_amended_flag == "Yes")
    downgraded = bool(_DOWNGRADE_RE.search(row_blob) or _DOWNGRADE_RE.search(docket_blob))
    if amended and downgraded:
        return "amended_downgraded"
    if amended:
        return "amended"
    if downgraded:
        return "downgraded"
    return "original"


def build_criminal_defendant_report(payload: Dict[str, Any], atn_filter: str = "") -> Dict[str, Any]:
    atn_filter_norm = normalize_space(atn_filter).lower()
    cases = payload.get("cases") or []
    results_rows = (payload.get("results") or {}).get("rows") or []

    records: List[Dict[str, Any]] = []

    for case in cases:
        case_facts = extract_case_facts(case)
        if "criminal" not in normalize_space(case_facts.get("case_type", "")).lower():
            continue

        parties = extract_defendant_parties(case, results_rows)
        if atn_filter_norm:
            parties = [
                p for p in parties if atn_filter_norm == normalize_space(p.get("party_atn", "")).lower()
            ]
            if not parties:
                continue

        charges = extract_charges(case)
        non_dismissed = []
        for charge in charges:
            disp = normalize_space(charge.get("disposition", ""))
            if disp and is_dismissed(disp):
                continue
            state = charge_state(charge, case_facts.get("docket_blob", ""), case_facts.get("amended_charges", "No"))
            non_dismissed.append(
                {
                    "charge": normalize_space(charge.get("charge", "")),
                    "statute": normalize_space(charge.get("statute", "")),
                    "degree": normalize_space(charge.get("degree", "")),
                    "disposition": disp,
                    "charge_state": state,
                    "is_conviction_disposition": is_conviction(disp),
                    "source_tab": normalize_space(charge.get("source_tab", "")),
                }
            )

        conviction_on_case = bool(case_facts.get("conviction_hint") or any(c.get("is_conviction_disposition") for c in non_dismissed))

        for party in parties:
            records.append(
                {
                    "person": {
                        "name": normalize_space(party.get("party_name", "")),
                        "dob": normalize_space(party.get("party_dob", "")),
                        "atn": normalize_space(party.get("party_atn", "")),
                    },
                    "case": {
                        "case_number": normalize_space(case.get("case_number", "")),
                        "case_url": normalize_space(case.get("case_url", "")),
                        "case_type": normalize_space(case_facts.get("case_type", "")),
                        "case_status": normalize_space(case_facts.get("case_status", "")),
                        "file_date": normalize_space(case_facts.get("file_date", "")),
                        "offense_date": normalize_space(case_facts.get("offense_date", "")),
                        "arrest_date": normalize_space(case_facts.get("arrest_date", "")),
                        "first_appearance_date": normalize_space(case_facts.get("first_appearance_date", "")),
                        "case_disposition": normalize_space(case_facts.get("case_disposition", "")),
                        "highest_charge_level": normalize_space(case_facts.get("highest_charge_level", "")),
                        "amended_charges": normalize_space(case_facts.get("amended_charges", "")),
                        "conviction_on_case": conviction_on_case,
                    },
                    "non_dismissed_charges": non_dismissed,
                }
            )

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "records": records,
        "totals": {
            "records": len(records),
            "cases": len({r["case"]["case_number"] for r in records if r.get("case", {}).get("case_number")}),
            "people": len({(r["person"]["name"], r["person"]["dob"], r["person"]["atn"]) for r in records}),
            "records_with_conviction": sum(1 for r in records if r.get("case", {}).get("conviction_on_case")),
            "total_non_dismissed_charges": sum(len(r.get("non_dismissed_charges") or []) for r in records),
        },
    }


def flatten_report_rows(report: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for record in report.get("records") or []:
        person = record.get("person") or {}
        case = record.get("case") or {}
        charges = record.get("non_dismissed_charges") or []
        if not charges:
            rows.append(
                {
                    "person_name": person.get("name", ""),
                    "person_dob": person.get("dob", ""),
                    "person_atn": person.get("atn", ""),
                    "case_number": case.get("case_number", ""),
                    "case_url": case.get("case_url", ""),
                    "case_disposition": case.get("case_disposition", ""),
                    "highest_charge_level": case.get("highest_charge_level", ""),
                    "offense_date": case.get("offense_date", ""),
                    "arrest_date": case.get("arrest_date", ""),
                    "first_appearance_date": case.get("first_appearance_date", ""),
                    "conviction_on_case": case.get("conviction_on_case", False),
                    "charge": "",
                    "statute": "",
                    "degree": "",
                    "disposition": "",
                    "charge_state": "",
                    "is_conviction_disposition": False,
                }
            )
            continue

        for charge in charges:
            rows.append(
                {
                    "person_name": person.get("name", ""),
                    "person_dob": person.get("dob", ""),
                    "person_atn": person.get("atn", ""),
                    "case_number": case.get("case_number", ""),
                    "case_url": case.get("case_url", ""),
                    "case_disposition": case.get("case_disposition", ""),
                    "highest_charge_level": case.get("highest_charge_level", ""),
                    "offense_date": case.get("offense_date", ""),
                    "arrest_date": case.get("arrest_date", ""),
                    "first_appearance_date": case.get("first_appearance_date", ""),
                    "conviction_on_case": case.get("conviction_on_case", False),
                    "charge": charge.get("charge", ""),
                    "statute": charge.get("statute", ""),
                    "degree": charge.get("degree", ""),
                    "disposition": charge.get("disposition", ""),
                    "charge_state": charge.get("charge_state", ""),
                    "is_conviction_disposition": charge.get("is_conviction_disposition", False),
                }
            )
    return rows
