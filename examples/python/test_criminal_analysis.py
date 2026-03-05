#!/usr/bin/env python3
from __future__ import annotations

import unittest

from courtview_criminal_analysis import build_criminal_defendant_report


class CriminalAnalysisTests(unittest.TestCase):
    def test_ignores_non_criminal_cases(self) -> None:
        payload = {
            "results": {"rows": []},
            "cases": [
                {
                    "case_number": "3AN-26-00001CV",
                    "case_url": "https://example/case",
                    "current": {
                        "label_values": {"Case Type": "Civil", "Case Status": "Open"},
                        "tables": [],
                        "main_text_excerpt": "Case Type: Civil Case Status: Open",
                    },
                    "tabs": {},
                }
            ],
        }
        report = build_criminal_defendant_report(payload)
        self.assertEqual(0, report["totals"]["records"])

    def test_extracts_non_dismissed_and_conviction(self) -> None:
        payload = {
            "results": {
                "rows": [
                    {
                        "case_number": "3AN-26-90002CR",
                        "values": {
                            "Party/Company": "Doe, Jane",
                            "Party Type": "Defendant",
                            "DOB": "01/02/1990",
                            "ATN": "12345678",
                        },
                    }
                ]
            },
            "cases": [
                {
                    "case_number": "3AN-26-90002CR",
                    "case_url": "https://example/case/2",
                    "current": {
                        "label_values": {"Case Type": "Criminal", "Case Status": "Closed"},
                        "main_text_excerpt": (
                            "Case Type: Criminal Case Status: Closed "
                            "Date of Offense: 02/10/2026 Arrest Date: 02/11/2026"
                        ),
                        "tables": [
                            {
                                "headers": ["Date", "Docket Text"],
                                "rows": [
                                    ["03/01/2026", "Charge amended to lesser offense"],
                                    ["03/15/2026", "Defendant convicted"],
                                ],
                            },
                            {
                                "headers": ["Charge", "Disposition", "Statute", "Degree", "Amended Charge"],
                                "rows": [
                                    ["Theft", "Dismissed", "11.46.130", "Misdemeanor B", ""],
                                    ["Assault", "Convicted", "11.41.220", "Misdemeanor A", "Yes"],
                                ],
                            },
                        ],
                    },
                    "tabs": {},
                }
            ],
        }

        report = build_criminal_defendant_report(payload, atn_filter="12345678")
        self.assertEqual(1, report["totals"]["records"])
        rec = report["records"][0]
        self.assertTrue(rec["case"]["conviction_on_case"])
        self.assertEqual("Doe, Jane", rec["person"]["name"])
        self.assertEqual(1, len(rec["non_dismissed_charges"]))
        charge = rec["non_dismissed_charges"][0]
        self.assertEqual("Assault", charge["charge"])
        self.assertEqual("amended_downgraded", charge["charge_state"])
        self.assertTrue(charge["is_conviction_disposition"])

    def test_text_fallback_extracts_charge_and_atn_when_tabs_fail(self) -> None:
        payload = {
            "results": {
                "rows": [
                    {
                        "case_number": "3AN-26-90002CR",
                        "values": {
                            "Party/Company": "Sample, Person",
                            "Party Type": "Defendant",
                            "Date of Birth": "01/05/1990",
                            "Case Type": "Criminal",
                            "Case Status": "Open",
                            "File Date": "01/01/2026",
                        },
                    }
                ]
            },
            "cases": [
                {
                    "case_number": "3AN-26-90002CR",
                    "case_url": "https://example/case/2",
                    "current": {
                        "label_values": {},
                        "tables": [],
                        "main_text_excerpt": (
                            "Case Type: Criminal Case Status: Open File Date: 01/01/2026 "
                            "Charge # 1: AMC810010B1-V2 - Class A Misdemeanor (City) "
                            "AMC8.10.010(B)(1): Sample Assault "
                            "Original Charge AMC810010B1-V2 AMC8.10.010(B)(1): Sample Assault "
                            "(Class A Misdemeanor (City)) Indicted Charge Amended Charge DV Related? Yes "
                            "Modifiers None Stage Date 01/01/2026 ATN # 000000001 Tracking # 001 "
                            "Offense Location Example City Date of Offense 01/01/2026"
                        ),
                    },
                    "tabs": {
                        "Charge": {
                            "main_text_excerpt": "tab fetch failed: http 500"
                        }
                    },
                }
            ],
        }

        report = build_criminal_defendant_report(payload)
        self.assertEqual(1, report["totals"]["records"])
        rec = report["records"][0]
        self.assertEqual("000000001", rec["person"]["atn"])
        self.assertEqual("Misdemeanor A", rec["case"]["highest_charge_level"])
        self.assertEqual(1, len(rec["non_dismissed_charges"]))
        charge = rec["non_dismissed_charges"][0]
        self.assertIn("Sample Assault", charge["charge"])
        self.assertEqual("AMC810010B1-V2", charge["statute"])


if __name__ == "__main__":
    unittest.main()
