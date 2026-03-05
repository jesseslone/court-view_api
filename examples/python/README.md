## Python Samples

These scripts call the Go API and derive criminal-case outputs from the API payload:

- non-dismissed charges
- charge state (`original`, `amended`, `downgraded`, `amended_downgraded`)
- case-level conviction signal

### Files

- `courtview_api_client.py`: reusable stdlib client
- `courtview_criminal_analysis.py`: extraction and classification helpers
- `criminal_charge_report.py`: one-shot report CLI
- `runtime_api_tests.py`: runtime integration checks with auto-discovered subjects
- `CourtView_API_Examples.ipynb`: notebook walkthrough (health, backfill 10 CR cases with defendant-network expansion, report generation, runtime tests)

### Privacy

- Do not commit generated JSON/CSV outputs.
- Scripts do not include any hardcoded personal data.
- `runtime_api_tests.py` emits redacted subject IDs only.
