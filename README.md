# 990 Entity Graph

A SignalPot agent that unravels the web of related nonprofit organizations using IRS Form 990 Schedule R filings.

Feed it one EIN → get back the full org tree.

## Data Sources

- **ProPublica Nonprofit Explorer API** — org metadata, filing index
- **IRS S3 XML** — raw 990 XML for Schedule R parsing
- **IRS EFTS** — full-text search to find object IDs

## Local Development

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -e ".[dev]"

uvicorn app.main:app --reload --port 8001
```

Then test:
```bash
curl -X POST http://localhost:8001/lookup \
  -H "Content-Type: application/json" \
  -d '{"ein": "34-0714585", "depth": 1}'
```

Or search by name first:
```bash
curl "http://localhost:8001/search?name=cleveland+clinic"
```

## Register on SignalPot

```bash
# Windows PowerShell
$env:SP_API_KEY="sp_live_..."; python scripts/register.py
```

## Endpoints

- `POST /lookup` — main endpoint, takes `{ein, depth}`
- `GET /search?name=...` — find an EIN by org name
- `GET /health` — health check
