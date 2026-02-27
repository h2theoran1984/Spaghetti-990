"""
ProPublica Nonprofit Explorer API client.
Docs: https://projects.propublica.org/nonprofits/api
Free, no key required.
"""
import httpx

BASE = "https://projects.propublica.org/nonprofits/api/v2"
HEADERS = {"User-Agent": "990-entity-graph/0.1 (signalpot.dev)"}


async def get_organization(ein: str) -> dict | None:
    """Fetch org metadata + filing list by EIN."""
    clean = ein.replace("-", "")
    async with httpx.AsyncClient(headers=HEADERS, timeout=15) as client:
        resp = await client.get(f"{BASE}/organizations/{clean}.json")
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get("error"):
            return None
        return data


async def search_organization(name: str) -> list[dict]:
    """Search orgs by name, returns list of matches."""
    async with httpx.AsyncClient(headers=HEADERS, timeout=15) as client:
        resp = await client.get(f"{BASE}/search.json", params={"q": name})
        if resp.status_code != 200:
            return []
        return resp.json().get("organizations", [])


def extract_object_ids(org_data: dict) -> list[str]:
    """
    Extract all IRS object IDs from ProPublica org data.
    Returns list ordered newest-first to try against IRS S3.
    """
    ids = []

    # latest_object_id on the org itself
    if oid := org_data.get("organization", {}).get("latest_object_id"):
        ids.append(str(oid))

    # Object IDs embedded in PDF URLs:
    # e.g. ...340714585_202212_990_2024010422167836.pdf
    for filing in org_data.get("filings_with_data", []):
        pdf = filing.get("pdf_url", "")
        if pdf:
            # last segment before .pdf is often the object id
            segment = pdf.rstrip("/").split("/")[-1].replace(".pdf", "")
            parts = segment.split("_")
            if parts:
                ids.append(parts[-1])

    # Deduplicate preserving order
    seen = set()
    unique = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            unique.append(i)
    return unique
