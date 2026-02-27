"""
IRS Form 990 XML fetcher and Schedule R parser.

IRS publishes machine-readable 990 XML at:
  https://s3.amazonaws.com/irs-form-990/{object_id}_public.xml

Schedule R (Part V) contains related tax-exempt organizations:
  - EIN
  - Name
  - Relationship (Parent, Subsidiary, Brother/Sister, Supporting, Supported)
  - Controlling percentage (if applicable)
  - Exempt Code section
"""
import httpx
from lxml import etree

S3_BASE = "https://s3.amazonaws.com/irs-form-990"
EFTS_BASE = "https://efts.irs.gov/LATEST/search-index"

# IRS 990 XML namespaces vary by year — handle both
NS = {
    "irs": "http://www.irs.gov/efile",
}


async def fetch_xml(object_id: str) -> bytes | None:
    """Fetch 990 XML from IRS S3 by object ID."""
    url = f"{S3_BASE}/{object_id}_public.xml"
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(url)
            if resp.status_code == 200 and b"<Error>" not in resp.content[:200]:
                return resp.content
        except httpx.RequestError:
            pass
    return None


async def find_object_id_via_efts(ein: str) -> str | None:
    """
    Use IRS full-text search to find the latest object ID for an EIN.
    Falls back gracefully if EFTS is unreachable.
    """
    clean = ein.replace("-", "")
    params = {
        "q": f'"{clean}"',
        "forms": "990",
    }
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(EFTS_BASE, params=params)
            if resp.status_code == 200:
                data = resp.json()
                hits = data.get("hits", {}).get("hits", [])
                if hits:
                    return hits[0].get("_source", {}).get("ObjectId")
        except (httpx.RequestError, Exception):
            pass
    return None


def parse_schedule_r(xml_bytes: bytes) -> list[dict]:
    """
    Parse Schedule R Part V — Transactions With Related Organizations.
    Returns list of related org dicts with EIN, name, relationship, etc.
    """
    related = []

    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return []

    # The XML namespace varies; find it dynamically
    ns_uri = root.nsmap.get(None) or root.nsmap.get("irs") or ""
    ns = {"irs": ns_uri} if ns_uri else {}

    def find_all(tag: str):
        if ns_uri:
            return root.findall(f".//irs:{tag}", ns)
        return root.findall(f".//{tag}")

    def text(el, tag: str) -> str | None:
        child = el.find(f"irs:{tag}", ns) if ns_uri else el.find(tag)
        return child.text.strip() if child is not None and child.text else None

    # Schedule R Part V: Related Tax-Exempt Orgs
    for org_el in find_all("IdRelatedTaxExemptOrgGrp"):
        ein_val = text(org_el, "EIN") or text(org_el, "EINOfRelatedOrg")
        name_val = (
            text(org_el, "OrganizationName")
            or text(org_el, "NameOfRelatedOrganization")
            or text(org_el, "BusinessName")
        )
        # Try nested BusinessNameLine1Txt
        if not name_val:
            bn = org_el.find(".//irs:BusinessNameLine1Txt", ns) if ns_uri else org_el.find(".//BusinessNameLine1Txt")
            if bn is not None and bn.text:
                name_val = bn.text.strip()

        rel = (
            text(org_el, "OrganizationRelationship")
            or text(org_el, "PrimaryActivitiesCd")
            or "Related"
        )
        pct_el = text(org_el, "OwnershipPct") or text(org_el, "ControllingInterestPct")
        pct = float(pct_el) if pct_el else None

        if ein_val:
            related.append({
                "ein": ein_val,
                "name": name_val or "Unknown",
                "relationship": rel,
                "controlling_pct": pct,
            })

    # Also check Part IV: Identification of Related Organizations Taxable as Corporations
    for org_el in find_all("IdRelatedOrgTaxablePartnershipGrp"):
        ein_val = text(org_el, "EIN")
        name_val = text(org_el, "OrganizationName") or text(org_el, "BusinessName")
        if not name_val:
            bn = org_el.find(".//irs:BusinessNameLine1Txt", ns) if ns_uri else org_el.find(".//BusinessNameLine1Txt")
            if bn is not None and bn.text:
                name_val = bn.text.strip()
        if ein_val:
            related.append({
                "ein": ein_val,
                "name": name_val or "Unknown",
                "relationship": "Taxable Entity / Partnership",
                "controlling_pct": None,
            })

    return related


async def get_schedule_r(ein: str, object_ids: list[str]) -> tuple[list[dict], str | None]:
    """
    Try to get Schedule R data for an EIN.
    Tries EFTS first, then falls back through provided object_ids.
    Returns (related_entities, filing_year_or_None).
    """
    # Try EFTS to find the best object ID
    efts_id = await find_object_id_via_efts(ein)
    if efts_id:
        object_ids = [efts_id] + object_ids

    for oid in object_ids:
        xml = await fetch_xml(oid)
        if xml:
            related = parse_schedule_r(xml)
            # Extract tax year from XML
            year = None
            try:
                root = etree.fromstring(xml)
                ns_uri = root.nsmap.get(None) or ""
                ns = {"irs": ns_uri} if ns_uri else {}
                ty = root.find(".//irs:TaxYr", ns) if ns_uri else root.find(".//TaxYr")
                if ty is not None and ty.text:
                    year = ty.text
            except Exception:
                pass
            return related, year

    return [], None
