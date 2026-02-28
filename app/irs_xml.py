"""
IRS Form 990 XML fetcher and Schedule R parser.

IRS publishes machine-readable 990 XML in monthly bulk ZIP archives at:
  https://apps.irs.gov/pub/epostcard/990/xml/{year}/{year}_TEOS_XML_{month}A.zip

Index CSV maps EINs to ObjectIds:
  https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv

Schedule R (Part V) contains related tax-exempt organizations:
  - EIN
  - Name
  - Relationship (Parent, Subsidiary, Brother/Sister, Supporting, Supported)
  - Controlling percentage (if applicable)
"""
import io
import csv
import zipfile
import datetime

import httpx
import remotezip
from lxml import etree

IRS_BASE = "https://apps.irs.gov/pub/epostcard/990/xml"

# IRS 990 XML namespaces vary by year — handle both
NS_URI = "http://www.irs.gov/efile"


def _julian_to_month(julian_day: int, year: int) -> int:
    """Convert a Julian day number to a calendar month (1-12)."""
    try:
        dt = datetime.date(year, 1, 1) + datetime.timedelta(days=julian_day - 1)
        return dt.month
    except (ValueError, OverflowError):
        return 1


def _object_id_to_zip_url(object_id: str) -> list[str]:
    """
    Derive likely batch ZIP URLs from an ObjectId.

    ObjectId format (18 digits): YYYYDDDXXXXXXXXXXXXXX
      YYYY = year filed
      DDD  = Julian day (digits 5-7, but IRS uses 2-digit day portion)

    Returns a list of candidate ZIP URLs to try (most likely first).
    """
    if len(object_id) < 6:
        return []

    try:
        year = int(object_id[:4])
        # Digits 5-6 encode Julian day in IRS DLN format
        julian_day = int(object_id[4:6])
        month = _julian_to_month(julian_day, year)
    except (ValueError, IndexError):
        year = datetime.date.today().year
        month = 1

    # Build candidate URLs: primary month first, then the rest
    candidates = []
    for suffix in ["A", "B"]:
        candidates.append(f"{IRS_BASE}/{year}/{year}_TEOS_XML_{month:02d}{suffix}.zip")
    # Add surrounding months as fallback
    for delta in [-1, 1, 2, -2]:
        m = ((month - 1 + delta) % 12) + 1
        y = year + (month - 1 + delta) // 12
        candidates.append(f"{IRS_BASE}/{y}/{y}_TEOS_XML_{m:02d}A.zip")

    return candidates


async def find_object_id_via_index(ein: str) -> tuple[str | None, str | None]:
    """
    Look up EIN in the IRS index CSVs to find the latest 990 ObjectId.
    Returns (object_id, tax_period) or (None, None).
    Searches the last 4 years.
    """
    clean = ein.replace("-", "")
    current_year = datetime.date.today().year

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for year in range(current_year, current_year - 4, -1):
            url = f"{IRS_BASE}/{year}/index_{year}.csv"
            try:
                resp = await client.get(url)
                if resp.status_code != 200:
                    continue
                reader = csv.DictReader(io.StringIO(resp.text))
                # Collect all 990 filings for this EIN (not 990T/990EZ)
                matches = []
                for row in reader:
                    row_ein = row.get("EIN", "").strip().replace("-", "")
                    if row_ein == clean and row.get("RETURN_TYPE", "").strip() == "990":
                        matches.append(row)
                if matches:
                    # Sort by OBJECT_ID descending (newest first)
                    matches.sort(key=lambda r: r.get("OBJECT_ID", ""), reverse=True)
                    best = matches[0]
                    return best.get("OBJECT_ID"), best.get("TAX_PERIOD")
            except Exception:
                continue

    return None, None


async def fetch_xml_from_zip(object_id: str) -> bytes | None:
    """
    Fetch a single 990 XML file from an IRS batch ZIP using HTTP range requests.
    Only downloads the Central Directory + the specific compressed file — not the full ZIP.
    """
    filename = f"{object_id}_public.xml"
    zip_urls = _object_id_to_zip_url(object_id)

    for zip_url in zip_urls:
        try:
            with remotezip.RemoteZip(zip_url) as rz:
                names = rz.namelist()
                if filename in names:
                    return rz.read(filename)
        except Exception:
            continue

    return None


def parse_schedule_r(xml_bytes: bytes) -> list[dict]:
    """
    Parse Schedule R Part V — Related Tax-Exempt Organizations.
    Returns list of related org dicts with EIN, name, relationship, etc.
    """
    related = []

    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return []

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
        if not name_val:
            bn = (
                org_el.find(".//irs:BusinessNameLine1Txt", ns)
                if ns_uri
                else org_el.find(".//BusinessNameLine1Txt")
            )
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

    # Also check Part IV: Taxable Entities / Partnerships
    for org_el in find_all("IdRelatedOrgTaxablePartnershipGrp"):
        ein_val = text(org_el, "EIN")
        name_val = text(org_el, "OrganizationName") or text(org_el, "BusinessName")
        if not name_val:
            bn = (
                org_el.find(".//irs:BusinessNameLine1Txt", ns)
                if ns_uri
                else org_el.find(".//BusinessNameLine1Txt")
            )
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


async def get_schedule_r(ein: str, _object_ids_unused: list[str]) -> tuple[list[dict], str | None]:
    """
    Get Schedule R data for an EIN.
    Uses IRS index CSV to find the correct ObjectId, then streams the XML from the batch ZIP.
    Returns (related_entities, filing_year_or_None).
    """
    object_id, tax_period = await find_object_id_via_index(ein)

    if not object_id:
        return [], None

    # Derive filing year from tax_period (format: YYYYMM)
    filing_year = tax_period[:4] if tax_period and len(tax_period) >= 4 else None

    xml = await fetch_xml_from_zip(object_id)
    if not xml:
        # Index found the filing but we couldn't stream the XML
        # Return empty relations but with the filing year we know
        return [], filing_year

    related = parse_schedule_r(xml)

    # Also extract TaxYr from XML for accuracy
    try:
        root = etree.fromstring(xml)
        ns_uri = root.nsmap.get(None) or ""
        ns = {"irs": ns_uri} if ns_uri else {}
        ty = root.find(".//irs:TaxYr", ns) if ns_uri else root.find(".//TaxYr")
        if ty is not None and ty.text:
            filing_year = ty.text
    except Exception:
        pass

    return related, filing_year
