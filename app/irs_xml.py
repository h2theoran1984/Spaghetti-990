"""
IRS Form 990 XML fetcher and Schedule R parser.

IRS publishes machine-readable 990 XML in monthly bulk ZIP archives at:
  https://apps.irs.gov/pub/epostcard/990/xml/{year}/{year}_TEOS_XML_{month}A.zip

Index CSV maps EINs to ObjectIds:
  https://apps.irs.gov/pub/epostcard/990/xml/{year}/index_{year}.csv

Schedule R (Part V) contains related tax-exempt organizations.
"""
import io
import csv
import struct
import zlib
import datetime

import httpx
from lxml import etree

IRS_BASE = "https://apps.irs.gov/pub/epostcard/990/xml"

# ---------------------------------------------------------------------------
# Async ZIP streaming helpers
# ---------------------------------------------------------------------------
# We use HTTP range requests to avoid downloading full 100-1200MB batch ZIPs.
# Algorithm:
#   1. Fetch the last 64KB of the ZIP → find End of Central Directory (EOCD)
#   2. Parse EOCD → CDR offset + size
#   3. Fetch CDR bytes → find our filename + its local header offset
#   4. Fetch local file header → compute data offset + compressed size
#   5. Fetch compressed bytes → zlib decompress → return XML
# ---------------------------------------------------------------------------

EOCD_SIG = b"PK\x05\x06"
LFH_SIG = b"PK\x03\x04"
CDR_SIG = b"PK\x01\x02"


async def _range_get(client: httpx.AsyncClient, url: str, start: int, end: int) -> bytes:
    """Fetch a byte range from a URL."""
    resp = await client.get(url, headers={"Range": f"bytes={start}-{end}"})
    resp.raise_for_status()
    return resp.content


async def _get_content_length(client: httpx.AsyncClient, url: str) -> int | None:
    """HEAD request to get file size."""
    resp = await client.head(url)
    if resp.status_code in (200, 206):
        cl = resp.headers.get("content-length")
        return int(cl) if cl else None
    return None


async def _find_xml_in_zip(client: httpx.AsyncClient, zip_url: str, target_name: str) -> bytes | None:
    """
    Async streaming ZIP extraction — only downloads what's needed.
    Returns the decompressed content of target_name, or None if not found.
    """
    # 1. Get file size
    total_size = await _get_content_length(client, zip_url)
    if not total_size:
        return None

    # 2. Fetch last 64KB to locate EOCD
    tail_size = min(65536, total_size)
    tail = await _range_get(client, zip_url, total_size - tail_size, total_size - 1)

    eocd_pos = tail.rfind(EOCD_SIG)
    if eocd_pos == -1:
        return None

    eocd = tail[eocd_pos:]
    if len(eocd) < 22:
        return None

    # EOCD structure (offsets from signature start):
    # 4 sig | 2 disk# | 2 cdr_disk | 2 entries_this | 2 entries_total
    # 4 cdr_size | 4 cdr_offset | 2 comment_len
    cdr_size = struct.unpack_from("<I", eocd, 12)[0]
    cdr_offset = struct.unpack_from("<I", eocd, 16)[0]

    # ZIP64 check
    if cdr_offset == 0xFFFFFFFF:
        return None  # ZIP64 not handled

    # 3. Fetch Central Directory
    cdr_bytes = await _range_get(client, zip_url, cdr_offset, cdr_offset + cdr_size - 1)

    # 4. Scan CDR for our filename
    target_bytes = target_name.encode()
    pos = 0
    while pos < len(cdr_bytes) - 46:
        if cdr_bytes[pos:pos+4] != CDR_SIG:
            break
        # CDR entry layout (from signature):
        # 4 sig | 2 ver_made | 2 ver_needed | 2 flags | 2 method
        # 2 mod_time | 2 mod_date | 4 crc32
        # 4 compressed_size | 4 uncompressed_size
        # 2 fname_len | 2 extra_len | 2 comment_len | 2 disk_start
        # 2 int_attrs | 4 ext_attrs | 4 local_header_offset
        compressed_size = struct.unpack_from("<I", cdr_bytes, pos + 20)[0]
        fname_len = struct.unpack_from("<H", cdr_bytes, pos + 28)[0]
        extra_len = struct.unpack_from("<H", cdr_bytes, pos + 30)[0]
        comment_len = struct.unpack_from("<H", cdr_bytes, pos + 32)[0]
        method = struct.unpack_from("<H", cdr_bytes, pos + 10)[0]
        local_offset = struct.unpack_from("<I", cdr_bytes, pos + 42)[0]
        fname = cdr_bytes[pos + 46: pos + 46 + fname_len]

        entry_size = 46 + fname_len + extra_len + comment_len
        if fname == target_bytes:
            # 5. Fetch local file header to find data offset
            lfh = await _range_get(client, zip_url, local_offset, local_offset + 29)
            if lfh[:4] != LFH_SIG:
                return None
            lfh_fname_len = struct.unpack_from("<H", lfh, 26)[0]
            lfh_extra_len = struct.unpack_from("<H", lfh, 28)[0]
            data_offset = local_offset + 30 + lfh_fname_len + lfh_extra_len

            # 6. Fetch compressed data
            if compressed_size == 0:
                return None
            compressed = await _range_get(client, zip_url, data_offset, data_offset + compressed_size - 1)

            # 7. Decompress
            if method == 0:  # stored
                return compressed
            elif method == 8:  # deflated
                return zlib.decompress(compressed, -zlib.MAX_WBITS)
            return None

        pos += entry_size

    return None


def _object_id_to_zip_urls(object_id: str) -> list[str]:
    """
    Derive candidate batch ZIP URLs from an ObjectId.
    ObjectId format: YYYY DD XX...  (YYYY=year, DD=Julian day digits 5-6)
    """
    if len(object_id) < 6:
        return []

    try:
        year = int(object_id[:4])
        julian_day = int(object_id[4:6])
        base = datetime.date(year, 1, 1) + datetime.timedelta(days=julian_day - 1)
        month = base.month
    except (ValueError, OverflowError):
        year = datetime.date.today().year
        month = 1

    candidates = []
    # Try primary month (A and B batches), then neighbours
    for m_delta in [0, -1, 1, 2, -2]:
        raw = month - 1 + m_delta
        m = (raw % 12) + 1
        y = year + raw // 12
        for suffix in ["A", "B"]:
            candidates.append(f"{IRS_BASE}/{y}/{y}_TEOS_XML_{m:02d}{suffix}.zip")

    return candidates


# ---------------------------------------------------------------------------
# IRS index CSV lookup
# ---------------------------------------------------------------------------

async def find_object_id_via_index(ein: str) -> tuple[str | None, str | None]:
    """
    Search IRS index CSVs to find the latest 990 ObjectId for an EIN.
    Streams each CSV line-by-line — never loads the full file into memory.
    Returns (object_id, tax_period) or (None, None).
    """
    clean = ein.replace("-", "")
    current_year = datetime.date.today().year

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        for year in range(current_year, current_year - 3, -1):
            url = f"{IRS_BASE}/{year}/index_{year}.csv"
            best: dict | None = None
            try:
                async with client.stream("GET", url) as resp:
                    if resp.status_code != 200:
                        continue
                    header: list[str] | None = None
                    line_buf = ""
                    async for chunk in resp.aiter_text(chunk_size=65536):
                        line_buf += chunk
                        lines = line_buf.split("\n")
                        line_buf = lines[-1]  # keep partial last line
                        for raw_line in lines[:-1]:
                            raw_line = raw_line.strip()
                            if not raw_line:
                                continue
                            if header is None:
                                header = [h.strip() for h in raw_line.split(",")]
                                continue
                            # Simple CSV split (field values don't contain commas)
                            parts = raw_line.split(",")
                            if len(parts) < len(header):
                                continue
                            row = dict(zip(header, parts))
                            row_ein = row.get("EIN", "").strip().replace("-", "")
                            if row_ein == clean and row.get("RETURN_TYPE", "").strip() == "990":
                                # Keep highest OBJECT_ID (latest filing)
                                if best is None or row.get("OBJECT_ID", "") > best.get("OBJECT_ID", ""):
                                    best = row
            except Exception:
                pass

            if best:
                return best.get("OBJECT_ID"), best.get("TAX_PERIOD")

    return None, None


# ---------------------------------------------------------------------------
# XML parser
# ---------------------------------------------------------------------------

def parse_schedule_r(xml_bytes: bytes) -> list[dict]:
    """Parse Schedule R — related tax-exempt organizations."""
    related = []
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        return []

    ns_uri = root.nsmap.get(None) or root.nsmap.get("irs") or ""
    ns = {"irs": ns_uri} if ns_uri else {}

    def find_all(tag: str):
        return root.findall(f".//irs:{tag}", ns) if ns_uri else root.findall(f".//{tag}")

    def text(el, tag: str) -> str | None:
        child = el.find(f"irs:{tag}", ns) if ns_uri else el.find(tag)
        return child.text.strip() if child is not None and child.text else None

    for org_el in find_all("IdRelatedTaxExemptOrgGrp"):
        ein_val = text(org_el, "EIN") or text(org_el, "EINOfRelatedOrg")
        name_val = (
            text(org_el, "OrganizationName")
            or text(org_el, "NameOfRelatedOrganization")
            or text(org_el, "BusinessName")
        )
        if not name_val:
            bn = (org_el.find(".//irs:BusinessNameLine1Txt", ns)
                  if ns_uri else org_el.find(".//BusinessNameLine1Txt"))
            if bn is not None and bn.text:
                name_val = bn.text.strip()

        rel = text(org_el, "OrganizationRelationship") or text(org_el, "PrimaryActivitiesCd") or "Related"
        pct_el = text(org_el, "OwnershipPct") or text(org_el, "ControllingInterestPct")

        if ein_val:
            related.append({
                "ein": ein_val,
                "name": name_val or "Unknown",
                "relationship": rel,
                "controlling_pct": float(pct_el) if pct_el else None,
            })

    for org_el in find_all("IdRelatedOrgTaxablePartnershipGrp"):
        ein_val = text(org_el, "EIN")
        name_val = text(org_el, "OrganizationName") or text(org_el, "BusinessName")
        if not name_val:
            bn = (org_el.find(".//irs:BusinessNameLine1Txt", ns)
                  if ns_uri else org_el.find(".//BusinessNameLine1Txt"))
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


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def get_schedule_r(ein: str, _unused: list[str]) -> tuple[list[dict], str | None]:
    """
    Get Schedule R data for an EIN.
    Uses IRS index CSV → async streaming ZIP extraction → XML parse.
    """
    object_id, tax_period = await find_object_id_via_index(ein)
    filing_year = tax_period[:4] if tax_period and len(tax_period) >= 4 else None

    if not object_id:
        return [], filing_year

    target_name = f"{object_id}_public.xml"
    zip_urls = _object_id_to_zip_urls(object_id)

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        for zip_url in zip_urls:
            try:
                xml = await _find_xml_in_zip(client, zip_url, target_name)
                if xml:
                    related = parse_schedule_r(xml)
                    # Extract TaxYr from XML
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
            except Exception:
                continue

    return [], filing_year
