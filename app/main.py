"""
990 Entity Graph Agent
Exposes related organizations from IRS Form 990 Schedule R filings.
"""
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from .models import EntityGraphRequest, EntityGraphResponse, EntityNode, RelatedEntity
from .propublica import get_organization, extract_object_ids
from .irs_xml import get_schedule_r

app = FastAPI(
    title="Spaghetti 990",
    description="Healthcare org structures look like spaghetti. Feed it one EIN, get back the full org tree from IRS Form 990 Schedule R.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/lookup", response_model=EntityGraphResponse)
async def lookup_entity_graph(req: EntityGraphRequest):
    depth = max(1, min(req.depth, 3))  # cap at 3 to avoid runaway recursion
    visited: set[str] = set()
    total = 0

    async def build_node(ein: str, current_depth: int) -> EntityNode:
        nonlocal total
        clean_ein = ein.replace("-", "")

        if clean_ein in visited:
            # Already processed — return stub to avoid cycles
            return EntityNode(ein=clean_ein, name="[Already mapped]")
        visited.add(clean_ein)

        # 1. Get org metadata from ProPublica
        org_data = await get_organization(clean_ein)
        if not org_data:
            raise HTTPException(
                status_code=404,
                detail=f"Organization not found in ProPublica for EIN {ein}. "
                       "Verify the EIN is correct and the org files a 990."
            )

        org = org_data.get("organization", {})
        name = org.get("name") or org.get("sort_name") or "Unknown"
        city = org.get("city")
        state = org.get("state")
        revenue = org.get("revenue_amount")

        # 2. Get Schedule R from IRS XML
        object_ids = extract_object_ids(org_data)
        raw_related, filing_year = await get_schedule_r(clean_ein, object_ids)

        total += 1

        # 3. Build related entity list
        related_entities = []
        for r in raw_related:
            rel_ein = r["ein"].replace("-", "")
            related_entities.append(RelatedEntity(
                ein=rel_ein,
                name=r["name"],
                relationship=r["relationship"],
                controlling_pct=r.get("controlling_pct"),
            ))

        # 4. Recurse if depth allows
        children = []
        if current_depth < depth:
            for rel in related_entities:
                if rel.ein not in visited:
                    child = await build_node(rel.ein, current_depth + 1)
                    children.append(child)

        return EntityNode(
            ein=clean_ein,
            name=name,
            city=city,
            state=state,
            total_revenue=int(revenue) if revenue else None,
            filing_year=int(filing_year) if filing_year else None,
            related_entities=related_entities,
            children=children,
        )

    root = await build_node(req.ein, 1)

    return EntityGraphResponse(
        root=root,
        total_entities_found=total,
        depth_reached=depth,
    )


@app.get("/debug/connectivity")
async def debug_connectivity():
    """Test EFTS and IRS S3 connectivity from this server."""
    import httpx
    results = {}

    # Test EFTS
    efts_url = "https://efts.irs.gov/LATEST/search-index?q=%22340714585%22&forms=990"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(efts_url)
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            if hits:
                obj_id = hits[0].get("_source", {}).get("ObjectId")
                results["efts"] = {"status": resp.status_code, "object_id": obj_id, "hit_count": len(hits)}
            else:
                results["efts"] = {"status": resp.status_code, "hits": 0}
    except Exception as e:
        results["efts"] = {"error": str(e)}

    # Test S3 with EFTS object ID if we got one
    obj_id = results.get("efts", {}).get("object_id")
    if obj_id:
        s3_url = f"https://s3.amazonaws.com/irs-form-990/{obj_id}_public.xml"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(s3_url)
                results["s3"] = {"status": resp.status_code, "url": s3_url, "bytes": len(resp.content)}
        except Exception as e:
            results["s3"] = {"error": str(e)}

    # Test apps.irs.gov index
    index_url = "https://apps.irs.gov/pub/epostcard/990/xml/2023/index_2023.csv"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(index_url)
            # Just grab first 200 chars to confirm it works
            results["irs_index"] = {"status": resp.status_code, "preview": resp.text[:200]}
    except Exception as e:
        results["irs_index"] = {"error": str(e)}

    return results


@app.get("/search")
async def search_by_name(name: str):
    """Quick name search to find an EIN before doing a full lookup."""
    from .propublica import search_organization
    results = await search_organization(name)
    return {
        "results": [
            {
                "ein": str(r.get("ein", "")),
                "name": r.get("name"),
                "city": r.get("city"),
                "state": r.get("state"),
            }
            for r in results[:10]
        ]
    }
