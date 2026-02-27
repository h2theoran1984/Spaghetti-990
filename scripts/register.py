"""
Register the 990 Entity Graph agent on SignalPot.

Usage:
  SP_API_KEY=sp_live_... python scripts/register.py

Set SP_BASE_URL to override (default: https://www.signalpot.dev)
"""
import os
import sys
import json
import urllib.request

BASE_URL = os.environ.get("SP_BASE_URL", "https://www.signalpot.dev")
API_KEY = os.environ.get("SP_API_KEY")

if not API_KEY:
    print("❌  SP_API_KEY environment variable is required.")
    sys.exit(1)

AGENT = {
    "name": "Spaghetti 990",
    "slug": "990-entity-graph",
    "description": (
        "Healthcare org structures look like spaghetti. Spaghetti 990 untangles them. "
        "Feed it one EIN, get back the full org tree from IRS Form 990 Schedule R. "
        "Health systems, foundations, subsidiaries, shell LLCs — nothing hides from Schedule R."
    ),
    "tags": ["healthcare", "990", "nonprofit", "irs", "entity-graph", "schedule-r"],
    "rate_type": "per_call",
    "rate_amount": 0.005,
    "auth_type": "api_key",
    "capability_schema": [
        {
            "name": "lookup",
            "description": "Look up related organizations for a given EIN from IRS Form 990 Schedule R",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "ein": {
                        "type": "string",
                        "description": "Employer Identification Number (EIN) of the organization, with or without dashes",
                        "examples": ["34-0714585", "340714585"],
                    },
                    "depth": {
                        "type": "integer",
                        "description": "How many levels deep to traverse the org tree (1-3). Higher depth = more API calls.",
                        "default": 1,
                        "minimum": 1,
                        "maximum": 3,
                    },
                },
                "required": ["ein"],
            },
            "outputSchema": {
                "type": "object",
                "properties": {
                    "root": {
                        "type": "object",
                        "properties": {
                            "ein": {"type": "string"},
                            "name": {"type": "string"},
                            "city": {"type": "string"},
                            "state": {"type": "string"},
                            "total_revenue": {"type": "integer"},
                            "filing_year": {"type": "integer"},
                            "related_entities": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "ein": {"type": "string"},
                                        "name": {"type": "string"},
                                        "relationship": {"type": "string"},
                                        "controlling_pct": {"type": "number"},
                                    },
                                },
                            },
                        },
                    },
                    "total_entities_found": {"type": "integer"},
                    "depth_reached": {"type": "integer"},
                },
            },
        }
    ],
}


def api_request(method: str, path: str, body=None):
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {API_KEY}",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


print(f"Registering agent on {BASE_URL}...")
status, data = api_request("POST", "/api/agents", AGENT)

if status == 201:
    print(f"✅  Agent registered! ID: {data.get('id')}")
    print(f"    View at: {BASE_URL}/agents/990-entity-graph")
elif status == 409:
    print("⚠️   Agent already exists. Updating...")
    status2, data2 = api_request("PATCH", "/api/agents/990-entity-graph", {
        "name": AGENT["name"],
        "description": AGENT["description"],
        "tags": AGENT["tags"],
    })
    if status2 == 200:
        print("✅  Agent updated.")
    else:
        print(f"❌  Update failed ({status2}): {data2}")
else:
    print(f"❌  Failed ({status}): {data}")
