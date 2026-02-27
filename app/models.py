from pydantic import BaseModel


class RelatedEntity(BaseModel):
    ein: str
    name: str
    relationship: str        # e.g. "Parent", "Subsidiary", "Brother/Sister"
    controlling_pct: float | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None
    total_revenue: int | None = None  # from ProPublica if available


class EntityNode(BaseModel):
    ein: str
    name: str
    city: str | None = None
    state: str | None = None
    total_revenue: int | None = None
    filing_year: int | None = None
    related_entities: list[RelatedEntity] = []
    children: list["EntityNode"] = []  # populated when depth > 1


class EntityGraphRequest(BaseModel):
    ein: str
    depth: int = 1           # 1 = direct relations only, 2+ = recursive


class EntityGraphResponse(BaseModel):
    root: EntityNode
    total_entities_found: int
    depth_reached: int
    data_source: str = "IRS Form 990 Schedule R via ProPublica + IRS S3"
