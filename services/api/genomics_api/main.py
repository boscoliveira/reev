import os
import json
import time
from typing import Any, Dict, List, Optional

import duckdb
import orjson
from fastapi import FastAPI, HTTPException
from opensearchpy import OpenSearch
from pydantic import BaseModel, Field
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse

DATA_ROOT = os.getenv("DATA_ROOT", "/data/parquet")
OPENSEARCH_URL = os.getenv("OPENSEARCH_URL", "http://localhost:9200")
INDEX_NAME = os.getenv("INDEX_NAME", "variants")
REPORTING_URL = os.getenv("REPORTING_URL", "")

# DuckDB connection (lazy)
_duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None


def get_duckdb() -> duckdb.DuckDBPyConnection:
    global _duckdb_conn
    if _duckdb_conn is None:
        _duckdb_conn = duckdb.connect(database=":memory:")
        _duckdb_conn.execute("INSTALL httpfs; LOAD httpfs;")
    return _duckdb_conn


def get_os() -> OpenSearch:
    return OpenSearch(hosts=[OPENSEARCH_URL], verify_certs=False)


class PageRequest(BaseModel):
    size: int = Field(100, ge=1, le=200)
    cursor: Optional[str] = None


class FilterClause(BaseModel):
    field: str
    op: str
    value: Any


class FilterGroup(BaseModel):
    op: str = Field(..., description="AND/OR")
    clauses: List[FilterClause] = Field(default_factory=list)
    groups: List["FilterGroup"] = Field(default_factory=list)


FilterGroup.model_rebuild()


class FilterRequest(BaseModel):
    project_id: str
    filters: Optional[FilterGroup] = None
    page: PageRequest = PageRequest()
    sort: Optional[List[Dict[str, str]]] = None


class VariantExportRequest(BaseModel):
    project_id: str
    variant_ids: List[str]
    format: str = Field("JSON", pattern="^(JSON|CSV)$")
    export_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


app = FastAPI(title="Genomics API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/api/filter/query")
def filter_query(req: FilterRequest):
    # Use OpenSearch for doc IDs by filter, then fetch Parquet rows via DuckDB
    client = get_os()
    must: List[Dict[str, Any]] = []
    if req.filters:
        must = _build_os_query(req.filters)
    body = {"query": {"bool": {"must": must}}}
    if req.sort:
        body["sort"] = req.sort
    size = req.page.size
    if req.page.cursor:
        body["search_after"] = json.loads(req.page.cursor)
    body["size"] = size
    body["track_total_hits"] = True

    res = client.search(index=_index_for_project(req.project_id), body=body)
    hits = res["hits"]["hits"]
    total = res["hits"]["total"]["value"]
    next_cursor = hits[-1]["sort"] if hits else None
    variant_ids = [h["_id"] for h in hits]

    # Fetch rows from Parquet via DuckDB
    if variant_ids:
        df = _fetch_variants_from_parquet(req.project_id, variant_ids)
        rows = json.loads(df.to_json(orient="records"))
    else:
        rows = []

    return JSONResponse(
        content={
            "total": total,
            "next_cursor": json.dumps(next_cursor) if next_cursor else None,
            "items": rows,
        }
    )


@app.post("/api/facets")
def facet_counts(req: FilterRequest):
    client = get_os()
    must: List[Dict[str, Any]] = []
    if req.filters:
        must = _build_os_query(req.filters)
    aggs = {
        "by_gene": {"terms": {"field": "csq.symbol.keyword", "size": 10000}},
        "by_consequence": {"terms": {"field": "csq.consequence.keyword", "size": 10000}},
        "by_clinsig": {"terms": {"field": "clinvar.clinsig.keyword", "size": 1000}},
    }
    body = {"size": 0, "query": {"bool": {"must": must}}, "aggs": aggs}
    res = client.search(index=_index_for_project(req.project_id), body=body)
    return res["aggregations"]


@app.get("/api/variant/{project_id}/{variant_id}")
def variant_detail(project_id: str, variant_id: str):
    df = _fetch_variants_from_parquet(project_id, [variant_id])
    if df.empty:
        raise HTTPException(status_code=404, detail="Variant not found")
    return json.loads(df.to_json(orient="records"))[0]


@app.post("/api/export")
def export_variants(req: VariantExportRequest):
    df = _fetch_variants_from_parquet(req.project_id, req.variant_ids)
    export_id = req.export_id or f"exp-{int(time.time()*1000)}"
    # Audit log
    _write_audit({
        "export_id": export_id,
        "project_id": req.project_id,
        "variant_count": len(req.variant_ids),
        "metadata": req.metadata,
        "timestamp": time.time(),
    })
    if req.format.upper() == "CSV":
        return JSONResponse(
            media_type="text/csv",
            content=df.to_csv(index=False),
        )
    # default JSON
    return JSONResponse(content=json.loads(df.to_json(orient="records")))


def _index_for_project(project_id: str) -> str:
    return f"{INDEX_NAME}-{project_id}".lower()


def _build_os_query(group: FilterGroup) -> List[Dict[str, Any]]:
    def clause_to_query(c: FilterClause) -> Dict[str, Any]:
        op = c.op.lower()
        if op in {"eq", "term"}:
            return {"term": {c.field: c.value}}
        if op in {"in"}:
            return {"terms": {c.field: c.value}}
        if op in {"lt", "lte", "gt", "gte"}:
            return {"range": {c.field: {op: c.value}}}
        if op in {"match"}:
            return {"match": {c.field: c.value}}
        raise HTTPException(400, f"Unsupported op: {c.op}")

    sub = []
    for c in group.clauses:
        sub.append(clause_to_query(c))
    for g in group.groups:
        sub.append({"bool": {"must" if g.op.upper()=="AND" else "should": _build_os_query(g)}})
    key = "must" if group.op.upper() == "AND" else "should"
    return [{"bool": {key: sub}}] if sub else []


def _fetch_variants_from_parquet(project_id: str, variant_ids: List[str]):
    conn = get_duckdb()
    parquet_root = os.path.join(DATA_ROOT, project_id)
    # Read Parquet and filter by variant_id
    query = f"""
        SELECT *
        FROM read_parquet('{parquet_root}/**/*.parquet', hive_partitioning=1)
        WHERE variant_id IN ({','.join([repr(v) for v in variant_ids])})
    """
    return conn.execute(query).df()


def _write_audit(event: Dict[str, Any]):
    audit_path = os.path.join(DATA_ROOT, "audit.log.ndjson")
    with open(audit_path, "a", encoding="utf-8") as fh:
        fh.write(orjson.dumps(event).decode("utf-8") + "\n")


def run():
    import uvicorn

    uvicorn.run("genomics_api.main:app", host="0.0.0.0", port=8000, reload=False)