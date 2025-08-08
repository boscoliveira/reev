# Genomics Explorer (tertiary analysis) — Minimal Scaffold

This adds a minimal stack for a tertiary genomic analysis web app:
- Ingestor CLI to parse VEP-annotated VCF into Parquet partitions and OpenSearch index
- FastAPI service for filter, facet, variant detail and export
- Next.js UI with a simple query page
- Docker Compose for OpenSearch, API and UI

## Prerequisites
- Docker and Docker Compose

## Start stack

```bash
docker compose up --build
```

- OpenSearch: http://localhost:9200
- API: http://localhost:8000/api/healthz
- UI: http://localhost:3000

## Ingest a VEP-annotated VCF

```bash
docker build -t ingestor ./services/ingestor
mkdir -p ./data/parquet
# Example
docker run --rm \
  -v $(pwd)/data/parquet:/out \
  -v /path/to/your.vcf.gz:/vcf.gz:ro \
  --net host \
  ingestor \
  --project-id demo \
  --vcf /vcf.gz \
  --out-root /out \
  --opensearch-url http://localhost:9200
```

## Query
- UI Variants page can query by gene symbol.
- API endpoints:
  - POST /api/filter/query
  - POST /api/facets
  - GET /api/variant/{project_id}/{variant_id}
  - POST /api/export (CSV/JSON)

## Notes
- Parquet partitioning: {project}/{chrom}/{year_month}
- OpenSearch index: `variants-{project_id}` with keyword fields for common facets
- DuckDB reads Parquet for row-level detail and export paths