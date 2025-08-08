from opensearchpy import OpenSearch

MAPPINGS = {
    "mappings": {
        "properties": {
            "variant_id": {"type": "keyword"},
            "chrom": {"type": "keyword"},
            "pos": {"type": "integer"},
            "csq": {
                "properties": {
                    "symbol": {"type": "keyword"},
                    "consequence": {"type": "keyword"},
                    "impact": {"type": "keyword"}
                }
            },
            "clinvar": {
                "properties": {
                    "clinsig": {"type": "keyword"},
                    "review_status": {"type": "keyword"}
                }
            },
            "population": {
                "properties": {
                    "gnomad_af": {"type": "float"},
                    "gnomad_popmax_af": {"type": "float"},
                    "gnomad_popmax_pop": {"type": "keyword"}
                }
            }
        }
    }
}


def ensure_index(client: OpenSearch, name: str):
    if not client.indices.exists(index=name):
        client.indices.create(index=name, body=MAPPINGS)