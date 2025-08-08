import argparse
import gzip
import os
import re
import sys
from datetime import datetime
from typing import Dict, Iterable, List, Tuple

import orjson
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
from opensearchpy import OpenSearch, helpers

CSQ_KEY = "CSQ"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Ingest VEP-annotated VCF to Parquet and OpenSearch")
    ap.add_argument("--project-id", required=True)
    ap.add_argument("--vcf", required=True, help="Path to VCF(.gz)")
    ap.add_argument("--out-root", required=True, help="Output Parquet root directory")
    ap.add_argument("--opensearch-url", default="http://localhost:9200")
    ap.add_argument("--index-name", default="variants")
    ap.add_argument("--batch-size", type=int, default=5000)
    return ap.parse_args()


def open_maybe_gzip(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt")
    return open(path, "rt")


def parse_header_for_csq_order(lines: Iterable[str]) -> Tuple[List[str], List[str]]:
    header_lines: List[str] = []
    csq_fields: List[str] = []
    for line in lines:
        if not line.startswith("#"):
            # body begins
            return header_lines, csq_fields
        header_lines.append(line)
        if line.startswith("##INFO=<ID=CSQ"):
            m = re.search(r"Format: ([^\">]+)", line)
            if m:
                csq_fields = m.group(1).strip().split("|")
    return header_lines, csq_fields


def compute_variant_id(chrom: str, pos: str, ref: str, alt: str) -> str:
    return f"{chrom}:{pos}:{ref}>{alt}".lower()


def write_parquet(project_id: str, records: List[Dict[str, object]], root: str):
    if not records:
        return
    df = pd.DataFrame.from_records(records)
    table = pa.Table.from_pandas(df, preserve_index=False)
    # partition by chromosome and year_month
    year_month = datetime.utcnow().strftime("%Y_%m")
    partitioning = ds.partitioning(pa.schema([("chrom", pa.string()), ("year_month", pa.string())]))
    outdir = os.path.join(root, project_id)
    ds.write_dataset(
        table,
        base_dir=outdir,
        format="parquet",
        partitioning=partitioning,
        basename_template="part-{i}.parquet",
        partition_cols=["chrom", "year_month"],
        existing_data_behavior="overwrite_or_ignore",
    )


def index_opensearch(project_id: str, docs: List[Dict[str, object]], client: OpenSearch, index_name: str):
    if not docs:
        return
    index = f"{index_name}-{project_id}".lower()

    actions = (
        {
            "_op_type": "index",
            "_index": index,
            "_id": d["variant_id"],
            "_source": d,
        }
        for d in docs
    )
    helpers.bulk(client, actions, chunk_size=2000, request_timeout=120)


def main():
    args = parse_args()
    os.makedirs(os.path.join(args.out_root, args.project_id), exist_ok=True)

    client = OpenSearch(hosts=[args.opensearch_url], verify_certs=False)
    # ensure index exists
    try:
        from genomics_api.index_bootstrap import ensure_index
    except Exception:
        ensure_index = None  # type: ignore
    if ensure_index is not None:
        ensure_index(client, f"{args.index_name}-{args.project_id}".lower())
    month = datetime.utcnow().strftime("%Y_%m")

    batch_records: List[Dict[str, object]] = []
    batch_docs: List[Dict[str, object]] = []

    with open_maybe_gzip(args.vcf) as fh:
        # parse header to get CSQ order
        header, csq_order = parse_header_for_csq_order(fh)
        if not csq_order:
            print("ERROR: CSQ layout not found in header", file=sys.stderr)
            sys.exit(2)
        # continue reading from after header
        for line in fh:
            if not line or line.startswith("#"):
                continue
            cols = line.rstrip("\n").split("\t")
            chrom, pos, _id, ref, alt, qual, flt, info = cols[:8]
            pos_i = int(pos)
            info_map = dict(kv.split("=", 1) if "=" in kv else (kv, True) for kv in info.split(";") if kv)
            csqs = info_map.get(CSQ_KEY, "").split(",") if info_map.get(CSQ_KEY) else []
            first_csq = csqs[0].split("|") if csqs else []
            csq = {csq_order[i]: (first_csq[i] if i < len(first_csq) else None) for i in range(len(csq_order))}

            variant_id = compute_variant_id(chrom, pos, ref, alt)
            record = {
                "project_id": args.project_id,
                "chrom": chrom,
                "pos": pos_i,
                "ref": ref,
                "alt": alt,
                "variant_id": variant_id,
                "rsid": _id if _id != "." else None,
                "qual": float(qual) if qual not in (".", "") else None,
                "filters": flt,
                "csq": csq,
                "year_month": month,
            }
            batch_records.append(record)
            # minimal index doc
            doc = {
                "variant_id": variant_id,
                "chrom": chrom,
                "pos": pos_i,
                "csq": {
                    "symbol": csq.get("SYMBOL"),
                    "consequence": csq.get("Consequence"),
                    "impact": csq.get("IMPACT"),
                },
                "clinvar": {},
                "population": {},
            }
            batch_docs.append(doc)

            if len(batch_records) >= args.batch_size:
                write_parquet(args.project_id, batch_records, args.out_root)
                index_opensearch(args.project_id, batch_docs, client, args.index_name)
                batch_records.clear()
                batch_docs.clear()

    if batch_records:
        write_parquet(args.project_id, batch_records, args.out_root)
        index_opensearch(args.project_id, batch_docs, client, args.index_name)

    print("Ingest completed")