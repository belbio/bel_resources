#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  hgnc.py

"""
import copy
import datetime
import gzip
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import TextIO

import structlog
import yaml

import app.settings as settings
import app.setup_logging
import typer
from app.common.collect_sources import get_ftp_file
from app.common.resources import get_metadata, get_species_labels
from app.common.text import quote_id
from app.schemas.main import Term
from typer import Option

log = structlog.getLogger("hgnc_namespace")

# Globals

namespace = "HGNC"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

species_key = "TAX:9606"

download_url = "ftp://ftp.ebi.ac.uk/pub/databases/genenames/new/json/hgnc_complete_set.json"
download_fn = f"{settings.DOWNLOAD_DIR}/hgnc.json.gz"
resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"


def build_json():
    """Build term json load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    species_labels = get_species_labels()

    # Map gene_types to BEL entity types
    bel_entity_type_map = {
        "gene with protein product": ["Gene", "RNA", "Protein"],
        "RNA, cluster": ["Gene", "RNA"],
        "RNA, long non-coding": ["Gene", "RNA"],
        "RNA, micro": ["Gene", "RNA", "Micro_RNA"],
        "RNA, ribosomal": ["Gene", "RNA"],
        "RNA, small cytoplasmic": ["Gene", "RNA"],
        "RNA, small misc": ["Gene", "RNA"],
        "RNA, small nuclear": ["Gene", "RNA"],
        "RNA, small nucleolar": ["Gene", "RNA"],
        "RNA, transfer": ["Gene", "RNA"],
        "phenotype only": ["Gene"],
        "RNA, pseudogene": ["Gene", "RNA"],
        "T-cell receptor pseudogene": ["Gene", "RNA"],
        "T cell receptor pseudogene": ["Gene", "RNA"],
        "immunoglobulin pseudogene": ["Gene", "RNA"],
        "pseudogene": ["Gene", "RNA"],
        "T-cell receptor gene": ["Gene", "RNA", "Protein"],
        "T cell receptor gene": ["Gene", "RNA", "Protein"],
        "complex locus constituent": ["Gene", "RNA", "Protein"],
        "endogenous retrovirus": ["Gene"],
        "fragile site": ["Gene"],
        "immunoglobulin gene": ["Gene", "RNA", "Protein"],
        "protocadherin": ["Gene", "RNA", "Protein"],
        "readthrough": ["Gene", "RNA"],
        "region": ["Gene"],
        "transposable element": ["Gene"],
        "unknown": ["Gene", "RNA", "Protein"],
        "virus integration site": ["Gene"],
        "RNA, micro": ["Gene", "RNA", "Micro_RNA"],
        "RNA, misc": ["Gene", "RNA"],
        "RNA, Y": ["Gene", "RNA"],
        "RNA, vault": ["Gene", "RNA"],
    }

    with gzip.open(download_fn, "rt") as fi, gzip.open(resource_fn, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        orig_data = json.load(fi)

        for doc in orig_data["response"]["docs"]:

            # Skip unused entries
            if doc["status"] != "Approved":
                continue

            hgnc_id = doc["hgnc_id"].replace("HGNC:", "")

            term = Term(
                key=f"{namespace}:{hgnc_id}",
                namespace=namespace,
                id=hgnc_id,
                label=doc["symbol"],
                name=doc["name"],
                species_key=species_key,
                species_label=species_labels[species_key],
            )

            # "alt_ids": [utils.get_prefixed_id(ns_prefix, hgnc_id)],
            term.alt_ids = [f"{namespace}:{doc['symbol']}"]

            # Synonyms
            term.synonyms.extend(doc.get("synonyms", []))
            term.synonyms.extend(doc.get("alias_symbol", []))
            term.synonyms.extend(doc.get("alias_name", []))
            term.synonyms.extend(doc.get("prev_name", []))

            # Equivalences
            for _id in doc.get("uniprot_ids", []):
                term.equivalence_keys.append(f"SP:{_id}")
                term.equivalence_keys.append(f"uniprot:{_id}")

            if "entrez_id" in doc:
                term.equivalence_keys.append(f"EG:{doc['entrez_id']}")

            for _id in doc.get("refseq_accession", []):
                term.equivalence_keys.append(f"refseq:{_id}")

            if "ensembl_gene_id" in doc:
                term.equivalence_keys.append(f"ensembl:{doc['ensembl_gene_id']}")

            if "orphanet" in doc:
                term.equivalence_keys.append(f"orphanet:{doc['orphanet']}")

            # Entity types
            if doc["locus_type"] in bel_entity_type_map:
                term.entity_types = bel_entity_type_map[doc["locus_type"]]
            else:
                log.error(
                    f'New HGNC locus_type not found in bel_entity_type_map {doc["locus_type"]}'
                )

            # Obsolete Namespace IDs
            if "prev_symbol" in doc:
                for obs_id in doc["prev_symbol"]:
                    term.obsolete_keys.append(f"{namespace}:{quote_id(obs_id)}")

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({"term": term.dict()})))


def main(
    overwrite: bool = Option(False, help="Force overwrite of output resource data file"),
    force_download: bool = Option(False, help="Force re-downloading of source data file"),
):

    (changed, msg) = get_ftp_file(download_url, download_fn, force_download=force_download)

    if msg:
        log.info("Collect download file", result=msg, changed=changed)

    if changed or overwrite:
        build_json()


if __name__ == "__main__":
    typer.run(main)
