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
from app.common.collect_sources import get_web_file
from app.common.resources import get_metadata, get_species_labels
from app.common.text import quote_id
from app.schemas.main import Term
from typer import Option

log = structlog.getLogger("zfin_namespace")

# Globals

namespace = "ZFIN"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

species_key = "TAX:7955"

download_url = "https://zfin.org/downloads/aliases.txt"
download_url2 = "https://zfin.org/downloads/gene.txt"
download_url3 = "https://zfin.org/downloads/transcripts.txt"

download_fn = f"{settings.DOWNLOAD_DIR}/zfin_aliases.txt.gz"
download_fn2 = f"{settings.DOWNLOAD_DIR}/zfin_gene.txt.gz"
download_fn3 = f"{settings.DOWNLOAD_DIR}/zfin_transcripts.txt.gz"

aliases_fn = download_fn
genes_fn = download_fn2
transcripts_fn = download_fn3

resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"


def build_json():
    """Build term JSONL file"""

    species_labels = get_species_labels()

    terms = {}
    with gzip.open(aliases_fn, "rt") as fi:
        for line in fi:
            if re.match("ZDB-GENE-", line):
                (src_id, name, symbol, syn, *extra) = line.split("\t")
                # print('Syn', syn)
                if src_id in terms:
                    terms[src_id]["synonyms"].append(syn)
                else:
                    terms[src_id] = {"name": name, "symbol": symbol, "synonyms": [syn]}

    with gzip.open(transcripts_fn, "rt") as fi:
        transcript_types = {}
        for line in fi:
            (tscript_id, so_id, name, gene_id, clone_id, tscript_type, status, *extra) = line.split(
                "\t"
            )
            if "withdrawn" in status.lower() or "artifact" in status.lower():
                continue
            if gene_id in transcript_types:
                transcript_types[gene_id][tscript_type] = 1
            else:
                transcript_types[gene_id] = {tscript_type: 1}

        for gene_id in transcript_types:
            types = transcript_types[gene_id].keys()

            entity_types = []
            for type_ in types:
                if type_ in [
                    "lincRNA",
                    "ncRNA",
                    "scRNA",
                    "snRNA",
                    "snoRNA",
                    "antisense",
                    "aberrant processed transcript",
                    "pseudogenic transcript",
                ]:
                    entity_types.extend(["Gene", "RNA"])
                elif type_ in ["mRNA", "V-gene"]:
                    entity_types.extend(["Gene", "RNA", "Protein"])
                elif type_ in ["miRNA"]:
                    entity_types.extend(["Gene", "Micro_RNA"])
                else:
                    print(f"Unknown gene type: {type_}")

            entity_types = list(set(entity_types))

            if gene_id in terms:
                terms[gene_id]["entity_types"] = list(entity_types)
            else:
                terms[gene_id] = {"name": name, "entity_types": list(entity_types)}

    with gzip.open(genes_fn, "rt") as fi:
        for line in fi:
            (src_id, so_id, symbol, eg_id, *extra) = line.split("\t")
            if src_id in terms:
                terms[src_id]["equivalences"] = [f"EG:{eg_id.strip()}"]
                if terms[src_id].get("symbol", None) and symbol:
                    terms[src_id]["symbol"] = symbol
            else:
                log.debug(f"No term record for ZFIN {src_id} to add equivalences to")
                continue

    with gzip.open(resource_fn, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        for term_id in terms:

            label = terms[term_id].get("symbol", terms[term_id].get("name", term_id))
            name = terms[term_id].get("name", term_id)

            term = Term(
                key=f"{namespace}:{term_id}",
                namespace=namespace,
                id=term_id,
                label=label,
                name=name,
                species_key=species_key,
                species_label=species_labels[species_key],
                synonyms=list(set(terms[term_id].get("synonyms", []))),
                entity_types=terms[term_id].get("entity_types", []),
                equivalence_keys=terms[term_id].get("equivalences", []),
            )

            if term.id != term.label:
                term.alt_keys = [f"{namespace}:{term.label}"]

            # Add term to JSONLines file
            fo.write("{}\n".format(json.dumps({"term": term.dict()})))


def main(
    overwrite: bool = Option(False, help="Force overwrite of output resource data file"),
    force_download: bool = Option(False, help="Force re-downloading of source data file"),
):

    (changed, msg) = get_web_file(download_url, download_fn, force_download=force_download)
    (changed2, msg2) = get_web_file(download_url2, download_fn2, force_download=force_download)
    (changed3, msg3) = get_web_file(download_url3, download_fn3, force_download=force_download)

    if msg:
        log.info("Collect download file", result=msg, changed=changed)

    if changed or overwrite:
        build_json()


if __name__ == "__main__":
    typer.run(main)
