#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  eg.py

"""

import copy
import datetime
import gzip
import json
import os
import re

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

log = structlog.getLogger("eg_namespace")

# Globals

namespace = "EG"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

download_url = "ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/All_Data.gene_info.gz"
download_history_url = "ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_history.gz"
download_fn = f"{settings.DOWNLOAD_DIR}/eg.csv.gz"
download_history_fn = f"{settings.DOWNLOAD_DIR}/eg_gene_history.gz"
resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"
resource_fn_hmrz = f"{settings.DATA_DIR}/namespaces/{namespace_lc}_hmrz.jsonl.gz"
hmrz_species = ["TAX:9606", "TAX:10090", "TAX:10116", "TAX:7955"]


def get_history():
    """Get history of gene records

    Returns:
        Mapping[str, Mapping[str, int]]: history dict of dicts - new gene_id and old_gene_id
    """

    history = {}
    with gzip.open(download_history_fn, "rt") as fi:

        fi.__next__()  # skip header line

        for line in fi:
            cols = line.split("\t")

            (gene_id, old_gene_id,) = (
                cols[1],
                cols[2],
            )
            if gene_id != "-":
                if history.get(gene_id, None):
                    history[gene_id] = {old_gene_id: 1}

    return history


def build_json():
    """Build EG namespace json load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    metadata = get_metadata(namespace_def)
    history = get_history()

    collect_prefixes = {}

    species_labels = get_species_labels()

    missing_entity_types = {}
    bel_entity_type_map = {
        "snoRNA": ["Gene", "RNA"],
        "snRNA": ["Gene", "RNA"],
        "ncRNA": ["Gene", "RNA"],
        "tRNA": ["Gene", "RNA"],
        "scRNA": ["Gene", "RNA"],
        "other": ["Gene"],
        "pseudo": ["Gene", "RNA"],
        "unknown": ["Gene", "RNA", "Protein"],
        "protein-coding": ["Gene", "RNA", "Protein"],
        "rRNA": ["Gene", "RNA"],
    }

    with gzip.open(download_fn, "rt") as fi, gzip.open(
        resource_fn, "wt"
    ) as fo, gzip.open(resource_fn_hmrz, "wt") as fz:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))
        fz.write("{}\n".format(json.dumps({"metadata": metadata})))

        fi.__next__()  # skip header line

        for line in fi:

            cols = line.split("\t")
            (tax_src_id, gene_id, symbol, syns, dbxrefs, desc, gene_type, name) = (
                cols[0],
                cols[1],
                cols[2],
                cols[4],
                cols[5],
                cols[8],
                cols[9],
                cols[11],
            )
            species_key = f"TAX:{tax_src_id}"

            # Process synonyms
            syns = syns.rstrip()
            if syns:
                synonyms = syns.split("|")

            # Process equivalences
            equivalence_keys = []
            dbxrefs = dbxrefs.rstrip()
            if dbxrefs == "-":
                dbxrefs = None
            else:
                dbxrefs = dbxrefs.split("|")
            if dbxrefs is not None:
                for dbxref in dbxrefs:
                    if "Ensembl:" in dbxref:
                        dbxref = dbxref.replace("Ensembl", "ensembl")
                        equivalence_keys.append(dbxref)
                    elif "MGI:MGI" in dbxref:
                        dbxref = dbxref.replace("MGI:MGI:", "MGI:")
                        equivalence_keys.append(dbxref)
                    elif "VGNC:VGNC:" in dbxref:
                        dbxref = dbxref.replace("VGNC:VGNC:", "VGNC:")
                        equivalence_keys.append(dbxref)
                    elif "HGNC:HGNC:" in dbxref:
                        dbxref = dbxref.replace("HGNC:HGNC:", "HGNC:")
                        equivalence_keys.append(dbxref)
                    else:
                        (prefix, rest) = dbxref.split(":")
                        collect_prefixes[prefix] = 1

            if gene_type in ["miscRNA", "biological-region"]:  # Skip gene types
                continue
            elif gene_type not in bel_entity_type_map:
                log.error(f"Unknown gene_type found {gene_type}")
                missing_entity_types[gene_type] = 1
                entity_types = None
            else:
                entity_types = bel_entity_type_map[gene_type]

            if name == "-":
                name = symbol

            term = Term(
                key=f"{namespace}:{gene_id}",
                namespace=namespace,
                id=gene_id,
                label=symbol,
                name=name,
                description=desc,
                species_key=species_key,
                species_label=species_labels.get(species_key, ""),
                equivalence_keys=copy.copy(equivalence_keys),
                synonyms=copy.copy(synonyms),
            )

            if entity_types:
                term.entity_types = copy.copy(entity_types)

            # TODO - check that this is working correctly
            if gene_id in history:
                term.obsolete_keys = [
                    f"{namespace}:{obs_id}" for obs_id in history[gene_id].keys()
                ]

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({"term": term.dict()})))

            if species_key in hmrz_species:
                fz.write("{}\n".format(json.dumps({"term": term.dict()})))

    log.info(f"Equivalence Prefixes {json.dumps(collect_prefixes, indent=4)}")

    if missing_entity_types:
        log.error("Missing Entity Types:\n", json.dumps(missing_entity_types))


def main(
    overwrite: bool = Option(
        False, help="Force overwrite of output resource data file"
    ),
    force_download: bool = Option(
        False, help="Force re-downloading of source data file"
    ),
):

    (changed, msg) = get_ftp_file(
        download_url, download_fn, force_download=force_download
    )
    (changed_history, msg_history) = get_ftp_file(
        download_history_url, download_history_fn, force_download=force_download
    )

    if msg:
        log.info("Collect download file", result=msg, changed=changed)

    if changed or overwrite:
        build_json()


if __name__ == "__main__":
    typer.run(main)
