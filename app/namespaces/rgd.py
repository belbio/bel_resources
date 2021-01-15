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

log = structlog.getLogger("rgd_namespace")

# Globals

namespace = "RGD"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

species_key = "TAX:10116"

download_url = "ftp://ftp.rgd.mcw.edu/pub/data_release/GENES_RAT.txt"
download_fn = f"{settings.DOWNLOAD_DIR}/rgd.txt.gz"
resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"


def build_json():
    """Build RGD namespace json load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    species_labels = get_species_labels()

    # Map gene_types to BEL entity types
    bel_entity_type_map = {
        "pseudo": ["Gene", "RNA"],
        "pseudogene": ["Gene", "RNA"],
        "transcribed_processed_pseudogene": ["Gene", "RNA"],
        "transcribed_unprocessed_pseudogene": ["Gene", "RNA"],
        "protein-coding": ["Gene", "RNA", "Protein"],
        "ribozyme": ["Gene", "RNA", "Protein"],
        "ncrna": ["Gene", "RNA"],
        "gene": ["Gene", "RNA", "Protein"],
        "snrna": ["Gene", "RNA"],
        "trna": ["Gene", "RNA"],
        "mt_trna": ["Gene", "RNA"],
        "rrna": ["Gene", "RNA"],
        "mt_rrna": ["Gene", "RNA"],
        "processed_transcript": ["Gene", "RNA"],
        "misc_rna": ["Gene", "RNA"],
        "mirna": ["Gene", "RNA"],
        "lincrna": ["Gene", "RNA"],
        "processed_transcript": [],
        "processed_pseudogene": [],
        "unprocessed_pseudogene": [],
        "antisense": [],
        "sense_intronic": [],
        "snorna": [],
        "scarna": [],
        "tec": [],
    }

    with gzip.open(download_fn, "rt") as fi, gzip.open(resource_fn, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        for line in fi:
            if re.match(
                "#|GENE_RGD_ID", line, flags=0
            ):  # many of the file header lines are comments
                continue

            cols = line.split("\t")
            (
                rgd_id,
                symbol,
                name,
                desc,
                ncbi_gene_id,
                uniprot_id,
                old_symbols,
                old_names,
                gene_type,
            ) = (
                cols[0],
                cols[1],
                cols[2],
                cols[3],
                cols[20],
                cols[21],
                cols[29],
                cols[30],
                cols[36],
            )
            # print(f'ID: {rgd_id}, S: {symbol}, N: {name}, D: {desc}, nbci_gene_id: {ncbi_gene_id}, up: {uniprot_id}, old_sym: {old_symbols}, old_names: {old_names}, gt: {gene_type}')

            synonyms = [val for val in old_symbols.split(";") + old_names.split(";") if val]

            equivalences = []
            if ncbi_gene_id:
                equivalences.append(f"EG:{ncbi_gene_id}")
            if uniprot_id:
                uniprots = uniprot_id.split(";")
                for uniprot in uniprots:
                    equivalences.append(f"SP:{uniprot}")

            entity_types = []
            if gene_type not in bel_entity_type_map:
                log.error(f"New RGD gene_type not found in bel_entity_type_map {gene_type}")
                continue
            else:
                entity_types = bel_entity_type_map[gene_type]

            term = Term(
                key=f"{namespace}:{rgd_id}",
                namespace=namespace,
                id=rgd_id,
                label=symbol,
                name=name,
                description=desc,
                species_key=species_key,
                species_label=species_labels[species_key],
                entity_types=copy.copy(entity_types),
                equivalence_keys=copy.copy(equivalences),
                synonyms=copy.copy(synonyms),
                alt_keys=[f"{namespace}:{symbol}"],
            )

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
