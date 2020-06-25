#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  mgi.py

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

log = structlog.getLogger(__name__)

# Globals

namespace = "MGI"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

species_key = "TAX:10090"

# File descriptions: http://www.informatics.jax.org/downloads/reports/index.html
# http://www.informatics.jax.org/downloads/reports/MRK_List1.rpt (including withdrawn marker symbols)
# http://www.informatics.jax.org/downloads/reports/MRK_List2.rpt (excluding withdrawn marker symbols)
download_url = "http://www.informatics.jax.org/downloads/reports/MRK_List2.rpt"
download_url2 = "http://www.informatics.jax.org/downloads/reports/MRK_SwissProt.rpt"  # Equivalences
download_url3 = "http://www.informatics.jax.org/downloads/reports/MGI_EntrezGene.rpt"  # Equivalences

download_fn = f"{settings.DOWNLOAD_DIR}/mgi_MRK_List2.rpt.gz"
download_fn2 = f"{settings.DOWNLOAD_DIR}/mgi_MRK_SwissProt.rpt.gz"
download_fn3 = f"{settings.DOWNLOAD_DIR}/mgi_MGI_EntrezGene.rpt.gz"

resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"


def build_json():
    """Build RGD namespace json load file"""

    species_labels = get_species_labels()

    # Map gene_types to BEL entity types
    bel_entity_type_map = {
        "gene": ["Gene", "RNA", "Protein"],
        "protein coding gene": ["Gene", "RNA", "Protein"],
        "non-coding RNA gene": ["Gene", "RNA"],
        "rRNA gene": ["Gene", "RNA"],
        "tRNA gene": ["Gene", "RNA"],
        "snRNA gene": ["Gene", "RNA"],
        "snoRNA gene": ["Gene", "RNA"],
        "miRNA gene": ["Gene", "RNA", "Micro_RNA"],
        "scRNA gene": ["Gene", "RNA"],
        "lincRNA gene": ["Gene", "RNA"],
        "lncRNA gene": ["Gene", "RNA"],
        "intronic lncRNA gene": ["Gene", "RNA"],
        "sense intronic lncRNA gene": ["Gene", "RNA"],
        "sense overlapping lncRNA gene": ["Gene", "RNA"],
        "bidirectional promoter lncRNA gene": ["Gene", "RNA"],
        "antisense lncRNA gene": ["Gene", "RNA"],
        "ribozyme gene": ["Gene", "RNA"],
        "RNase P RNA gene": ["Gene", "RNA"],
        "RNase MRP RNA gene": ["Gene", "RNA"],
        "telomerase RNA gene": ["Gene", "RNA"],
        "unclassified non-coding RNA gene": ["Gene", "RNA"],
        "heritable phenotypic marker": ["Gene"],
        "gene segment": ["Gene"],
        "unclassified gene": ["Gene", "RNA", "Protein"],
        "other feature types": ["Gene"],
        "pseudogene": ["Gene", "RNA"],
        "transgene": ["Gene"],
        "other genome feature": ["Gene"],
        "pseudogenic region": ["Gene", "RNA"],
        "polymorphic pseudogene": ["Gene", "RNA", "Protein"],
        "pseudogenic gene segment": ["Gene", "RNA"],
        "SRP RNA gene": ["Gene", "RNA"],
    }

    # Swissprot equivalents
    sp_eqv = {}
    with gzip.open(download_fn2, "rt") as fi:

        for line in fi:
            cols = line.rstrip().split("\t")
            if cols[2] == "W":
                continue
            (mgi_id, sp_accession) = (cols[0], cols[6])
            mgi_id = mgi_id.replace("MGI:", "")
            sp_eqv[mgi_id] = sp_accession.split(" ")

    # EntrezGene equivalents
    eg_eqv = {}
    with gzip.open(download_fn3, "rt") as fi:

        for line in fi:
            cols = line.split("\t")
            if cols[2] == "W":
                continue
            (mgi_id, eg_id) = (cols[0], cols[8])
            mgi_id = mgi_id.replace("MGI:", "")
            eg_eqv[mgi_id] = [eg_id]

    with gzip.open(download_fn, "rt") as fi, gzip.open(resource_fn, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        firstline = fi.readline()
        firstline = firstline.split("\t")

        for line in fi:
            cols = line.split("\t")
            (mgi_id, symbol, name, marker_type, gene_type, synonyms) = (
                cols[0],
                cols[6],
                cols[8],
                cols[9],
                cols[10],
                cols[11],
            )

            mgi_id = mgi_id.replace("MGI:", "")

            # Skip non-gene entries
            if marker_type != "Gene":
                continue

            synonyms = synonyms.rstrip()
            if synonyms:
                synonyms = synonyms.split("|")

            if gene_type not in bel_entity_type_map:
                log.error(f"Unknown gene_type found {gene_type}")
                entity_types = None
            else:
                entity_types = bel_entity_type_map[gene_type]

            equivalences = []
            if mgi_id in sp_eqv:
                for sp_accession in sp_eqv[mgi_id]:
                    if not sp_accession:
                        continue
                    equivalences.append(f"SP:{sp_accession}")

            if mgi_id in eg_eqv:
                for eg_id in eg_eqv[mgi_id]:
                    if not eg_id:
                        continue
                    equivalences.append(f"EG:{eg_id}")

            term = Term(
                key=f"{namespace}:{mgi_id}",
                namespace=namespace,
                id=mgi_id,
                label=symbol,
                name=name,
                alt_keys=[f"{namespace}:{symbol}"],
                species_key=species_key,
                species_label=species_labels.get(species_key, ""),
                entity_types=copy.copy(entity_types),
                equivalences=copy.copy(equivalences),
            )

            if len(synonyms):
                term.synonyms = copy.copy(synonyms)

            # Add term to JSONL
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
