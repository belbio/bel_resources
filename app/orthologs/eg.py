#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  eg.py  -- orthologs

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
from app.common.text import dt_now, quote_id
from app.schemas.main import Orthologs, ResourceMetadata
from typer import Option

log = structlog.getLogger("eg_orthologs")

namespace = "EG"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

download_url = "ftp://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_orthologs.gz"
download_fn = f"{settings.DOWNLOAD_DIR}/eg_orthologs.csv.gz"
download_history_fn = f"{settings.DOWNLOAD_DIR}/eg_gene_history.json.gz"
resource_fn = f"{settings.DATA_DIR}/orthologs/{namespace_lc}.jsonl.gz"
resource_fn_hmrz = f"{settings.DATA_DIR}/orthologs/{namespace_lc}_hmrz.jsonl.gz"
hmrz_species = ["TAX:9606", "TAX:10090", "TAX:10116", "TAX:7955"]


orthologs_metadata = ResourceMetadata(
    name="Orthologs_EntrezGene",
    source_name="NCBI EntrezGene database",
    source_url=download_url,
    resource_type="orthologs",
    description="Orthologs defined by EntrezGene",
    version=dt_now(),
).dict(skip_defaults=True)


def build_json():
    """Build EG orthologs json load file"""

    with gzip.open(download_fn, "rt") as fi, gzip.open(resource_fn, "wt") as fo, gzip.open(
        resource_fn_hmrz, "wt"
    ) as fz:

        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({"metadata": orthologs_metadata})))
        fz.write("{}\n".format(json.dumps({"metadata": orthologs_metadata})))

        fi.__next__()  # skip header line

        for line in fi:
            (
                subject_species_id,
                subject_gene_id,
                relationship,
                object_species_id,
                object_gene_id,
            ) = line.rstrip().split("\t")
            if relationship != "Ortholog":
                continue

            subject_species_key = f"TAX:{subject_species_id}"
            object_species_key = f"TAX:{object_species_id}"

            subject_key = f"{namespace}:{subject_gene_id}"
            object_key = f"{namespace}:{object_gene_id}"

            # Simple lexical sorting (e.g. not numerical) to ensure 1 entry per pair
            if subject_key > object_key:
                subject_key, subject_species_key, object_key, object_species_key = (
                    object_key,
                    object_species_key,
                    subject_key,
                    subject_species_key,
                )

            ortholog = {
                "subject_key": subject_key,
                "subject_species_key": subject_species_key,
                "object_key": object_key,
                "object_species_key": object_species_key,
            }

            # Add ortholog to JSONL
            fo.write("{}\n".format(json.dumps({"ortholog": ortholog})))

            if subject_species_key in hmrz_species and object_species_key in hmrz_species:
                fz.write("{}\n".format(json.dumps({"ortholog": ortholog})))


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
