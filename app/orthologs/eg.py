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
from app.schemas.main import ResourceMetadata, Term
from typer import Option

log = structlog.getLogger("orthologs/eg")


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
    name="EntrezGene orthologs",
    source_url=download_url,
    type="orthologs",
    description="Orthologs defined by EntrezGene",
    version=dt_now(),
)


def build_json():
    """Build EG orthologs json load file"""

    with gzip.open(download_fn, "rt") as fi, gzip.open(resource_fn, "wt") as fo, gzip.open(resource_fn_hmrz, "wt") as fz:

        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({"metadata": orthologs_metadata.dict(skip_defaults=True)})))

        fi.__next__()  # skip header line

        for line in fi:
            (
                subj_tax_id,
                subj_gene_id,
                relationship,
                obj_tax_id,
                obj_gene_id,
            ) = line.rstrip().split("\t")
            if relationship != "Ortholog":
                continue
        
            subj_key
            subj_species_key = f"TAX:{subj_tax_id}"
            obj_species_key = f"TAX:{obj_tax_id}"

            ortholog = {
                "subject": {
                    "key": f"{namespace}:{subj_gene_id}",
                    "tax_id": subj_species_key,
                },
                "object": {
                    "key": f"{namespace}:{obj_gene_id}",
                    "tax_id": obj_species_key",
                },
            }

            # Add ortholog to JSONL
            fo.write("{}\n".format(json.dumps({"ortholog": ortholog})))

            if subj_species_key in hmrz_species and obj_species_key in hmrz_species:
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
