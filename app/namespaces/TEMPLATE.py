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

log = structlog.getLogger("CHANGEME_namespace")

# Globals

namespace = "CHANGEME"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

species_key = "TAX:CHANGEME"

download_url = "ftp://CHANGEME"
download_fn = f"{settings.DOWNLOAD_DIR}/CHANGEME.gz"
resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"


def build_json():
    """Build term json load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    species_labels = get_species_labels()

    with gzip.open(download_fn, "rt") as fi, gzip.open(resource_fn, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        orig_data = json.load(fi)

        for doc in orig_data:

            id = doc["CHANGEME"]

            term = Term(
                key=f"{namespace}:{id}",
                namespace=namespace,
                id=id,
                # label=doc["symbol"],
                # name=doc["name"],
                # species_key=species_key,
                # species_label=species_labels[species_key],
            )

            term.alt_ids = ["NS:1"]

            # Synonyms
            term.synonyms.extend(["one", "two"])

            # Equivalences
            term.equivalence_keys.append("NS:1")

            # Entity types
            term.entity_types = []

            # Obsolete Namespace IDs
            term.obsolete_keys.append("NS:1")

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
