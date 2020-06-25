#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  chebi.py

"""
import copy
import datetime
import gzip
import json
import os
import re
import sys
import tempfile
from typing import Any, Iterable, List, Mapping

import structlog
import yaml

import app.settings as settings
import app.setup_logging
import pronto
import typer
from app.common.collect_sources import get_ftp_file
from app.common.resources import get_metadata
from app.common.text import quote_id, strip_quotes
from app.schemas.main import Term
from typer import Option

log = structlog.getLogger(__name__)

# Globals

namespace = "CHEBI"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

download_url = "ftp://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.obo.gz"
download_fn = f"{settings.DOWNLOAD_DIR}/chebi.obo.gz"
resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"


def build_json():

    with gzip.open(download_fn, "rt") as fi, gzip.open(resource_fn, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        keyval_regex = re.compile("(\w[\-\w]+)\:\s(.*?)\s*$")
        term_regex = re.compile("\[Term\]")
        blankline_regex = re.compile("\s*$")

        term = None

        unique_names = {}

        for line in fi:
            term_match = term_regex.match(line)
            blank_match = blankline_regex.match(line)
            keyval_match = keyval_regex.match(line)
            if term_match:
                term = Term(namespace=namespace, entity_types=["Abundance"])

            elif blank_match:  # On blank line save term record
                # Add term to JSONL
                if term and term.id:
                    fo.write("{}\n".format(json.dumps({"term": term.dict()})))

                term = Term(namespace=namespace, entity_types=["Abundance"])

            elif term and keyval_match:
                key = keyval_match.group(1)
                val = keyval_match.group(2)

                if key == "id":
                    term.id = val.replace("CHEBI:", "")
                    term.key = val

                elif key == "name":
                    term.name = val
                    term.label = val
                    term.alt_keys.append(f"CHEBI:{quote_id(val)}")

                elif key == "subset":
                    if val not in ["2_STAR", "3_STAR"]:
                        term = None
                        continue

                elif key == "def":
                    val = val.replace("[]", "")
                    term.description = strip_quotes(val)

                elif key == "synonym":
                    matches = re.search('"(.*?)"', val)
                    if matches:
                        syn = matches.group(1)
                        term.synonyms.append(syn)
                    else:
                        log.warning(f"Unmatched synonym: {val}")

                elif key == "alt_id":
                    term.alt_keys.append(val.strip())

                elif key == "property_value":
                    matches = re.search('inchikey\s"(.*?)"', val)
                    if matches:
                        inchikey = matches.group(1)
                        term.equivalence_keys.append(f"INCHIKEY:{inchikey}")

                elif key == "is_obsolete":
                    term = None


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
