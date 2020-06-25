#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  go.py

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
from app.common.resources import get_metadata
from app.common.text import quote_id
from app.schemas.main import Term
from typer import Option

log = structlog.getLogger(__name__)

# Globals

namespace = "GO"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

download_url = "http://purl.obolibrary.org/obo/go.obo"
download_fn = f"{settings.DOWNLOAD_DIR}/go.obo.gz"
resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"

complex_parent_id = "GO:0032991"


def is_complex(check_id, parent_ids) -> bool:
    """Check to see if parent_id is a parent of check_id"""

    if check_id in parent_ids:

        for parent_id in parent_ids[check_id]:
            result = is_complex(parent_id, parent_ids)

            if result is True:
                return result
            elif complex_parent_id == parent_id:
                return True
            else:
                return False


def build_json():

    # collect parents/hierarchy
    parent_ids = {}
    with gzip.open(download_fn, "rt") as fi:
        id_re = re.compile("id:\s+(\S+)\s*")
        isa_re = re.compile("is_a:\s+(\S+)\s")

        for line in fi:
            id_match = id_re.match(line)
            isa_match = isa_re.match(line)
            if id_match:
                goid = id_match.group(1)
            if isa_match:
                isa_id = isa_match.group(1)
                if goid in parent_ids:
                    parent_ids[goid][isa_id] = 1
                else:
                    parent_ids[goid] = {isa_id: 1}

    with gzip.open(download_fn, "rt") as fi, gzip.open(resource_fn, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        keyval_regex = re.compile("(\w[\-\w]+)\:\s(.*?)\s*$")
        term_regex = re.compile("\[Term\]")
        blankline_regex = re.compile("\s*$")

        term = None
        obsolete_flag = False

        for line in fi:
            term_match = term_regex.match(line)
            blank_match = blankline_regex.match(line)
            keyval_match = keyval_regex.match(line)

            if term_match:
                obsolete_flag = False
                term = Term(namespace=namespace)

            elif blank_match:
                # Add term to JSONL
                if not obsolete_flag and term and term.id:
                    fo.write("{}\n".format(json.dumps({"term": term.dict()})))
                    term = None

            elif term and keyval_match:
                key = keyval_match.group(1)
                val = keyval_match.group(2)

                if key == "id":
                    term.id = val.replace("GO:", "")
                    term.key = f"{namespace}:{term.id}"

                    if is_complex(term.key, parent_ids):
                        # print("Adding complex entity type")
                        term.entity_types.append("Complex")

                elif key == "name":
                    term.label = val
                    term.name = val

                    label_id = quote_id(val)
                    term.alt_keys.append(f"{namespace}:{label_id}")

                elif key == "is_obsolete":
                    obsolete_flag = True

                elif key == "def":
                    matches = re.search('"(.*?)"', val)
                    if matches:
                        description = matches.group(1).strip()
                        term.description = description

                elif key == "synonym":
                    matches = re.search('"(.*?)"', val)
                    if matches:
                        syn = matches.group(1).strip()
                        term.synonyms.append(syn)
                    else:
                        log.warning(f"Unmatched synonym: {val}")

                elif key == "alt_id":
                    val = val.replace("DOID", "DO").strip()
                    term.alt_keys.append(val)

                elif key == "is_a":
                    matches = re.match("DOID:(\d+)\s", val)
                    if matches:
                        parent_id = matches.group(1)
                        term.parent_keys.append(f"DO:{parent_id}")

                elif key == "namespace":
                    if "biological_process" == val:
                        term.entity_types.append("BiologicalProcess")
                    elif "cellular_component" == val:
                        term.entity_types.append("Location")
                    elif "molecular_function" == val:
                        term.entity_types.append("Activity")


def main(
    overwrite: bool = Option(False, help="Force overwrite of output resource data file"),
    force_download: bool = Option(False, help="Force re-downloading of source data file"),
):

    (changed, msg) = get_web_file(download_url, download_fn, force_download=force_download)

    if msg:
        log.info("Collect download file", result=msg, changed=changed)

    if changed or overwrite:
        build_json()


if __name__ == "__main__":
    typer.run(main)
