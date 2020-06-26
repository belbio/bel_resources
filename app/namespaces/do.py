#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  do.py

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

log = structlog.getLogger("do_namespace")

# Globals

namespace = "DO"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

download_url = "http://purl.obolibrary.org/obo/doid.obo"
download_fn = f"{settings.DOWNLOAD_DIR}/{namespace_lc}.obo.gz"
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
        obsolete_flag = False

        for line in fi:
            term_match = term_regex.match(line)
            blank_match = blankline_regex.match(line)
            keyval_match = keyval_regex.match(line)

            if term_match:
                obsolete_flag = False
                term = Term(namespace=namespace, annotation_types=["Disease"], entity_types=["Pathology"])

            elif blank_match:
                # Add term to JSONL
                if not obsolete_flag and term and term.id:
                    fo.write("{}\n".format(json.dumps({"term": term.dict()})))
                    term = None

            elif term and keyval_match:
                key = keyval_match.group(1)
                val = keyval_match.group(2)

                if key == "id":
                    term.id = val.replace("DOID:", "")
                    term.key = f"{namespace}:{term.id}"

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

                elif key == "xref":
                    matches = re.match("(\w+):(\w+)\s*", val)
                    if matches:
                        ns = matches.group(1)
                        nsval = matches.group(2)
                        if "UMLS_CUI" in ns:
                            term.equivalence_keys.append(f"UMLS:{nsval}")
                        elif "SNOMED" in ns:
                            term.equivalence_keys.append(f"SNOMEDCT:{nsval}")
                        elif "NCI" in ns:
                            term.equivalence_keys.append(f"NCI:{nsval}")
                        elif "MESH" == ns:
                            term.equivalence_keys.append(f"MESH:{nsval}")
                        elif "ICD" in ns:
                            continue


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
