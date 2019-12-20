#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  do.py

"""

import sys
import os
import re
import tempfile
import json
import yaml
import datetime
import copy
import gzip

import app.utils as utils
import app.settings as settings

import app.setup_logging
import structlog

log = structlog.getLogger(__name__)

# Globals
namespace_key = "do"
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_key]
ns_prefix = namespace_def["namespace"]

url = "http://purl.obolibrary.org/obo/doid.obo"

# Local data filepath setup
basename = os.path.basename(url)

if not re.search(
    "\.gz$", basename
):  # we basically gzip everything retrieved that isn't already gzipped
    basename = f"{basename}.gz"

local_data_fp = f"{settings.DOWNLOAD_DIR}/{basename}"


def get_metadata():
    # Setup metadata info - mostly captured from namespace definition file which
    # can be overridden in belbio_conf.yml file
    dt = datetime.datetime.now().replace(microsecond=0).isoformat()
    metadata = {
        "name": namespace_def["namespace"],
        "type": "namespace",
        "namespace": namespace_def["namespace"],
        "description": namespace_def["description"],
        "version": dt,
        "src_url": namespace_def["src_url"],
        "url_template": namespace_def["template_url"],
    }

    return metadata


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    (changed_flag, msg) = utils.get_web_file(
        url, local_data_fp, days_old=settings.UPDATE_CYCLE_DAYS
    )
    log.info(msg)

    return changed_flag


def process_obo(force: bool = False):

    # Terminology JSONL output filename
    data_fp = settings.DATA_DIR
    terms_fp = f"{data_fp}/namespaces/{namespace_key}.jsonl.gz"

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.info("Will not rebuild data file as it is newer than downloaded source file")
            return False

    with gzip.open(local_data_fp, "rt") as fi, gzip.open(terms_fp, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        term = {}

        keyval_regex = re.compile("(\w[\-\w]+)\:\s(.*?)\s*$")
        term_regex = re.compile("\[Term\]")
        blankline_regex = re.compile("\s*$")

        unique_names = {}

        for line in fi:
            term_match = term_regex.match(line)
            blank_match = blankline_regex.match(line)
            keyval_match = keyval_regex.match(line)
            if term_match:
                term = {
                    "namespace": ns_prefix,
                    "namespace_value": "",
                    "src_id": "",
                    "id": "",
                    "label": "",
                    "name": "",
                    "description": "",
                    "synonyms": [],
                    "annotation_types": ["Disease"],
                    "entity_types": ["Pathology"],
                    "equivalences": [],
                    "parents": [],
                    "alt_ids": [],
                }

            elif blank_match:
                # Add term to JSONL
                if term.get("obsolete", False):
                    pass  # Skip obsolete terms
                elif term.get("id", None):
                    fo.write("{}\n".format(json.dumps({"term": term})))

                    if term["name"] not in unique_names:
                        unique_names[term["name"]] = 1
                    else:
                        log.error(f'Duplicate name in DO: {term["name"]}')

                term = {}

            elif term and keyval_match:
                key = keyval_match.group(1)
                val = keyval_match.group(2)

                if key == "id":
                    term["src_id"] = val

                elif key == "name":
                    name_id = utils.get_prefixed_id(ns_prefix, val)
                    term["label"] = val
                    term["name"] = val
                    if len(name_id) > 80:
                        term["id"] = term["src_id"].replace("DOID", "DO")
                        term["alt_ids"].append(name_id)
                        term["namespace_value"] = term["src_id"].replace("DO:", "")
                    else:
                        term["id"] = name_id
                        term["alt_ids"].append(term["src_id"].replace("DOID", "DO"))
                        term["namespace_value"] = val

                elif key == "is_obsolete":
                    # print('Obsolete', term['alt_ids'])
                    term["obsolete"] = True

                elif key == "def":
                    term["description"] = val

                elif key == "synonym":
                    matches = re.search('"(.*?)"', val)
                    if matches:
                        syn = matches.group(1).strip()
                        term["synonyms"].append(syn)
                    else:
                        log.warning(f"Unmatched synonym: {val}")

                elif key == "alt_id":
                    val = val.replace("DOID", "DO").strip()
                    term["alt_ids"].append(val)

                elif key == "is_a":
                    matches = re.match("DOID:(\d+)\s", val)
                    if matches:
                        parent_id = matches.group(1)
                        term["parents"].append(f"DO:{parent_id}")

                elif key == "xref":
                    matches = re.match("(\w+):(\w+)\s*", val)
                    if matches:
                        ns = matches.group(1)
                        nsval = matches.group(2)
                        if "UMLS_CUI" in ns:
                            term["equivalences"].append(f"UMLS:{nsval}")
                        elif "SNOMED" in ns:
                            term["equivalences"].append(f"SNOMEDCT:{nsval}")
                        elif "NCI" in ns:
                            term["equivalences"].append(f"NCI:{nsval}")
                        elif "MESH" == ns:
                            term["equivalences"].append(f"MESH:{nsval}")
                        elif "ICD" in ns:
                            continue


def main():

    update_data_files()
    process_obo()


if __name__ == "__main__":
    main()
