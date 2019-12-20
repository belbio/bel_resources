#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  taxonomy.py

This utility will download new versions of the taxonomy file
into the ../downloads directory.

It will read in taxonomy species label overrides from the taxonomy_labels.yml
in the resources folder.

A result file is created as data/terms/tax.jsonl.gz file that will act as the load
file for taxonomy data into Elasticsearch and ArangoDB.

ALT IDs: Add name as alternate ID if scientific names and taxonomy rank is species
ALT IDs: preferred taxonomy label from resources/taxonomy_labels.yml

NOTE: Reviewed all of the species scientific names and the only ones that were not unique
      had the pattern of Genus sp. - so we are restricting any scientific names with sp.
      from being used as an alt_id
"""

import datetime
import gzip
import json
import os
import re
import tarfile
import tempfile

import yaml

import app.settings as settings
import app.setup_logging
import app.utils as utils
import structlog

log = structlog.getLogger(__name__)

# Globals
namespace_key = "tax"
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_key]

server = "ftp.ncbi.nih.gov"
source_data_fp = "/pub/taxonomy/taxdump.tar.gz"

# Local data filepath setup
basename = os.path.basename(source_data_fp)

if not re.search(
    ".gz$", basename
):  # we basically gzip everything retrieved that isn't already gzipped
    basename = f"{basename}.gz"

local_data_fp = f"{settings.DOWNLOAD_DIR}/{namespace_key}_{basename}"

# Terminology JSONL output filename
data_fp = settings.DATA_DIR
terms_fp = f"{data_fp}/namespaces/{namespace_key}.jsonl.gz"
terms_hmrz_fp = (
    f"{data_fp}/namespaces/{namespace_key}_hmrz.jsonl.gz"  # Human, mouse, rat and zebrafish subset
)

hmrz_species = ["TAX:9606", "TAX:10090", "TAX:10116", "TAX:7955"]


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

    result = utils.get_ftp_file(
        server, source_data_fp, local_data_fp, days_old=settings.UPDATE_CYCLE_DAYS
    )
    log.debug("After update data files")

    changed = False
    if "Downloaded" in result[1]:
        changed = True

    return changed


def build_json(force: bool = False):
    """Build taxonomy.json file

    Args:
        force (bool): build json result regardless of file mod dates
    Returns:
        None
    """

    # Terminology JSONL output filename
    data_fp = settings.DATA_DIR
    terms_fp = f"{data_fp}/namespaces/{namespace_key}.jsonl.gz"

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.info("Will not rebuild data file as it is newer than downloaded source file")
            return False

    tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)

    tar = tarfile.open(name=local_data_fp)
    tar.extractall(path=tmpdir.name, members=None, numeric_owner=False)
    tar.close()

    terms = {}

    log.debug("Before nodes.dmp")
    # print(f'Before nodes {datetime.datetime.now()}')
    with open(f"{tmpdir.name}/nodes.dmp", "r") as f:
        log.debug("Processing nodes.dmp file")
        for line in f:
            # line = line.rstrip('\t|\n')
            (src_id, parent_id, rank, embl_code, *rest) = line.split("\t|\t")

            terms[src_id] = {
                "namespace": namespace_def["namespace"],
                "namespace_value": src_id,
                "src_id": src_id,
                "id": f"{namespace_def['namespace']}:{src_id}",
                "parent_id": f"{namespace_def['namespace']}:{parent_id}",
                "children": [],
                "taxonomy_rank": rank,
                "name": "",
                "label": "",
                "species_id": f"{namespace_def['namespace']}:{src_id}",
                "species_label": "",
                "synonyms": [],
                "alt_ids": [],
                "taxonomy_names": [],
                "annotation_types": [],
                "entity_types": ["Species"],
            }

            # Add preferred label as alt_id
            if settings.TAXONOMY_LABELS.get(src_id, None):
                terms[src_id]["alt_ids"].append(
                    f"{namespace_def['namespace']}:{settings.TAXONOMY_LABELS[src_id]}"
                )

            if rank == "species":
                terms[src_id]["annotation_types"].append("Species")

            if embl_code:
                terms[src_id]["embl_code"] = embl_code

    # Flip the hierarchy data structure
    for src_id in terms:
        if terms[src_id]["parent_id"]:
            parent_id = terms[src_id]["parent_id"].replace(f"{namespace_def['namespace']}:", "")
            if src_id != "1":  # Skip root node so we don't make root a child of root
                terms[parent_id]["children"].append(f"{namespace_def['namespace']}:{src_id}")

        del terms[src_id]["parent_id"]

    # Add labels
    log.debug("Before names.dmp")
    with open(f"{tmpdir.name}/names.dmp", "r") as fi:
        log.debug("Processing names.dmp")
        for line in fi:
            line = line.rstrip("\t|\n")
            (src_id, name, unique_variant, name_type) = line.split("\t|\t")

            if name not in terms[src_id]["synonyms"]:
                terms[src_id]["synonyms"].append(name)

            terms[src_id]["taxonomy_names"].append({"name": name, "type": name_type})

            if name_type == "genbank common name":
                terms[src_id]["label"] = settings.TAXONOMY_LABELS.get(
                    src_id, name
                )  # Override label if available
                terms[src_id]["species_label"] = settings.TAXONOMY_LABELS.get(
                    src_id, name
                )  # Override label if available
            elif name_type == "scientific name":
                terms[src_id]["name"] = name
                if not terms[src_id]["label"]:
                    terms[src_id]["label"] = name
                    terms[src_id]["species_label"] = name

                # Add name as alternate ID if scientific names and taxonomy rank is species
                if terms[src_id]["taxonomy_rank"] == "species":
                    if not re.search("sp.", name):
                        terms[src_id]["alt_ids"].append(
                            utils.get_prefixed_id(namespace_def["namespace"], name)
                        )

    with gzip.open(terms_fp, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        for src_id in terms:
            # Add terms record to JSONL
            fo.write("{}\n".format(json.dumps({"term": terms[src_id]})))

    species_labels = {}

    for src_id in terms:

        if terms[src_id]["taxonomy_rank"] != "species":
            continue
        tax_id = terms[src_id]["id"]
        label = terms[src_id]["label"]
        species_labels[tax_id] = label

    data_fp = settings.DATA_DIR
    species_labels_fn = f"{data_fp}/namespaces/{namespace_key}_labels.json.gz"
    with gzip.open(species_labels_fn, "wt") as fo:
        json.dump(species_labels, fo)


def build_hmr_json():

    """Extract Human, Mouse and Rat from EG into a new file """

    with gzip.open(terms_fp, "rt") as fi, gzip.open(terms_hmrz_fp, "wt") as fo:
        for line in fi:
            doc = json.loads(line)
            if "term" in doc and doc["term"]["species_id"] in hmrz_species:
                fo.write("{}\n".format(json.dumps(doc)))
            elif "metadata" in doc:
                fo.write("{}\n".format(json.dumps(doc)))


def main():

    update_data_files()
    build_json()
    build_hmr_json()


if __name__ == "__main__":
    main()
