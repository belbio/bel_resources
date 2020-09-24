#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  tax.py

"""

import copy
import datetime
import gzip
import json
import os
import re
import tarfile
import tempfile
import sys

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

log = structlog.getLogger("tax_namespace")

# Globals

namespace = "TAX"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

download_url = "ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz"
download_fn = f"{settings.DOWNLOAD_DIR}/taxdump.tar.gz"

species_labels_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}_labels.json.gz"
resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"
resource_fn_hmrz = f"{settings.DATA_DIR}/namespaces/{namespace_lc}_hmrz.jsonl.gz"

hmrz_species = ["TAX:9606", "TAX:10090", "TAX:10116", "TAX:7955"]


def build_json():
    """Build taxonomy.json file"""

    # Extract nodes.dmp file from tarfile
    tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
    try:
        tar = tarfile.open(name=download_fn)
        tar.extractall(path=tmpdir.name, members=None, numeric_owner=False)
        tar.close()
    except Exception as e:
        print(f"Error trying to open tarfile: {str(e)}")
        sys.exit(1)

    terms = {}

    # Collect IDs and tree structure
    with open(f"{tmpdir.name}/nodes.dmp", "r") as f:
        for line in f:
            (id, parent_id, rank, *rest) = line.split("\t|\t")

            terms[id] = {
                "namespace": namespace,
                "id": id,
                "key": f"{namespace}:{id}",
                "parent_keys": [],
                "taxonomy_rank": rank,
                "name": "",
                "label": "",
                "species_key": f"{namespace}:{id}",
                "species_label": "",
                "synonyms": [],
                "alt_keys": [],
                "taxonomy_names": [],
                "annotation_types": [],
                "entity_types": [],
            }

            # Add preferred label as alt_id
            if settings.TAXONOMY_LABELS.get(id, False):
                terms[id]["alt_keys"].append(
                    f"{namespace_def['namespace']}:{settings.TAXONOMY_LABELS[id]}"
                )

            if parent_id and parent_id != id:
                terms[id]["parent_keys"].append(f"{namespace}:{parent_id}")

            # Only add Species to annotation/entity types to records with rank == species in the nodes.dmp file
            if rank == "species":
                terms[id]["annotation_types"].append("Species")
                terms[id]["entity_types"].append("Species")

    # Add labels
    with open(f"{tmpdir.name}/names.dmp", "r") as fi:
        for line in fi:
            line = line.rstrip("\t|\n")
            (id, name, unique_variant, name_type) = line.split("\t|\t")

            if name not in terms[id]["synonyms"]:
                terms[id]["synonyms"].append(name)

            terms[id]["taxonomy_names"].append({"name": name, "type": name_type})

            if name_type == "genbank common name":
                terms[id]["label"] = settings.TAXONOMY_LABELS.get(
                    id, name
                )  # Override label if available
                terms[id]["species_label"] = settings.TAXONOMY_LABELS.get(
                    id, name
                )  # Override label if available
            elif name_type == "scientific name":
                terms[id]["name"] = name
                if not terms[id]["label"]:
                    terms[id]["label"] = name
                    terms[id]["species_label"] = name

                # Add name as alternate ID if scientific names and taxonomy rank is species
                if terms[id]["taxonomy_rank"] == "species":
                 
                    if not re.search("sp.", name):
                        terms[id]["alt_keys"].append(f"{namespace}:{quote_id(name)}")

    with gzip.open(resource_fn, "wt") as fo, gzip.open(
        resource_fn_hmrz, "wt"
    ) as fz:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))
        fz.write("{}\n".format(json.dumps({"metadata": metadata})))

        for id in terms:

            # Make sure the term record conforms to defined Term structure
            term = Term(
                key = terms[id]["key"],
                namespace = namespace,
                id = id,
                label = terms[id]["label"],
                name = terms[id]["name"],
                description = f"Taxonomy rank: {terms[id]['taxonomy_rank']}",
                synonyms = terms[id]["synonyms"],
                alt_keys = terms[id]["alt_keys"],
                parent_keys = terms[id]["parent_keys"],
                species_key = terms[id]["species_key"],
                species_label = terms[id]["species_label"],
                entity_types = terms[id]["entity_types"],
                annotation_types = terms[id]["annotation_types"],
            )

            # Add terms record to JSONL
            fo.write("{}\n".format(json.dumps({"term": term.dict()})))

            if terms[id]["species_key"] in hmrz_species:
                fz.write("{}\n".format(json.dumps({"term": term.dict()})))


    # Create species label file
    species_labels = {}

    for id in terms:
        if terms[id]["taxonomy_rank"] != "species":
            continue
        species_labels[terms[id]["key"]] = terms[id]["label"]    

    with gzip.open(species_labels_fn, "wt") as fo:
        json.dump(species_labels, fo)


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
