#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  mesh.py

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
from app.common.collect_sources import get_ftp_file, get_mesh_version
from app.common.resources import get_metadata, get_species_labels
from app.common.text import quote_id
from app.schemas.main import Term
from typer import Option

# TODO - figure out how to add the children attributes - complicated by the fact that only
#        some terms are used and not others and terms have multiple locations on DAG
#        An approach would be to process this after the MESH hierarchy is created.
#        Need to add the tree numbers as a dictionary split by the decimal point (use nested_dict python module for this)

# Tree example in MESH Browser ('MeSH Tree Structures' tab)
# ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/meshtrees/mtrees2017.bin

log = structlog.getLogger(__name__)

# Globals

version = get_mesh_version("ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/asciimesh")

namespace = "MESH"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

download_concepts_url = f"ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/asciimesh/c{version}.bin"
download_concepts_fn = f"{settings.DOWNLOAD_DIR}/mesh_c{version}.bin.gz"
download_descriptors_url = (
    f"ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/asciimesh/d{version}.bin"
)
download_descriptors_fn = f"{settings.DOWNLOAD_DIR}/mesh_d{version}.bin.gz"

resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"


def process_types(mesh_tree_ids):

    entity_types = set()
    annotation_types = set()

    if mesh_tree_ids:  # Description records
        for tree_id in mesh_tree_ids:
            if re.match("A", tree_id) and not re.match("A11", tree_id):
                annotation_types.add("Anatomy")
            if re.match("A11", tree_id) and not re.match("A11.284", tree_id):
                annotation_types.add("Cell")
                entity_types.add("Cell")
            if re.match("A11.251.210", tree_id):
                annotation_types.add("CellLine")
            if re.match("A11.284", tree_id):
                entity_types.add("Location")
                annotation_types.add("CellStructure")

            # Original OpenBEL was C|F03 - Charles Hoyt suggested C|F Natalie Catlett overrode that
            if re.match("C|F03", tree_id):
                annotation_types.add("Disease")
                entity_types.add("Pathology")
            if re.match("G", tree_id) and not re.match("G01|G15|G17", tree_id):
                entity_types.add("BiologicalProcess")
            if re.match("F", tree_id) and not re.match("F03", tree_id):
                entity_types.add("BiologicalProcess")
            if re.match("D", tree_id) and not re.match("D12.776", tree_id):
                entity_types.add("Abundance")
            if re.match("D12.776", tree_id):
                entity_types.add("Gene")
                entity_types.add("RNA")
                entity_types.add("Protein")
            if re.match("J02", tree_id):
                entity_types.add("Abundance")

    return (list(entity_types), list(annotation_types))


def build_json():
    """Build MESH namespace json load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    links = {}  # links[mh|hm][id] -- allow linking between descriptor and concept records

    blankline_regex = re.compile("\s*$")

    with gzip.open(download_descriptors_fn, "rt") as fid, gzip.open(
        download_concepts_fn, "rt"
    ) as fic, gzip.open(resource_fn, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        mesh_tree_ids = []

        # Process descriptor records
        for line in fid:
            blank_match = blankline_regex.match(line)

            if line.startswith("*NEWRECORD"):
                term = Term(namespace=namespace)
                mesh_tree_ids = []

            elif blank_match:
                (entity_types, annotation_types) = process_types(mesh_tree_ids)

                # only save terms that have entity or annotation types
                if entity_types or annotation_types:
                    if entity_types:
                        term.entity_types = entity_types
                    if annotation_types:
                        term.annotation_types = annotation_types

                    if entity_types or annotation_types:
                        # Add term to JSONL
                        fo.write("{}\n".format(json.dumps({"term": term.dict()})))

                    # Linking to concept records
                    links[term.name] = (term.key, entity_types, annotation_types)

            # term.id
            elif line.startswith("UI = "):
                term.id = line.replace("UI = ", "").rstrip()
                term.key = f"{namespace}:{term.id}"

            # term.name
            elif line.startswith("MH = "):
                mh = line.replace("MH = ", "").rstrip()
                term.alt_keys.append(f"{namespace}:{quote_id(mh)}")
                term.label = mh
                term.name = mh

            # term.description
            elif line.startswith("MS = "):
                term.description = line.replace("MS = ", "").rstrip()

            # term.synonyms
            elif line.startswith("ENTRY = "):
                syn = line.replace("ENTRY = ", "").split("|")[0].rstrip()
                term.synonyms.append(syn)

            elif line.startswith("PRINT ENTRY = "):
                syn = line.replace("PRINT ENTRY = ", "").split("|")[0].rstrip()
                term.synonyms.append(syn)

            # needed for mapping entity and annotation types
            elif line.startswith("MN = "):
                mesh_tree_ids.append(line.replace("MN = ", "").rstrip())

        # Process concept records (AFTER descriptors)
        mesh_heading = ""

        for line in fic:
            blank_match = blankline_regex.match(line)

            if line.startswith("*NEWRECORD"):
                term = Term(namespace=namespace)
                mesh_heading = ""

            elif blank_match:

                (parent_key, entity_types, annotation_types) = links.get(mesh_heading, ("", [], []))

                # only save terms that have entity or annotation types
                if entity_types or annotation_types:
                    if entity_types:
                        term.entity_types = entity_types
                    if annotation_types:
                        term.annotation_types = annotation_types

                    if entity_types or annotation_types:
                        # Add term to JSONL
                        fo.write("{}\n".format(json.dumps({"term": term.dict()})))

            # term.id
            elif line.startswith("UI = "):
                term.id = line.replace("UI = ", "").rstrip()
                term.key = f"{namespace}:{term.id}"

            # mesh_heading
            elif line.startswith("HM = "):
                mesh_heading = line.replace("HM = ", "").rstrip()

            # term.name
            elif line.startswith("NM = "):
                nm = line.replace("NM = ", "").rstrip()
                term.alt_keys.append(f"{namespace}:{quote_id(nm)}")
                term.label = nm
                term.name = nm

            # term.synonyms
            elif line.startswith("SY = "):
                syn = line.replace("SY = ", "").split("|")[0].rstrip()
                term.synonyms.append(syn)


def main(
    overwrite: bool = Option(False, help="Force overwrite of output resource data file"),
    force_download: bool = Option(False, help="Force re-downloading of source data file"),
):

    (changed_concepts, msg) = get_ftp_file(
        download_concepts_url, download_concepts_fn, force_download=force_download
    )
    (changed_descriptors, msg) = get_ftp_file(
        download_descriptors_url, download_descriptors_fn, force_download=force_download
    )

    if msg:
        log.info("Collect download file", result=msg, changed=changed_descriptors)

    if changed_descriptors or overwrite:
        build_json()


if __name__ == "__main__":
    typer.run(main)


# Used in process_types() for concept records but we are pulling the descriptor
#    record types onto the concept records now

# chemicals_ST = (
#     "T116",
#     "T195",
#     "T123",
#     "T122",
#     "T118",
#     "T103",
#     "T120",
#     "T104",
#     "T200",
#     "T111",
#     "T196",
#     "T126",
#     "T131",
#     "T125",
#     "T129",
#     "T130",
#     "T197",
#     "T119",
#     "T124",
#     "T114",
#     "T109",
#     "T115",
#     "T121",
#     "T192",
#     "T110",
#     "T127",
# )
#     # Used in process_types()
#     elif sts:  # Concepts
#         flag = 0
#         for st in sts:
#             if st in chemicals_ST:
#                 flag = 1
#                 break
#         if flag:
#             entity_types.add("Abundance")
