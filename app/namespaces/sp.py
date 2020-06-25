#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  sp.py

"""

import copy
import datetime
import gzip
import json
import os
import re
from typing import Any, List, Mapping

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

log = structlog.getLogger(__name__)

# file documentation:  http://web.expasy.org/docs/userman.html
# 500Mb ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz
# 112Gb ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_trembl.dat.gz


# Globals

namespace = "SP"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

download_url = "ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz"
download_fn = f"{settings.DOWNLOAD_DIR}/sp_uniprot_sprot.dat.gz"
resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"
resource_fn_hmrz = f"{settings.DATA_DIR}/namespaces/{namespace_lc}_hmrz.jsonl.gz"
hmrz_species = ["TAX:9606", "TAX:10090", "TAX:10116", "TAX:7955"]

species_labels = get_species_labels()
model_org_prefixes = ["HGNC", "MGI", "RGD", "ZFIN"]
model_org_prefix_str = "|".join(model_org_prefixes)

def process_record(record: List[str]) -> Term:
    """Process SwissProt Dat file record

    Args:
        record (List[str]): array of swissprot dat file for one protein

    Returns:
        Mapping[str, Any]: term record for namespace
    """

    equivalences = []
    accessions = []
    de = ""
    gn = ""

    for line in record:

        # Get ID
        match = re.match("^ID\s+(\w+);?", line)
        if match:
            entry_name = match.group(1)

        # Get accessions
        if re.match("^AC", line):
            ac_line = re.sub("^AC\s+", "", line).rstrip()
            ac_line = re.sub(";$", "", ac_line)
            ac_line = re.sub(";\s+", ";", ac_line)
            accessions.extend(ac_line.split(";"))

        # Get Taxonomy ID
        match = re.match("^OX\s+NCBI_TaxID=(\d+)", line)
        if match:
            species_id = match.group(1)
            species_key = f"TAX:{species_id}"

        # Get Equivalences
        match = re.match("^DR\s+(\w+);\s(\w+);\s([\w\-]+)\.", line)
        if match:
            (db, db_id, extra) = match.group(1, 2, 3)
            if db == "HGNC":
                equivalences.append(f"{db}:{extra}")
                # print(equivalences)
            elif db == "MGI":
                equivalences.append(f"{db}:{extra}")
                # print(equivalences)
            if db == "RGD":
                equivalences.append(f"{db}:{extra}")
                # print(equivalences)
            elif db == "GeneID":
                equivalences.append(f"EG:{db_id}")
                # print(equivalences)

        if re.match("^DE\s+", line):
            de += line.replace("DE", "").strip()
        if re.match("^GN\s+", line):
            gn += line.replace("GN", "").strip()

    synonyms = []
    name = None
    full_name = None
    
    # GN - gene names processing
    gn = re.sub(" {.*?}", "", gn, flags=re.S)
    match = re.search("Name=(.*?)[;{]+", gn)
    if match:
        name = match.group(1)
    match = re.search("Synonyms=(.*?);", gn)
    if match:
        syns = match.group(1)
        synonyms.extend(syns.split(", "))

    match = re.search("ORFNames=(.*?);", gn)
    if match:
        syns = match.group(1)
        orfnames = syns.split(", ")
        synonyms.extend(orfnames)
        if not name:
            name = orfnames[0]

    # Equivalence processing
    #    Remove EG ID's if HGNC/MGI/RGD or other model organism database IDs
    #    We do this because some SP have multiple EG IDs - we want to remove readthrough entries
    #        and resolve SP IFNA1_Human to EG:3439!IFNA1 instead of EG:3447!IFNA13
    #    Protocol therefore is:
    #        1. Check if model org ID exists
    #        2. If so, take first model org ID from sorted list
    #        3. Else take first EG ID from sorted list

    equivalences.sort()

    eg_equivalences = [e for e in equivalences if e.startswith("EG")]
    if len(eg_equivalences) > 1:
        model_org_equivalences = [e for e in equivalences if re.match(model_org_prefix_str, e)]
        if len(model_org_equivalences) >= 1:
            equivalences = [model_org_equivalences[0]]
        else:
            equivalences = [eg_equivalences[0]]

    # DE - name processing
    log.debug(f"DE {de}")
    de = re.sub(" {.*?}", "", de, flags=re.S)
    match = re.search("RecName:(.*?;)\s*(\w+:)?", de)
    if match:
        recname_grp = match.group(1)
        match_list = re.findall("\s*(\w+)=(.*?);", recname_grp)
        for key, val in match_list:
            if key == "Full":
                full_name = val
            if not name and key == "Short":
                name = val

            log.debug(f"DE RecName Key: {key}  Val: {val}")
        if not name and full_name:  # Use long name for protein name if all else fails
            name = full_name

    match = re.search("AltName:(.*?;)\s*\w+:", de, flags=0)
    if match:
        altname_grp = match.group(1)
        match_list = re.findall("\s*(\w+)=(.*?);", altname_grp)
        for key, val in match_list:
            if key in ["Full", "Short"]:
                synonyms.append(val)
            log.debug(f"DE AltName Key: {key}  Val: {val}")

    if not name:
        name = entry_name

    term = Term(
        key=f"{namespace}:{accessions[0]}",
        namespace=namespace,
        id=accessions[0],
        label=name,
        name=name,
        species_key=species_key,
        species_label=species_labels.get(species_key, ""),
        entity_types=["Gene", "RNA", "Protein"],
        synonyms=copy.copy(synonyms),
        equivalence_keys=copy.copy(equivalences),
        alt_keys=[f"{namespace}:{entry_name}"],
        obsolete_keys=[],
    )

    if full_name:
        term.description = full_name

    # Obsolete IDs
    for obs_id in accessions[1:]:
        term.obsolete_keys.append(f"{namespace}:{obs_id}")

    return term


def build_json():
    """Build Swissprot namespace jsonl load file"""

    with gzip.open(download_fn, "rt") as fi, gzip.open(resource_fn, "wt") as fo, gzip.open(
        resource_fn_hmrz, "wt"
    ) as fz:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        record = []
        for line in fi:
            record.append(line)

            if re.match("^//", line):
                term = process_record(record)

                fo.write("{}\n".format(json.dumps({"term": term.dict()})))
                if term.species_key in hmrz_species:
                    fz.write("{}\n".format(json.dumps({"term": term.dict()})))

                record = []


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
