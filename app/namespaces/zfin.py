#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  zfin.py

"""

import re
import os
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

# Globals ###################################################################
namespace_key = "zfin"  # namespace key into namespace definitions file
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_key]
ns_prefix = namespace_def["namespace"]


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

    # Can override/hard-code settings.UPDATE_CYCLE_DAYS in each term collection file if desired

    files = [
        "https://zfin.org/downloads/aliases.txt",
        "https://zfin.org/downloads/gene.txt",
        "https://zfin.org/downloads/transcripts.txt",
    ]

    for url in files:
        # Local data filepath setup
        basename = os.path.basename(url)

        if not re.search(
            ".gz$", basename
        ):  # we basically gzip everything retrieved that isn't already gzipped
            basename = f"{basename}.gz"

        # Pick one of the two following options
        local_data_fp = f"{settings.DOWNLOAD_DIR}/{namespace_key}_{basename}"

        # Get web file - but not if local downloaded file is newer
        (changed_flag, msg) = utils.get_web_file(
            url, local_data_fp, days_old=settings.UPDATE_CYCLE_DAYS
        )
        log.info(msg)


def build_json(force: bool = False):
    """Build term JSONL file"""

    # Terminology JSONL output filename
    data_fp = settings.DATA_DIR
    terms_fp = f"{data_fp}/namespaces/{namespace_key}.jsonl.gz"

    aliases_fp = f"{settings.DOWNLOAD_DIR}/{namespace_key}_aliases.txt.gz"
    genes_fp = f"{settings.DOWNLOAD_DIR}/{namespace_key}_gene.txt.gz"
    transcripts_fp = f"{settings.DOWNLOAD_DIR}/{namespace_key}_transcripts.txt.gz"

    # used if you need a tmp dir to do some processing
    # tmpdir = tempfile.TemporaryDirectory()

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, aliases_fp) and utils.file_newer(terms_fp, genes_fp):
            log.info("Will not rebuild data file as it is newer than downloaded source files")
            return False

    terms = {}
    with gzip.open(aliases_fp, "rt") as fi:
        for line in fi:
            if re.match("ZDB-GENE-", line):
                (src_id, name, symbol, syn, *extra) = line.split("\t")
                # print('Syn', syn)
                if src_id in terms:
                    terms[src_id]["synonyms"].append(syn)
                else:
                    terms[src_id] = {"name": name, "symbol": symbol, "synonyms": [syn]}

    with gzip.open(transcripts_fp, "rt") as fi:
        transcript_types = {}
        for line in fi:
            (tscript_id, so_id, name, gene_id, clone_id, tscript_type, status, *extra) = line.split(
                "\t"
            )
            if "withdrawn" in status.lower() or "artifact" in status.lower():
                continue
            if gene_id in transcript_types:
                transcript_types[gene_id][tscript_type] = 1
            else:
                transcript_types[gene_id] = {tscript_type: 1}

        for gene_id in transcript_types:
            types = transcript_types[gene_id].keys()

            entity_types = []
            for type_ in types:
                if type_ in ["lincRNA", "ncRNA", "scRNA", "snRNA", "snoRNA"]:
                    entity_types.extend(["Gene", "RNA"])
                if type_ in ["mRNA"]:
                    entity_types.extend(["Gene", "RNA", "Protein"])
                if type_ in ["miRNA"]:
                    entity_types.extend(["Gene", "Micro_RNA"])

            entity_types = list(set(entity_types))

            if gene_id == "ZDB-GENE-030115-1":
                print("Entity types", entity_types, "Types", types)

            if gene_id in terms:
                terms[gene_id]["entity_types"] = list(entity_types)
            else:
                terms[gene_id] = {"name": name, "entity_types": list(entity_types)}

    with gzip.open(genes_fp, "rt") as fi:
        for line in fi:
            (src_id, so_id, symbol, eg_id, *extra) = line.split("\t")
            if src_id in terms:
                terms[src_id]["equivalences"] = [f"EG:{eg_id.strip()}"]
                if terms[src_id].get("symbol", None) and symbol:
                    terms[src_id]["symbol"] = symbol
            else:
                log.debug(f"No term record for ZFIN {src_id} to add equivalences to")
                continue

    with gzip.open(terms_fp, "wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        for term in terms:

            main_id = terms[term].get("symbol", terms[term].get("name", term))

            term = {
                "namespace": ns_prefix,
                "namespace_value": main_id,
                "src_id": term,
                "id": f"{ns_prefix}:{main_id}",
                "alt_ids": [],
                "label": main_id,
                "name": terms[term].get("name", term),
                "species_id": "TAX:7955",
                "species_label": "zebrafish",
                "synonyms": copy.copy(list(set(terms[term].get("synonyms", [])))),
                "entity_types": copy.copy(terms[term].get("entity_types", [])),
                "equivalences": copy.copy(terms[term].get("equivalences", [])),
            }

            # Add term to JSONLines file
            fo.write("{}\n".format(json.dumps({"term": term})))


def main():

    update_data_files()
    build_json()


if __name__ == "__main__":
    main()
