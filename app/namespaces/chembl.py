#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  chembl.py

"""

import copy
import datetime
import gzip
import json
import os
import re
import sqlite3
import sys
import tarfile
import tempfile
from typing import Any, Iterable, List, Mapping

import structlog
import yaml

import app.settings as settings
import app.setup_logging
import typer
from app.common.collect_sources import get_chembl_version, get_ftp_file
from app.common.resources import get_metadata
from app.common.text import quote_id, strip_quotes
from app.schemas.main import Term
from typer import Option

log = structlog.getLogger("chembl_namespace")

# Globals

namespace = "CHEMBL"
namespace_lc = namespace.lower()
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_lc]

chembl_version = get_chembl_version(
    "ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest"
)

download_url = f"ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/latest/chembl_{chembl_version}_sqlite.tar.gz"
download_fn = f"{settings.DOWNLOAD_DIR}/chembl_{chembl_version}_sqlite.tar.gz"
resource_fn = f"{settings.DATA_DIR}/namespaces/{namespace_lc}.jsonl.gz"
download_db_fn = f"{settings.DOWNLOAD_DIR}/chembl_{chembl_version}/chembl_{chembl_version}_sqlite/chembl_{chembl_version}.db"


def query_db() -> Iterable[Mapping[str, Any]]:
    """Generator to run chembl term queries using sqlite chembl db"""
    log.error(
        "This script requires MANUAL interaction to get latest chembl and untar it."
    )

    db_filename = (
        f"{settings.DOWNLOAD_DIR}/"
        f"chembl_{chembl_version}/chembl_{chembl_version}_sqlite/chembl_{chembl_version}.db"
    )

    conn = sqlite3.connect(db_filename)
    conn.row_factory = sqlite3.Row

    main_sql = """
        SELECT
            chembl_id, syn_type, group_concat(synonyms, "||") AS syns,
            standard_inchi_key, chebi_par_id, molecule_type, pref_name
        FROM
            molecule_dictionary md
        LEFT OUTER JOIN molecule_synonyms ms ON md.molregno=ms.molregno
        LEFT OUTER JOIN compound_structures cs ON md.molregno=cs.molregno
        GROUP
            by md.molregno
    """

    with conn:
        for row in conn.execute(main_sql):

            chembl_id = row["chembl_id"].replace("CHEMBL", "CHEMBL:")
            src_id = row["chembl_id"].replace("CHEMBL", "")
            syns = row["syns"]

            if syns:
                syns = syns.lower().split("||")
            else:
                syns = []

            pref_name = row["pref_name"]
            alt_keys = []
            namespace_value = src_id

            if pref_name:
                pref_name = pref_name.lower()
                alt_keys.append(f"CHEMBL:{quote_id(pref_name)}")
                name = pref_name
            elif syns:
                name = syns[0]
            else:
                name = chembl_id

            record = {
                "name": name,
                "pref_name": pref_name,
                "chembl_id": chembl_id,
                "src_id": src_id,
                "alt_keys": alt_keys,
                "syns": copy.copy(syns),
            }
            if row["standard_inchi_key"]:
                record["inchi_key"] = f'INCHIKEY:{row["standard_inchi_key"]}'
            if row["chebi_par_id"]:
                record["chebi_id"] = f"CHEBI:{row['chebi_par_id']}"

            yield record


def build_json():
    """Build CHEMBL namespace json load file

    There are multiple tables that have to be joined and records collapsed to the Parent ID.
    """

    with gzip.open(resource_fn, mode="wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata(namespace_def)
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        for record in query_db():
            key = f"{namespace}:{record['src_id']}"
            if not record["pref_name"]:
                name = key
                label = ""
            else:
                name = record["pref_name"]
                label = name

            term = Term(
                key=key,
                namespace=namespace,
                id=record["src_id"],
                alt_keys=record["alt_keys"],
                label=label,
                name=name,
                synonyms=copy.copy(list(set(record["syns"]))),
                entity_types=["Abundance"],
            )

            if record.get("chebi_id", None):
                term.equivalence_keys.append(record["chebi_id"])
            if record.get("inchi_key", None):
                term.equivalence_keys.append(record["inchi_key"])

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({"term": term.dict()})))


def main(
    overwrite: bool = Option(
        False, help="Force overwrite of output resource data file"
    ),
    force_download: bool = Option(
        False, help="Force re-downloading of source data file"
    ),
):

    (changed, msg) = get_ftp_file(
        download_url, download_fn, force_download=force_download
    )

    if msg:
        log.info("Collect download file", result=msg, changed=changed)

    if changed or overwrite:
        build_json()


if __name__ == "__main__":
    typer.run(main)
