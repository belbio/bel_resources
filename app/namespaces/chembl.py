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

import yaml

import app.settings as settings
import app.setup_logging
import app.utils as utils
import structlog

log = structlog.getLogger(__name__)

# Globals
namespace_key = "chembl"
namespace_def = settings.NAMESPACE_DEFINITIONS[namespace_key]
ns_prefix = namespace_def["namespace"]

server = "ftp.ebi.ac.uk"
filename_regex = r"chembl_(.*?)_sqlite\.tar\.gz"
server_directory = "/pub/databases/chembl/ChEMBLdb/latest/"

# chembl_version = utils.get_chembl_version(filename_regex, server, server_directory, 1)
chembl_version = "26"

source_data_fp = f"/pub/databases/chembl/ChEMBLdb/latest/chembl_{chembl_version}_sqlite.tar.gz"

# Local data filepath setup
basename = os.path.basename(source_data_fp)

if not re.search(
    ".gz$", basename
):  # we basically gzip everything retrieved that isn't already gzipped
    basename = f"{basename}.gz"

local_data_fp = f"{settings.DOWNLOAD_DIR}/{basename}"

print(
    """
      This script requires MANUAL interaction to get latest chembl and untar it.

      Further, the 16Gb sqlite database for Chembl won't copy into the S3 Bucket (might be an s3fuse issue).
      This script has to be run outside of the BELResGen server.
      """
)


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

    result = utils.get_ftp_file(server, source_data_fp, local_data_fp, days_old=380)

    # Notify admin if CHEMBL file is missing (e.g. they updated the CHEMBL version)
    if not result[0]:
        mail_to = settings.MAIL_NOTIFY_EMAIL
        subject = "Missing CHEMBL file"
        msg = f"Cannot find http://{server}/{source_data_fp} - please update BEL Resource Generator CHEMBL download script"
        result = utils.send_mail(mail_to, subject, msg)
        log.info("Cannot find CHEMBL file to download")
        quit()

    changed = False
    if "Downloaded" in result[1]:
        changed = True

    if changed:
        tar = tarfile.open(local_data_fp)
        tar.extractall()
        tar.close()

    return changed


def pref_name_dupes():
    """Check that pref_name in chembl is uniq"""

    check_pref_term_sql = """
        select
            chembl_id, pref_name
        from
            molecule_dictionary
    """

    db_filename = (
        f"{settings.DOWNLOAD_DIR}/"
        f"chembl_{chembl_version}/chembl_{chembl_version}_sqlite/chembl_{chembl_version}.db"
    )

    conn = sqlite3.connect(db_filename)
    conn.row_factory = sqlite3.Row

    dupes_flag = False  # set to false if any duplicates exist
    with conn:
        check_pref_term_uniqueness = {}
        for row in conn.execute(check_pref_term_sql):
            pref_name = row["pref_name"]
            if pref_name:
                pref_name = pref_name.lower()
            chembl_id = row["chembl_id"]
            if check_pref_term_uniqueness.get(pref_name, None):
                log.error(
                    f'CHEMBL pref_name used for multiple chembl_ids {chembl_id}, {check_pref_term_uniqueness["pref_name"]}'
                )
                dupes_flag = True

    return dupes_flag


def query_db() -> Iterable[Mapping[str, Any]]:
    """Generator to run chembl term queries using sqlite chembl db"""
    log.error("This script requires MANUAL interaction to get latest chembl and untar it.")

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
            alt_ids = []
            namespace_value = src_id

            if pref_name:
                alt_ids.append(chembl_id)
                pref_name = pref_name.lower()
                name = pref_name
                chembl_id = utils.get_prefixed_id(ns_prefix, pref_name)
                namespace_value = pref_name
            elif syns:
                name = syns[0]
            else:
                name = chembl_id

            term = {
                "name": name,
                "namespace_value": namespace_value,
                "pref_name": pref_name,
                "chembl_id": chembl_id,
                "src_id": src_id,
                "alt_ids": alt_ids,
                "syns": copy.copy(syns),
            }
            if row["standard_inchi_key"]:
                term["inchi_key"] = f'INCHIKEY:{row["standard_inchi_key"]}'
            if row["chebi_par_id"]:
                term["chebi_id"] = f"CHEBI:{row['chebi_par_id']}"

            yield term


def build_json(force: bool = False):
    """Build CHEMBL namespace json load file

    Have to build this as a JSON term file since there are multiple tables that
    have to be joined and records collapsed to the Parent ID.

    Args:
        force (bool): build jsonl result regardless of file mod dates

    Returns:
        None
    """

    # Terminology JSONL output filename
    data_fp = settings.DATA_DIR
    terms_fp = f"{data_fp}/namespaces/{namespace_key}.jsonl.gz"

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.warning("Will not rebuild data file as it is newer than downloaded source files")
            return False

    with gzip.open(terms_fp, mode="wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({"metadata": metadata})))

        for row in query_db():

            term = {
                "namespace": ns_prefix,
                "namespace_value": row["namespace_value"],
                "src_id": row["src_id"],
                "id": row["chembl_id"],
                "alt_ids": row["alt_ids"],
                "label": row["name"],
                "name": row["name"],
                "synonyms": copy.copy(list(set(row["syns"]))),
                "entity_types": ["Abundance"],
                "equivalences": [],
            }
            if row.get("chebi_id", None):
                term["equivalences"].append(row["chebi_id"])
            if row.get("inchi_key", None):
                term["equivalences"].append(row["inchi_key"])

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({"term": term})))


def main():

    # Cannot detect changes as ftp server doesn't support MLSD cmd

    # update_data_files()

    if pref_name_dupes():
        quit()

    build_json()


if __name__ == "__main__":
    main()
