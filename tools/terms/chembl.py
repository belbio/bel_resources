#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  chembl.py

"""

import sys
import re
import os
import tempfile
import json
import yaml
import datetime
import sqlite3
import copy
import gzip
from typing import List, Mapping, Any, Iterable
import logging
import logging.config

import tools.utils.utils as utils
from bel_lang.Config import config

# Globals
namespace_key = 'chembl'
namespace_def = utils.get_namespace(namespace_key)
ns_prefix = namespace_def['namespace']

server = 'ftp.ebi.ac.uk'
source_data_fp = '/pub/databases/chembl/ChEMBLdb/latest/chembl_23_sqlite.tar.gz'

# Local data filepath setup
basename = os.path.basename(source_data_fp)
gzip_flag = False
if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
    gzip_flag = True
    basename = f'{basename}.gz'
local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{basename}'


def get_metadata():
    # Setup metadata info - mostly captured from namespace definition file which
    # can be overridden in belbio_conf.yml file
    dt = datetime.datetime.now().replace(microsecond=0).isoformat()
    metadata = {
        "name": namespace_def['namespace'],
        "namespace": namespace_def['namespace'],
        "description": namespace_def['description'],
        "version": dt,
        "src_url": namespace_def['src_url'],
        "url_template": namespace_def['template_url'],
    }

    return metadata


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    # TODO fix the update - hardcoded to chembl_23
    # update_cycle_days = config['bel_resources']['update_cycle_days']
    result = utils.get_ftp_file(server, source_data_fp, local_data_fp, days_old=60)

    changed = False
    if 'Downloaded' in result[1]:
        changed = True

    return changed


def pref_name_dupes():
    """Check that pref_name in chembl is uniq"""

    check_pref_term_sql = """
        select
            chembl_id, pref_name
        from
            molecule_dictionary
    """
    db_filename = f'{config["bel_resources"]["file_locations"]["downloads"]}/chembl_23/chembl_23_sqlite/chembl_23.db'
    conn = sqlite3.connect(db_filename)
    conn.row_factory = sqlite3.Row

    dupes_flag = False  # set to false if any duplicates exist
    with conn:
        check_pref_term_uniqueness = {}
        for row in conn.execute(check_pref_term_sql):
            pref_name = row['pref_name']
            if pref_name:
                pref_name = pref_name.lower()
            chembl_id = row['chembl_id']
            if check_pref_term_uniqueness.get(pref_name, None):
                log.error(f'CHEMBL pref_name used for multiple chembl_ids {chembl_id}, {check_pref_term_uniqueness["pref_name"]}')
                dupes_flag = True

    return dupes_flag


def query_db() -> Iterable[Mapping[str, Any]]:
    """Generator to run chembl term queries using sqlite chembl db"""
    log.error('This script requires MANUAL interaction to get latest chembl and untar it.')
    db_filename = f'{config["bel_resources"]["file_locations"]["downloads"]}/chembl_23/chembl_23_sqlite/chembl_23.db'
    conn = sqlite3.connect(db_filename)
    conn.row_factory = sqlite3.Row

    main_sql = """
        select
            chembl_id, syn_type, group_concat(synonyms, "||") as syns,
            standard_inchi_key, chebi_par_id, molecule_type, pref_name
        from
            molecule_dictionary md,
            molecule_synonyms ms,
            compound_structures cs
        where
            md.molregno=ms.molregno and
            md.molregno=cs.molregno
        group
            by ms.molregno"""

    with conn:
        for row in conn.execute(main_sql):

            chembl_id = row['chembl_id'].replace('CHEMBL', 'CHEMBL:')
            src_id = row['chembl_id'].replace('CHEMBL', '')
            syns = row['syns']
            syns = syns.lower().split('||')
            pref_name = row['pref_name']
            alt_ids = []

            if pref_name:
                alt_ids.append(chembl_id)
                pref_name = pref_name.lower()
                name = pref_name
                chembl_id = utils.get_prefixed_id(ns_prefix, pref_name)

            elif syns[0]:
                name = syns[0]
            else:
                name = chembl_id

            term = {
                "name": name,
                "pref_name": pref_name,
                "chembl_id": chembl_id,
                "src_id": src_id,
                "alt_ids": alt_ids,
                "syns": copy.copy(syns),
            }
            if row['standard_inchi_key']:
                term['inchi_key'] = f'INCHIKEY:{row["standard_inchi_key"]}'
            if row['chebi_par_id']:
                term['chebi_id'] = f"CHEBI:{row['chebi_par_id']}"

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
    terms_data = config["bel_resources"]["file_locations"]["terms_data"]
    terms_fp = f'{terms_data}/{namespace_key}.jsonl.gz'

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source files')
            return False

    with gzip.open(terms_fp, mode="wt") as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        for row in query_db():

            term = {
                'namespace': ns_prefix,
                'src_id': row['src_id'],
                'id': row['chembl_id'],
                'alt_ids': row['alt_ids'],
                'label': row['name'],
                'name': row['name'],
                'synonyms': copy.copy(list(set(row['syns']))),
                'entity_types': ['Abundance'],
                'equivalences': [],
            }
            if row.get('chebi_id', None):
                term['equivalences'].append(row['chebi_id'])
            if row.get('inchi_key', None):
                term['equivalences'].append(row['inchi_key'])

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({'term': term})))


def main():

    if pref_name_dupes():
        quit()

    # Cannot detect changes as ftp server doesn't support MLSD cmd
    update_data_files()
    build_json()


if __name__ == '__main__':
    # Setup logging
    global log
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging_conf_fn = f'{config["bel_resources"]["file_locations"]["root"]}/logging_conf.yml'
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}-terms')
    main()

