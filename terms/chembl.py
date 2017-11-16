#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  swissprot.py

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

# Import local util module
sys.path.append("..")
import utils

# Globals
prefix = 'chembl'
namespace = utils.get_namespace(prefix)
ns_prefix = namespace['namespace']

terms_fp = f'../data/terms/{prefix}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = 'ftp.ebi.ac.uk'
remote_file_compounds = '/pub/databases/chembl/ChEMBLdb/latest/chembl_23_sqlite.tar.gz'
download_compounds_fp = f'../downloads/chembl_23_sqlite.tar.gz'

terminology_metadata = {
    "name": namespace['namespace'],
    "namespace": namespace['namespace'],
    "description": namespace['description'],
    "version": dt,
    "src_url": namespace['src_url'],
    "url_template": namespace['template_url'],
}


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    result = utils.get_ftp_file(server, remote_file_compounds, download_compounds_fp, days_old=60)

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
    db_filename = '../downloads/chembl_23/chembl_23_sqlite/chembl_23.db'
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
    db_filename = '../downloads/chembl_23/chembl_23_sqlite/chembl_23.db'
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
            if pref_name:
                pref_name = pref_name.lower()
                name = pref_name
                alt_id = chembl_id
                if re.search('\s+', pref_name):
                    chembl_id = f'CHEMBL:"{pref_name}"'
                else:
                    chembl_id = f'CHEMBL:{pref_name}'
            elif syns[0]:
                name = syns[0]
            else:
                name = chembl_id

            term = {
                "name": name,
                "pref_name": pref_name,
                "chembl_id": chembl_id,
                "src_id": src_id,
                "alt_id": alt_id,
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

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, download_compounds_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source files')
            return False

    with gzip.open(terms_fp, mode="wt") as fo:

        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({'metadata': terminology_metadata})))

        for row in query_db():

            term = {
                'namespace': ns_prefix,
                'src_id': row['src_id'],
                'id': row['chembl_id'],
                'alt_ids': [row['alt_id']],
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

    logging_conf_fn = '../logging-conf.yaml'
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}-terms')
    main()

