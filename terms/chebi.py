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
import copy
import gzip
from typing import List, Mapping, Any
import logging
import logging.config

# Import local util module
sys.path.append("..")
import utils

# Globals
prefix = 'chebi'
namespace = utils.get_namespace(prefix)
ns_prefix = namespace['namespace']

terms_fp = f'../data/terms/{prefix}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

# ftp://ftp.ebi.ac.uk/pub/databases/chebi/Flat_file_tab_delimited/compounds.tsv.gz
# ftp://ftp.ebi.ac.uk/pub/databases/chebi/Flat_file_tab_delimited/names.tsv.gz

server = 'ftp.ebi.ac.uk'
remote_file_compounds = '/pub/databases/chebi/Flat_file_tab_delimited/compounds.tsv.gz'
remote_file_names = '/pub/databases/chebi/Flat_file_tab_delimited/names.tsv.gz'
download_compounds_fp = f'../downloads/chebi_compounds.tsv.gz'
download_names_fp = f'../downloads/chebi_names.tsv.gz'

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

    result = utils.get_ftp_file(server, remote_file_compounds, download_compounds_fp)
    result = utils.get_ftp_file(server, remote_file_names, download_names_fp)

    changed = False
    if 'Downloaded' in result[1]:
        changed = True

    return changed


def build_json(force: bool = False):
    """Build CHEBI namespace json load file

    Have to build this as a JSON term file since there are multiple tables that
    have to be joined and records collapsed to the Parent ID.

    Args:
        force (bool): build jsonl result regardless of file mod dates

    Returns:
        None
    """

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, download_compounds_fp) and utils.file_newer(terms_fp, download_names_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source files')
            return False

    names = {}
    # collect chebi names and synonyms
    with gzip.open(download_names_fp, mode="rt") as fi:
        fi.__next__()  # skip header line
        for line in fi:
            cols = line.rstrip().split('\t')
            (chebi_id, name_type, name, language) = (cols[1], cols[2], cols[4], cols[6])
            # print(f'ID: {chebi_id}, Type: {name_type}, Name: {name}, L: {language}')
            if language != 'en':
                continue

            if chebi_id not in names:
                names[chebi_id] = {'synonyms': []}

            names[chebi_id]['synonyms'].append(name)

    with open('names.txt', 'w') as f:
        json.dump(names, f, indent=4)

    chebi = {}
    # Collect chebi ids, description and obsolete_ids
    with gzip.open(download_compounds_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({'metadata': terminology_metadata})))

        fi.__next__()  # skip header line
        for line in fi:
            cols = line.rstrip().split('\t')
            (chebi_id, parent_id, name, description) = (cols[0], cols[4], cols[5], cols[6])

            # print(f'ID: {chebi_id}, Parent: {parent_id}, {parent_id == "null"}, {chebi.get(chebi_id, None)}')
            if parent_id == 'null' and chebi.get(chebi_id, None) is None:
                chebi[chebi_id] = {'id': chebi_id, 'name': name, 'description': description, 'obsolete_ids': []}
            elif parent_id == 'null':
                chebi[chebi_id]['id'] = chebi_id
                chebi[chebi_id]['description'] = description
            elif parent_id and chebi.get(parent_id, None) is None:
                chebi[parent_id] = {'id': parent_id, 'description': description, 'obsolete_ids': [chebi_id]}
            elif parent_id:
                chebi[parent_id]['obsolete_ids'].append(chebi_id)

        for chebi_id in chebi:

            name = chebi[chebi_id]['name']
            synonyms = []
            if chebi_id in names:
                synonyms = names[chebi_id]['synonyms']

            term = {
                'namespace': ns_prefix,
                'src_id': chebi_id,
                'id': utils.get_prefixed_id(ns_prefix, chebi_id),
                'label': name,
                'name': name,
                'description': chebi[chebi_id]['description'],
                'entity_types': ['Abundance'],
            }

            if len(synonyms):
                term['synonyms'] = copy.copy(synonyms)

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({'term': term})))


def main():

    # Setup logging
    global log
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging_conf_fn = '../logging-conf.yaml'
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}-terms')

    # Cannot detect changes as ftp server doesn't support MLSD cmd
    update_data_files()
    build_json()


if __name__ == '__main__':
    main()

