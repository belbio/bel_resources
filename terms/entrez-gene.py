#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  eg.py

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
import logging
import logging.config

# Import local util module
sys.path.append("..")
import utils

# Globals
prefix = 'eg'
namespace = utils.get_namespace(prefix)
ns_prefix = namespace['namespace']

terms_fp = f'../data/terms/{prefix}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

# ftp://ftp.ncbi.nih.gov/gene/DATA/gene_history.gz
# ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/
# ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/All_Data.gene_info.gz

server = "ftp.ncbi.nlm.nih.gov"
remote_file = '/gene/DATA/GENE_INFO/All_Data.gene_info.gz'
remote_file_history = '/gene/DATA/gene_history.gz'
download_fp = f'../downloads/eg_entrez_all_gene_info.gz'
download_history_fp = '../downloads/eg_entrez_gene_history.gz'

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

    result = utils.get_ftp_file(server, remote_file_history, download_history_fp)
    result = utils.get_ftp_file(server, remote_file, download_fp)

    changed = False
    if 'Downloaded' in result[1]:
        changed = True

    return changed


def get_history():
    """Get history of gene records

    Returns:
        Mapping[str, Mapping[str, int]]: history dict of dicts - new gene_id and old_gene_id
    """

    history = {}
    with gzip.open(download_history_fp, 'rt') as fi:

        fi.__next__()  # skip header line

        for line in fi:
            cols = line.split('\t')

            (gene_id, old_gene_id,) = (cols[1], cols[2],)
            if gene_id != '-':
                if history.get(gene_id, None):
                    history[gene_id] = {old_gene_id: 1}

    return history


def build_json(force: bool = False):
    """Build EG namespace json load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    history = get_history()

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, download_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source file')
            return False

    missing_entity_types = {}
    bel_entity_type_map = {
        'snoRNA': ['Gene', 'RNA'],
        'snRNA': ['Gene', 'RNA'],
        'ncRNA': ['Gene', 'RNA'],
        'tRNA': ['Gene', 'RNA'],
        'scRNA': ['Gene', 'RNA'],
        'other': ['Gene'],
        'pseudo': ['Gene', 'RNA'],
        'unknown': ['Gene', 'RNA', 'Protein'],
        'protein-coding': ['Gene', 'RNA', 'Protein'],
        'rRNA': ['Gene', 'RNA'],
    }

    with gzip.open(download_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:
        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({'metadata': terminology_metadata})))

        fi.__next__()  # skip header line

        for line in fi:

            cols = line.split('\t')
            (tax_id, gene_id, symbol, synonyms, desc, gene_type, name) = (cols[0], cols[1], cols[2], cols[4], cols[8], cols[9], cols[11], )

            synonyms = synonyms.rstrip()
            if synonyms == '-':
                synonyms = None
            else:
                synonyms = synonyms.split('|')

            if gene_type in ['miscRNA', 'biological-region']:  # Skip gene types
                continue
            elif gene_type not in bel_entity_type_map:
                log.error(f'Unknown gene_type found {gene_type}')
                missing_entity_types[gene_type] = 1
                entity_types = None
            else:
                entity_types = bel_entity_type_map[gene_type]

            term = {
                'namespace': ns_prefix,
                'src_id': gene_id,
                'id': utils.get_prefixed_id(ns_prefix, gene_id),
                'label': symbol,
                'name': name,
                'description': desc,
                'species': f'TAX:{tax_id}',
            }
            if name != '-':
                term['name'] = name

            if synonyms:
                term['synonyms'] = copy.copy(synonyms)

            if entity_types:
                term['entity_types'] = copy.copy(entity_types)

            if gene_id in history:
                term['obsolete_ids'] = history[gene_id].keys()

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({'term': term})))

    if missing_entity_types:
        log.error('Missing Entity Types:\n', json.dumps(missing_entity_types))


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

