#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  rgd.py

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

module_fn = os.path.basename(__file__)
module_fn = module_fn.replace('.py', '')

# Setup logging
logging_conf_fn = '../logging-conf.yaml'
with open(logging_conf_fn, mode='r') as f:
    logging.config.dictConfig(yaml.load(f))
log = logging.getLogger(f'{module_fn}-terms')

# Import local util module
sys.path.append("..")
import utils
# Globals
prefix = 'rgd'
namespace = utils.get_namespace(prefix)

terms_fp = f'../data/terms/{prefix}.json.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = 'ftp.rgd.mcw.edu'
remote_file = '/pub/data_release/GENES_RAT.txt'
download_fp = f'../downloads/rgd_GENES_RAT.txt.gz'


terminology = {
    "name": namespace['namespace'],
    "namespace": namespace['namespace'],
    "description": namespace['description'],
    "version": dt,
    "src_url": namespace['src_url'],
    "url_template": namespace['template_url'],
    "terms": [],
}


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    result = utils.get_ftp_file(server, remote_file, download_fp, gzipflag=True)

    changed = False
    if 'Downloaded' in result[1]:
        changed = True

    return changed


def build_json(force: bool = False):
    """Build RGD namespace json load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, download_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source file')
            return False

    # Map gene_types to BEL entity types
    bel_entity_type_map = {
        'pseudo': ['Gene', 'RNA'],
        "protein-coding": ['Gene', 'RNA', 'Protein'],
        "ncrna": ['Gene', 'RNA'],
        "gene": ['Gene', 'RNA', 'Protein'],
        "snrna": ['Gene', 'RNA'],
        "trna": ['Gene', 'RNA'],
        "rrna": ['Gene', 'RNA'],
    }

    with gzip.open(download_fp, 'rt') as f:
        for line in f:
            if re.match('#|GENE_RGD_ID', line, flags=0):
                continue
            cols = line.split('\t')
            (rgd_id, symbol, name, desc, ncbi_gene_id, uniprot_id, old_symbols, old_names, gene_type) = (cols[0], cols[1], cols[2], cols[3], cols[20], cols[21], cols[29], cols[30], cols[36])
            # print(f'ID: {rgd_id}, S: {symbol}, N: {name}, D: {desc}, nbci_gene_id: {ncbi_gene_id}, up: {uniprot_id}, old_sym: {old_symbols}, old_names: {old_names}, gt: {gene_type}')

            synonyms = [val for val in old_symbols.split(';') + old_names.split(';') if val]

            xref_ids = []
            if ncbi_gene_id:
                xref_ids.append(f'EG:{ncbi_gene_id}')
            if uniprot_id:
                xref_ids.append(f'UP:{uniprot_id}')

            entity_types = []
            if gene_type not in bel_entity_type_map:
                log.error(f'New RGD gene_type not found in bel_entity_type_map {gene_type}')
            else:
                entity_types = bel_entity_type_map[gene_type]

            term = {
                'src_id': rgd_id,
                'id': symbol,
                'alt_ids': [rgd_id],
                'label': symbol,
                'name': name,
                'description': desc,
                'species': 10116,
                'entity_types': entity_types,
                'synonyms': synonyms,

            }
            # Add term to terms
            terminology['terms'].append(copy.deepcopy(term, memo=None, _nil=[]))

    with gzip.open(terms_fp, 'wt') as f:
        json.dump(terminology, f, indent=4)


def main():

    # Cannot detect changes as ftp server doesn't support MLSD cmd
    update_data_files()
    build_json()


if __name__ == '__main__':
    main()

