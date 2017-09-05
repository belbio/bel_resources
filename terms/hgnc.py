#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  hgnc.py

"""

import sys
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
prefix = 'hgnc'
namespace = utils.get_namespace(prefix)

description = "Human Gene Nomenclature Committee namespace"
src_url = "http://www.genenames.org"
url_template = "http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=<src_id>"
terms_fp = '../data/terms/hgnc.json.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = 'ftp.ebi.ac.uk'
remote_file = '/pub/databases/genenames/new/json/hgnc_complete_set.json'
download_fp = '../downloads/hgnc_complete_set.json.gz'

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
    """Build term json load file

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
        'gene with protein product': ['Gene', 'RNA', 'Protein'],
        'RNA, cluster': ['Gene', 'RNA'],
        'RNA, long non-coding': ['Gene', 'RNA'],
        'RNA, micro': ['Gene', 'RNA', 'Micro_RNA'],
        'RNA, ribosomal': ['Gene', 'RNA'],
        'RNA, small cytoplasmic': ['Gene', 'RNA'],
        'RNA, small misc': ['Gene', 'RNA'],
        'RNA, small nuclear': ['Gene', 'RNA'],
        'RNA, small nucleolar': ['Gene', 'RNA'],
        'RNA, transfer': ['Gene', 'RNA'],
        'phenotype only': ['Gene'],
        'RNA, pseudogene': ['Gene', 'RNA'],
        'T-cell receptor pseudogene': ['Gene', 'RNA'],
        'T cell receptor pseudogene': ['Gene', 'RNA'],
        'immunoglobulin pseudogene': ['Gene', 'RNA'],
        'pseudogene': ['Gene', 'RNA'],
        'T-cell receptor gene': ['Gene', 'RNA', 'Protein'],
        'complex locus constituent': ['Gene', 'RNA', 'Protein'],
        'endogenous retrovirus': ['Gene'],
        'fragile site': ['Gene'],
        'immunoglobulin gene': ['Gene', 'RNA', 'Protein'],
        'protocadherin': ['Gene', 'RNA', 'Protein'],
        'readthrough': ['Gene', 'RNA'],
        'region': ['Gene'],
        'transposable element': ['Gene'],
        'unknown': ['Gene', 'RNA', 'Protein'],
        'virus integration site': ['Gene'],
        'RNA, micro': ['Gene', 'RNA', 'Micro_RNA'],
        'RNA, misc': ['Gene', 'RNA'],
        'RNA, Y': ['Gene', 'RNA'],
        'RNA, vault': ['Gene', 'RNA'],

    }

    with gzip.open(download_fp, 'rt') as f:
        orig_data = json.load(f)

    for doc in orig_data['response']['docs']:

        # Skip unused entries
        if doc['status'] != 'Approved':
            continue

        hgnc_id = doc['hgnc_id'].replace('HGNC:', '')
        term = {
            'species': 9606,
            'src_id': hgnc_id,
            'id': doc['symbol'],
            'alt_ids': [hgnc_id],
            'label': doc['symbol'],
            'name': doc['name'],
            'description': '',
            'entity_types': [],
            'equivalences': [],
            'synonyms': [],
            'children': [],
            'obsolete_ids': [],
        }

        # Synonyms
        term['synonyms'].extend(doc.get('synonyms', []))
        term['synonyms'].extend(doc.get('alias_symbol', []))

        # Equivalences
        for _id in doc.get('uniprot_ids', []):
            term['equivalences'].append(f"UP:{_id}")

        if 'entrez_id' in doc:
            term['equivalences'].append(f"EG:{doc['entrez_id']}")

        # Entity types
        if doc['locus_type'] in bel_entity_type_map:
            term['entity_types'] = bel_entity_type_map[doc['locus_type']]
        else:
            log.error(f'New HGNC locus_type not found in bel_entity_type_map {doc["locus_type"]}')

        # Obsolete Namespace IDs
        if 'prev_symbol' in doc:
            for prev_id in doc['prev_symbol']:
                term['obsolete_ids'].append(prev_id)

        # Add term to terms
        terminology['terms'].append(copy.deepcopy(term))

    with gzip.open(terms_fp, 'wt') as f:
        json.dump(terminology, f, indent=4)


def main():
    # Cannot detect changes as ftp server doesn't support MLSD cmd
    update_data_files()
    build_json()


if __name__ == '__main__':
    main()

