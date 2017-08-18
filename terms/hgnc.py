#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  hgnc.py

"""

import sys
import tarfile
import os
import tempfile
import json
import datetime
import copy
import gzip
from typing import Mapping, List, Any
import logging

sys.path.append("..")
import utils

ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
ch.setFormatter(formatter)

log = logging.getLogger('hgnc-terms')
log.addHandler(ch)
log.setLevel(logging.ERROR)  # This toggles all the logging in your app

# Globals
hgnc_json = '../data/terms/hgnc.json.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
hgnc_orig = '../downloads/hgnc_complete_set.json'
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

terminology = {
    "name": "HGNC",
    "namespace": "HGNC",
    "description": "Human Gene Nomenclature Committee namespace",
    "version": dt,
    "src_url": "http://www.genenames.org",
    "url_template": "http://www.genenames.org/cgi-bin/gene_symbol_report?hgnc_id=<ID>",
    "terms": [],
}


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """
    # Update data file

    server = 'ftp.ebi.ac.uk'
    remote_file = '/pub/databases/genenames/new/json/hgnc_complete_set.json'
    # filename = os.path.basename(rfile)

    result = utils.get_ftp_file(server, remote_file, hgnc_orig)

    changed = False
    if 'Downloaded' in result[1]:
        changed = True

    return changed


def build_json(force: bool = False):
    """Build HGNC namespace json load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(hgnc_json, hgnc_orig):
            return False

    with open(hgnc_orig, 'r') as f:
        hgnc_data = json.load(f)

    for doc in hgnc_data['response']['docs']:

        # Skip unused entries
        if doc['status'] != 'Approved':
            continue

        term = {
            'entity_types': [], 'xref_ids': [], 'alias_ids': [], 'synonyms': [],
            'children': [], 'obsolete_ids': [],
        }

        term['species'] = 'TAXID:9606:human'
        term['id'] = doc['symbol']
        term['label'] = doc['symbol']
        term['src_id'] = doc['hgnc_id']
        term['name'] = doc['name']
        term['description'] = ''

        term['synonyms'].extend(doc.get('synonyms', []))
        term['synonyms'].extend(doc.get('alias_symbol', []))
        term['alias_ids'] = [doc['hgnc_id']]

        for _id in doc.get('uniprot_ids', []):
            term['xref_ids'].append({'id': _id, 'source': 'Uniprot'})

        for _id in doc.get('mgd_id', []):
            term['xref_ids'].append({'id': _id, 'source': 'MGD'})

        for _id in doc.get('rgd_id', []):
            term['xref_ids'].append({'id': _id, 'source': 'RGD'})

        if 'entrez_id' in doc:
            term['xref_ids'].append({'id': doc['entrez_id'], 'source': 'EntrezGene'})

        doc['entity_types'] = ['Gene']

        if 'RNA, long non-coding' in doc['locus_type']:
            doc['entity_types'].extend(['RNA'])

        if 'protein product' in doc['locus_type']:
            doc['entity_types'].extend(['RNA', 'Protein'])

        if 'RNA, micro' in doc['locus_type']:
            doc['entity_types'].extend(['Micro_RNA'])

        for prev_id in doc.get('prev_symbol', []):
            term['obsolete_ids'].extend(prev_id)

        terminology['terms'].append(copy.deepcopy(doc))

    with gzip.open(hgnc_json, 'wt') as f:
        json.dump(terminology, f, indent=4)


def main():
    # Cannot detect changes as ftp server doesn't support it
    update_data_files()
    build_json()


if __name__ == '__main__':
    main()

