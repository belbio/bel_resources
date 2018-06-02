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
import re
import gzip

import tools.utils.utils as utils
from tools.utils.Config import config

import tools.setup_logging
import structlog
log = structlog.getLogger(__name__)

# Globals
namespace_key = 'hgnc'
namespace_def = utils.get_namespace(namespace_key, config)
ns_prefix = namespace_def['namespace']

tax_id = "TAX:9606"

server = 'ftp.ebi.ac.uk'
source_data_fp = '/pub/databases/genenames/new/json/hgnc_complete_set.json'

# Local data filepath setup
basename = os.path.basename(source_data_fp)

if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
    basename = f'{basename}.gz'

local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{basename}'


def get_metadata():
    # Setup metadata info - mostly captured from namespace definition file which
    # can be overridden in belbio_conf.yml file
    dt = datetime.datetime.now().replace(microsecond=0).isoformat()
    metadata = {
        "name": namespace_def['namespace'],
        "type": "namespace",
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

    update_cycle_days = config['bel_resources']['update_cycle_days']

    result = utils.get_ftp_file(server, source_data_fp, local_data_fp, days_old=update_cycle_days)

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

    # Terminology JSONL output filename
    data_fp = config["bel_resources"]["file_locations"]["data"]
    terms_fp = f'{data_fp}/namespaces/{namespace_key}.jsonl.gz'

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.info('Will not rebuild data file as it is newer than downloaded source file')
            return False

    species_labels_fn = f'{data_fp}/namespaces/tax_labels.json.gz'
    with gzip.open(species_labels_fn, 'r') as fi:
        species_label = json.load(fi)

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
        'T cell receptor gene': ['Gene', 'RNA', 'Protein'],
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

    with gzip.open(local_data_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        orig_data = json.load(fi)

        for doc in orig_data['response']['docs']:

            # Skip unused entries
            if doc['status'] != 'Approved':
                continue

            hgnc_id = doc['hgnc_id'].replace('HGNC:', '')
            term = {
                'namespace': ns_prefix,
                'namespace_value': doc['symbol'],
                'src_id': hgnc_id,
                'id': utils.get_prefixed_id(ns_prefix, doc['symbol']),
                'alt_ids': [utils.get_prefixed_id(ns_prefix, hgnc_id)],
                'label': doc['symbol'],
                'name': doc['name'],
                'species_id': tax_id,
                'species_label': species_label[tax_id],
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
            term['synonyms'].extend(doc.get('alias_name', []))
            term['synonyms'].extend(doc.get('prev_name', []))

            # Equivalences
            for _id in doc.get('uniprot_ids', []):
                term['equivalences'].append(f"SP:{_id}")

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

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({'term': term})))


def main():

    update_data_files()
    build_json()


if __name__ == '__main__':
    main()

