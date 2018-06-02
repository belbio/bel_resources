#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  eg.py

"""

import re
import os
import json
import yaml
import datetime
import copy
import gzip

import tools.utils.utils as utils
from tools.utils.Config import config

import tools.setup_logging
import structlog
log = structlog.getLogger(__name__)

# Globals
namespace_key = 'eg'
namespace_def = utils.get_namespace(namespace_key, config)
ns_prefix = namespace_def['namespace']

# ftp://ftp.ncbi.nih.gov/gene/DATA/gene_history.gz
# ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/
# ftp://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/All_Data.gene_info.gz

server = "ftp.ncbi.nlm.nih.gov"
source_data_fp = '/gene/DATA/GENE_INFO/All_Data.gene_info.gz'
source_data_history_fp = '/gene/DATA/gene_history.gz'

# Local data filepath setup
basename = os.path.basename(source_data_fp)

if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
    basename = f'{basename}.gz'

local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_{basename}'

basename = os.path.basename(source_data_history_fp)

if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped

    basename = f'{basename}.gz'
local_data_history_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_{basename}'

# Terminology JSONL output filename
data_fp = config["bel_resources"]["file_locations"]["data"]
terms_fp = f'{data_fp}/namespaces/{namespace_key}.jsonl.gz'
terms_hmrz_fp = f'{data_fp}/namespaces/{namespace_key}_hmrz.jsonl.gz'  # Human, mouse, rat and zebrafish EG dataset
hmrz_species = ['TAX:9606', 'TAX:10090', 'TAX:10116', 'TAX:7955']


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

    result = utils.get_ftp_file(server, source_data_history_fp, local_data_history_fp, days_old=update_cycle_days)
    result = utils.get_ftp_file(server, source_data_fp, local_data_fp, days_old=update_cycle_days)

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
    with gzip.open(local_data_history_fp, 'rt') as fi:

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

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.info('Will not rebuild data file as it is newer than downloaded source file')
            return False

    species_labels_fn = f'{data_fp}/namespaces/tax_labels.json.gz'
    with gzip.open(species_labels_fn, 'r') as fi:
        species_label = json.load(fi)

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

    history = get_history()

    with gzip.open(local_data_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        fi.__next__()  # skip header line

        for line in fi:

            cols = line.split('\t')
            (tax_src_id, gene_id, symbol, synonyms, desc, gene_type, name) = (cols[0], cols[1], cols[2], cols[4], cols[8], cols[9], cols[11], )
            tax_id = f'TAX:{tax_src_id}'

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

            if name == '-':
                name = symbol

            term = {
                'namespace': ns_prefix,
                'namespace_value': gene_id,
                'src_id': gene_id,
                'id': utils.get_prefixed_id(ns_prefix, gene_id),
                'label': symbol,
                'name': name,
                'description': desc,
                'species_id': tax_id,
                'species_label': species_label.get(tax_id, None),
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


def build_hmr_json():
    """Extract Human, Mouse and Rat from EG into a new file """

    with gzip.open(terms_fp, 'rt') as fi, gzip.open(terms_hmrz_fp, 'wt') as fo:
        for line in fi:
            doc = json.loads(line)
            if 'term' in doc and doc['term']['species_id'] in hmrz_species:
                fo.write("{}\n".format(json.dumps(doc)))
            elif 'metadata' in doc:
                fo.write("{}\n".format(json.dumps(doc)))


def main():

    update_data_files()
    build_json()
    build_hmr_json()  # human, mouse, rat filtered EG namespace


if __name__ == '__main__':
    main()
