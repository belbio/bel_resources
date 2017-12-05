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

import tools.utils.utils as utils
from bel_lang.Config import config

# Globals
namespace_key = 'rgd'
namespace_def = utils.get_namespace(namespace_key)
ns_prefix = namespace_def['namespace']

tax_id = "TAX:10116"

terms_fp = f'../data/terms/{namespace_key}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = 'ftp.rgd.mcw.edu'
source_data_fp = '/pub/data_release/GENES_RAT.txt'

# Local data filepath setup
basename = os.path.basename(source_data_fp)
gzip_flag = False
if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
    gzip_flag = True
    basename = f'{basename}.gz'
local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_{basename}'


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

    update_cycle_days = config['bel_resources']['update_cycle_days']

    result = utils.get_ftp_file(server, source_data_fp, local_data_fp, gzip_flag=gzip_flag, days_old=update_cycle_days)

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

    # Terminology JSONL output filename
    terms_data = config["bel_resources"]["file_locations"]["terms_data"]
    terms_fp = f'{terms_data}/{namespace_key}.jsonl.gz'

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source file')
            return False

    terms_data = config["bel_resources"]["file_locations"]["terms_data"]
    species_labels_fn = f'{terms_data}/tax_labels.json.gz'
    with gzip.open(species_labels_fn, 'r') as fi:
        species_label = json.load(fi)

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

    with gzip.open(local_data_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        for line in fi:
            if re.match('#|GENE_RGD_ID', line, flags=0):  # many of the file header lines are comments
                continue

            cols = line.split('\t')
            (rgd_id, symbol, name, desc, ncbi_gene_id, uniprot_id, old_symbols, old_names, gene_type) = (cols[0], cols[1], cols[2], cols[3], cols[20], cols[21], cols[29], cols[30], cols[36])
            # print(f'ID: {rgd_id}, S: {symbol}, N: {name}, D: {desc}, nbci_gene_id: {ncbi_gene_id}, up: {uniprot_id}, old_sym: {old_symbols}, old_names: {old_names}, gt: {gene_type}')

            synonyms = [val for val in old_symbols.split(';') + old_names.split(';') if val]

            equivalences = []
            if ncbi_gene_id:
                equivalences.append(f'EG:{ncbi_gene_id}')
            if uniprot_id:
                uniprots = uniprot_id.split(';')
                for uniprot in uniprots:
                    equivalences.append(f'SP:{uniprot}')

            entity_types = []
            if gene_type not in bel_entity_type_map:
                log.error(f'New RGD gene_type not found in bel_entity_type_map {gene_type}')
            else:
                entity_types = bel_entity_type_map[gene_type]

            term = {
                'namespace': ns_prefix,
                'src_id': rgd_id,
                'id': utils.get_prefixed_id(ns_prefix, symbol),
                'alt_ids': [utils.get_prefixed_id(ns_prefix, rgd_id)],
                'label': symbol,
                'name': name,
                'description': desc,
                'species_id': tax_id,
                'species_label': species_label[tax_id],
                'entity_types': copy.copy(entity_types),
                'equivalences': copy.copy(equivalences),
                'synonyms': copy.copy(synonyms),
            }

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({'term': term})))


def main():

    update_data_files()
    build_json()


if __name__ == '__main__':
    # Setup logging
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging_conf_fn = config['bel_resources']['file_locations']['logging_conf_fn']
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}-terms')

    main()
