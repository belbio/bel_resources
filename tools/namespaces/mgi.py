#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  mgi.py

"""

import sys
import os
import tempfile
import json
import re
import yaml
import datetime
import copy
import gzip
import logging
import logging.config

import tools.utils.utils as utils
from tools.utils.Config import config

# Globals
namespace_key = 'mgi'
namespace_def = utils.get_namespace(namespace_key, config)
ns_prefix = namespace_def['namespace']

tax_id = "TAX:10090"

terms_fp = f'../data/terms/{namespace_key}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

# File description: http://www.informatics.jax.org/downloads/reports/index.html
# http://www.informatics.jax.org/downloads/reports/MRK_List1.rpt (including withdrawn marker symbols)
# http://www.informatics.jax.org/downloads/reports/MRK_List2.rpt (excluding withdrawn marker symbols)


url_main = "http://www.informatics.jax.org/downloads/reports/MRK_List2.rpt"
url2 = "http://www.informatics.jax.org/downloads/reports/MRK_SwissProt.rpt"  # Equivalences
url3 = "http://www.informatics.jax.org/downloads/reports/MGI_EntrezGene.rpt"  # Equivalences

# Local data filepath setup
basename = os.path.basename(url_main)

if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
    basename = f'{basename}.gz'

local_data_main_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_{basename}'

basename = os.path.basename(url2)

if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped

    basename = f'{basename}.gz'
local_data_2_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_{basename}'

basename = os.path.basename(url3)

if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped

    basename = f'{basename}.gz'
local_data_3_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_{basename}'


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

    (changed_main, response_main) = utils.get_web_file(url_main, local_data_main_fp, days_old=update_cycle_days)
    (changed2, response2) = utils.get_web_file(url2, local_data_2_fp, days_old=update_cycle_days)
    (changed3, response3) = utils.get_web_file(url3, local_data_3_fp, days_old=update_cycle_days)

    log.info(response_main)

    return changed_main


def build_json(force: bool = False):
    """Build RGD namespace json load file

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
        if utils.file_newer(terms_fp, local_data_main_fp):
            log.info('Will not rebuild data file as it is newer than downloaded source file')
            return False

    species_labels_fn = f'{data_fp}/namespaces/tax_labels.json.gz'
    with gzip.open(species_labels_fn, 'r') as fi:
        species_label = json.load(fi)

    # Map gene_types to BEL entity types
    bel_entity_type_map = {
        'gene': ['Gene', 'RNA', 'Protein'],
        'protein coding gene': ['Gene', 'RNA', 'Protein'],
        'non-coding RNA gene': ['Gene', 'RNA'],
        'rRNA gene': ['Gene', 'RNA'],
        'tRNA gene': ['Gene', 'RNA'],
        'snRNA gene': ['Gene', 'RNA'],
        'snoRNA gene': ['Gene', 'RNA'],
        'miRNA gene': ['Gene', 'RNA', 'Micro_RNA'],
        'scRNA gene': ['Gene', 'RNA'],
        'lincRNA gene': ['Gene', 'RNA'],
        'lncRNA gene': ['Gene', 'RNA'],
        'intronic lncRNA gene': ['Gene', 'RNA'],
        'sense intronic lncRNA gene': ['Gene', 'RNA'],
        'sense overlapping lncRNA gene': ['Gene', 'RNA'],
        'bidirectional promoter lncRNA gene': ['Gene', 'RNA'],
        'antisense lncRNA gene': ['Gene', 'RNA'],
        'ribozyme gene': ['Gene', 'RNA'],
        'RNase P RNA gene': ['Gene', 'RNA'],
        'RNase MRP RNA gene': ['Gene', 'RNA'],
        'telomerase RNA gene': ['Gene', 'RNA'],
        'unclassified non-coding RNA gene': ['Gene', 'RNA'],
        'heritable phenotypic marker': ['Gene'],
        'gene segment': ['Gene'],
        'unclassified gene': ['Gene', 'RNA', 'Protein'],
        'other feature types': ['Gene'],
        'pseudogene': ['Gene', 'RNA'],
        'transgene': ['Gene'],
        'other genome feature': ['Gene'],
        'pseudogenic region': ['Gene', 'RNA'],
        'polymorphic pseudogene': ['Gene', 'RNA', 'Protein'],
        'pseudogenic gene segment': ['Gene', 'RNA'],
        'SRP RNA gene': ['Gene', 'RNA']
    }

    sp_eqv = {}
    with gzip.open(local_data_2_fp, 'rt') as fi:

        for line in fi:
            cols = line.rstrip().split('\t')
            (mgi_id, sp_accession) = (cols[0], cols[6])
            mgi_id = mgi_id.replace('MGI:', '')
            sp_eqv[mgi_id] = sp_accession.split(' ')

    eg_eqv = {}
    with gzip.open(local_data_3_fp, 'rt') as fi:

        for line in fi:
            cols = line.split('\t')
            (mgi_id, eg_id) = (cols[0], cols[8])
            mgi_id = mgi_id.replace('MGI:', '')
            eg_eqv[mgi_id] = [eg_id]

    with gzip.open(local_data_main_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        firstline = fi.readline()
        firstline = firstline.split('\t')

        for line in fi:
            cols = line.split('\t')
            (mgi_id, symbol, name, marker_type, gene_type, synonyms) = (cols[0], cols[6], cols[8], cols[9], cols[10], cols[11],)

            mgi_id = mgi_id.replace('MGI:', '')

            # Skip non-gene entries
            if marker_type != 'Gene':
                continue

            synonyms = synonyms.rstrip()
            if synonyms:
                synonyms = synonyms.split('|')

            if gene_type not in bel_entity_type_map:
                log.error(f'Unknown gene_type found {gene_type}')
                entity_types = None
            else:
                entity_types = bel_entity_type_map[gene_type]

            equivalences = []
            if mgi_id in sp_eqv:
                for sp_accession in sp_eqv[mgi_id]:
                    if not sp_accession:
                        continue
                    equivalences.append(f'SP:{sp_accession}')
            if mgi_id in eg_eqv:
                for eg_id in eg_eqv[mgi_id]:
                    if not eg_id:
                        continue
                    equivalences.append(f'EG:{eg_id}')

            term = {
                'namespace': ns_prefix,
                'src_id': mgi_id,
                'id': utils.get_prefixed_id(ns_prefix, symbol),
                'alt_ids': [utils.get_prefixed_id(ns_prefix, mgi_id)],
                'label': symbol,
                'name': name,
                'species_id': tax_id,
                'species_label': species_label[tax_id],
                'entity_types': copy.copy(entity_types),
                'equivalences': copy.copy(equivalences),
            }
            if len(synonyms):
                term['synonyms'] = copy.copy(synonyms)

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({'term': term})))


def main():

    update_data_files()
    build_json()


if __name__ == '__main__':
    # Setup logging
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging.config.dictConfig(config['logging'])
    log = logging.getLogger(f'{module_fn}-namespaces')

    main()
