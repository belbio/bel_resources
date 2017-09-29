#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  mgi.py

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
prefix = 'mgi'
namespace = utils.get_namespace(prefix)
ns_prefix = namespace['namespace']

terms_fp = f'../data/terms/{prefix}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

# File description: http://www.informatics.jax.org/downloads/reports/index.html
# http://www.informatics.jax.org/downloads/reports/MRK_List1.rpt (including withdrawn marker symbols)
# http://www.informatics.jax.org/downloads/reports/MRK_List2.rpt (excluding withdrawn marker symbols)


url_main = "http://www.informatics.jax.org/downloads/reports/MRK_List2.rpt"
url2 = "http://www.informatics.jax.org/downloads/reports/MRK_SwissProt.rpt"  # Equivalences
url3 = "http://www.informatics.jax.org/downloads/reports/MGI_EntrezGene.rpt"  # Equivalences

download_fp_main = f'../downloads/mgi_MRK_List2.rpt.gz'
download_fp2 = f'../downloads/mgi_MRK_SwissProt.rpt.gz'
download_fp3 = f'../downloads/mgi_MGI_EntrezGene.rpt.gz'

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

    (changed_main, response_main) = utils.get_web_file(url_main, download_fp_main, gzipflag=True)
    (changed2, response2) = utils.get_web_file(url2, download_fp2, gzipflag=True)
    (changed3, response3) = utils.get_web_file(url3, download_fp3, gzipflag=True)

    if changed_main and response_main.getcode() != 200:
        log.error(f'Could not get url {url_main}')

    return changed_main


def build_json(force: bool = False):
    """Build RGD namespace json load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, download_fp_main):
            log.warning('Will not rebuild data file as it is newer than downloaded source file')
            return False

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
    with gzip.open(download_fp2, 'rt') as fi:

        for line in fi:
            cols = line.rstrip().split('\t')
            (mgi_id, sp_accession) = (cols[0], cols[6])
            mgi_id = mgi_id.replace('MGI:', '')
            sp_eqv[mgi_id] = sp_accession.split(' ')

    eg_eqv = {}
    with gzip.open(download_fp3, 'rt') as fi:

        for line in fi:
            cols = line.split('\t')
            (mgi_id, eg_id) = (cols[0], cols[8])
            mgi_id = mgi_id.replace('MGI:', '')
            eg_eqv[mgi_id] = [eg_id]

    with gzip.open(download_fp_main, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({'metadata': terminology_metadata})))

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
                'species': 'TAX:10090',
                'entity_types': copy.copy(entity_types),
                'equivalences': copy.copy(equivalences),
            }
            if len(synonyms):
                term['synonyms'] = copy.copy(synonyms)

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({'term': term})))


def main():

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

