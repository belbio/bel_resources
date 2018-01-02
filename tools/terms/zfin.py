#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  TEMPLATE.py

"""

import re
import os
import json
import yaml
import datetime
import copy
import gzip
import logging
import logging.config

import tools.utils.utils as utils
from tools.utils.Config import config


"""
1.  Set up globals - what files to download, any adjustments to metadata, filenames, etc
    update 'REPLACEME' text
2.  Download source data files [update_data_files()]
3.  Dataset preprocessing - e.g. double check term names for duplicates if you
    plan on using them for IDs, pre-collect information needed to build the term record
4.  Process terms and write them to terms_fp file
    filter out species not in config['bel_resources']['species_list'] unless empty list
"""

# Globals ###################################################################
namespace_key = 'zfin'  # namespace key into namespace definitions file
namespace_def = utils.get_namespace(namespace_key, config)
ns_prefix = namespace_def['namespace']


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
    # Can override/hard-code update_cycle_days in each term collection file if desired

    files = [
        "https://zfin.org/downloads/aliases.txt",
        "https://zfin.org/downloads/gene.txt",
        "https://zfin.org/downloads/transcripts.txt",
    ]

    for url in files:
        # Local data filepath setup
        basename = os.path.basename(url)
        gzip_flag = False
        if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
            gzip_flag = True
            basename = f'{basename}.gz'

        # Pick one of the two following options
        local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_{basename}'

        # Get web file - but not if local downloaded file is newer
        utils.get_web_file(url, local_data_fp, gzip_flag=gzip_flag, days_old=update_cycle_days)


def build_json(force: bool = False):
    """Build term JSONL file"""

    # Terminology JSONL output filename
    terms_data = config["bel_resources"]["file_locations"]["terms_data"]
    terms_fp = f'{terms_data}/{namespace_key}.jsonl.gz'

    aliases_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_aliases.txt.gz'
    genes_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_gene.txt.gz'
    transcripts_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_transcripts.txt.gz'

    # used if you need a tmp dir to do some processing
    # tmpdir = tempfile.TemporaryDirectory()

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, aliases_fp) and utils.file_newer(terms_fp, genes_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source files')
            return False

    terms = {}
    with gzip.open(aliases_fp, 'rt') as fi:
        for line in fi:
            if re.match('ZDB-GENE-', line):
                (src_id, name, symbol, syn, *extra) = line.split('\t')
                # print('Syn', syn)
                if src_id in terms:
                    terms[src_id]['synonyms'].append(syn)
                else:
                    terms[src_id] = {'name': name, 'symbol': symbol, 'synonyms': [syn]}

    with gzip.open(transcripts_fp, 'rt') as fi:
        transcript_types = {}
        for line in fi:
            (tscript_id, so_id, name, gene_id, clone_id, tscript_type, status, *extra) = line.split('\t')
            if 'withdrawn' in status.lower() or 'artifact' in status.lower():
                continue
            if gene_id in transcript_types:
                transcript_types[gene_id][tscript_type] = 1
            else:
                transcript_types[gene_id] = {tscript_type: 1}

        for gene_id in transcript_types:
            types = transcript_types[gene_id].keys()

            entity_types = []
            for type_ in types:
                if type_ in ['lincRNA', 'ncRNA', 'scRNA', 'snRNA', 'snoRNA']:
                    entity_types.extend(['Gene', 'RNA'])
                if type_ in ['mRNA']:
                    entity_types.extend(['Gene', 'RNA', 'Protein'])
                if type_ in ['miRNA']:
                    entity_types.extend(['Gene', 'Micro_RNA'])

            entity_types = list(set(entity_types))

            if gene_id == 'ZDB-GENE-030115-1':
                print('Entity types', entity_types, 'Types', types)

            if gene_id in terms:
                terms[gene_id]['entity_types'] = list(entity_types)
            else:
                terms[gene_id] = {'name': name, 'entity_types': list(entity_types)}

    with gzip.open(genes_fp, 'rt') as fi:
        for line in fi:
            (src_id, so_id, symbol, eg_id, *extra) = line.split('\t')
            if src_id in terms:
                terms[src_id]['equivalences'] = [f'EG:{eg_id}']
                if terms[src_id].get('symbol', None) and symbol:
                    terms[src_id]['symbol'] = symbol
            else:
                log.debug(f'No term record for ZFIN {src_id} to add equivalences to')
                continue

    with gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        for term in terms:

            main_id = terms[term].get('symbol', terms[term].get('name', term))

            term = {
                'namespace': ns_prefix,
                'src_id': term,
                'id': f'{ns_prefix}:{main_id}',
                'alt_ids': [],
                'label': main_id,
                'name': terms[term].get('name', term),
                'species_id': "TAX:7955",
                'species_label': "zebrafish",
                'synonyms': copy.copy(list(set(terms[term].get('synonyms', [])))),
                'entity_types': copy.copy(terms[term].get('entity_types', [])),
                'equivalences': copy.copy(terms[term].get('equivalences', [])),
            }

            # Add term to JSONLines file
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
