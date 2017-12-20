#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  go.py

"""

import sys
import os
import tempfile
import json
from typing import Mapping, Any
import yaml
import datetime
import copy
import re
import gzip
import pronto
import logging
import logging.config


# TODO: should molecular function branch be tagged as entity_type: Activity?

import tools.utils.utils as utils
from tools.utils.Config import config

# Globals
namespace_key = 'go'
namespace_def = utils.get_namespace(namespace_key, config)
ns_prefix = namespace_def['namespace']

url = 'http://purl.obolibrary.org/obo/go.obo'

# Local data filepath setup
basename = os.path.basename(url)
gzip_flag = False
if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
    gzip_flag = True
    basename = f'{basename}.gz'
local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{basename}'


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

    (changed, response) = utils.get_web_file(url, local_data_fp, gzip_flag=gzip_flag, days_old=update_cycle_days)
    if changed and response.getcode() != 200:
        log.error(f'Could not get url {url}')

    return changed


def add_complex_entity_type(start_id: str, ontology: Mapping[str, Any]) -> None:
    """Add complex entity type to children of GO:0032991 -- macromolecular complex

    Args:
        start_id (int): parent id of GO terms of complexes

    Returns:
        None
    """

    ontology[start_id].other['entity_type'] = 'Complex'

    for c in ontology[start_id].children:
        add_complex_entity_type(c.id, ontology)


def process_obo(force: bool = False):

    # Terminology JSONL output filename
    terms_data = config["bel_resources"]["file_locations"]["terms_data"]
    terms_fp = f'{terms_data}/{namespace_key}.jsonl.gz'

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source file')
            return False

    with gzip.open(local_data_fp, 'r') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        ont = pronto.Ontology(fi)
        add_complex_entity_type('GO:0032991', ont)
        unique_names = {}

        for ont_term in ont:
            if 'is_obsolete' in ont_term.other:
                continue

            src_id = ont_term.id.replace('GO:', '')
            term = {
                'namespace': ns_prefix,
                'src_id': src_id,
                'id': utils.get_prefixed_id(ns_prefix, ont_term.name),
                'alt_ids': [utils.get_prefixed_id(ns_prefix, src_id)],
                'label': ont_term.name,
                'name': ont_term.name,
                'description': ont_term.desc,
                'synonyms': [],
                'children': [],
                'entity_types': [],
            }
            if ont_term.name not in unique_names:
                unique_names[ont_term.name] = 1
            else:
                log.error(f'Duplicate name in GO: {ont_term.name}')

            namespace = ont_term.other['namespace']
            if 'entity_type' in ont_term.other:
                term['entity_types'].append(ont_term.other['entity_type'])
            elif 'biological_process' in namespace:
                term['entity_types'].append('BiologicalProcess')
            elif 'cellular_component' in namespace:
                term['entity_types'].append('Location')

            for syn in ont_term.synonyms:
                term['synonyms'].append(syn.desc)

            for c in ont_term.children:
                term['children'].append(c.id.replace('GO:', ''))

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({'term': term})))


def main():

    update_data_files()
    process_obo()


if __name__ == '__main__':
    # Setup logging
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging_conf_fn = config['bel_resources']['file_locations']['logging_conf_fn']
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}-terms')

    main()

