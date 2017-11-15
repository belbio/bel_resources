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
import gzip
import pronto
import logging
import logging.config


# TODO: should molecular function branch be tagged as entity_type: Activity?

# Import local util module
sys.path.append("..")
import utils

# Globals
prefix = 'go'
namespace = utils.get_namespace(prefix)
ns_prefix = namespace['namespace']

terms_fp = f'../data/terms/{prefix}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

url = 'http://purl.obolibrary.org/obo/go.obo'
download_fp = f'../downloads/go.obo.gz'


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

    (changed, response) = utils.get_web_file(url, download_fp, gzipflag=True)
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


def process_obo():

    with gzip.open(download_fp, 'r') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({'metadata': terminology_metadata})))

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


def build_json(force: bool = False):
    """Build GO namespace json load file

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

    process_obo()


def main():

    # Setup logging
    global log
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging_conf_fn = '../logging-conf.yaml'
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}-terms')

    # Cannot detect changes as ftp server doesn't support MLSD cmd
    update_data_files()
    build_json()


if __name__ == '__main__':
    main()



