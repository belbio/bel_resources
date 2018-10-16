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

import tools.utils.utils as utils
from tools.utils.Config import config

import tools.setup_logging
import structlog
log = structlog.getLogger(__name__)

import tools.utils.utils as utils
from tools.utils.Config import config

# Globals
namespace_key = 'go'
namespace_def = utils.get_namespace(namespace_key, config)
ns_prefix = namespace_def['namespace']

url = 'http://purl.obolibrary.org/obo/go.obo'

complex_parent_id = 'GO:0032991'

# Local data filepath setup
basename = os.path.basename(url)

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

    (changed_flag, msg) = utils.get_web_file(url, local_data_fp, days_old=update_cycle_days)
    log.info(msg)

    return changed_flag


def check_parents(check_id, test_parent_id, parent_ids, debug: bool = False):
    """Check to see if parent_id is a parent of check_id"""

    if check_id in parent_ids:

        if debug:
            print('Check id', check_id)
        for parent_id in parent_ids[check_id]:
            result = check_parents(parent_id, test_parent_id, parent_ids)
            if debug:
                print('Parent ID', parent_id)
            if result is True:
                return result
            elif test_parent_id == parent_id:
                return True
            else:
                return False


def process_obo(force: bool = False):

    # Terminology JSONL output filename
    data_fp = config["bel_resources"]["file_locations"]["data"]
    terms_fp = f'{data_fp}/namespaces/{namespace_key}.jsonl.gz'

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.info('Will not rebuild data file as it is newer than downloaded source file')
            return False

    # collect parents/hierarchy
    parent_ids = {}
    with gzip.open(local_data_fp, 'rt') as fi:
        id_re = re.compile('id:\s+(\S+)\s*')
        isa_re = re.compile('is_a:\s+(\S+)\s')

        for line in fi:
            id_match = id_re.match(line)
            isa_match = isa_re.match(line)
            if id_match:
                goid = id_match.group(1)
            if isa_match:
                isa_id = isa_match.group(1)
                try:
                    parent_ids[goid][isa_id] = 1
                except Exception as e:
                    parent_ids[goid] = {}
                    parent_ids[goid][isa_id] = 1

    with gzip.open(local_data_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        term = {}

        keyval_regex = re.compile('(\w[\-\w]+)\:\s(.*?)\s*$')
        term_regex = re.compile('\[Term\]')
        blankline_regex = re.compile('\s*$')

        unique_names = {}

        for line in fi:
            term_match = term_regex.match(line)
            blank_match = blankline_regex.match(line)
            keyval_match = keyval_regex.match(line)
            if term_match:
                term = {
                    'namespace': ns_prefix,
                    'namespace_value': '',
                    'src_id': '',
                    'id': '',
                    'label': '',
                    'name': '',
                    'description': '',
                    'synonyms': [],
                    'entity_types': [],
                    'equivalences': [],
                    'parents': [],
                    'alt_ids': [],
                }

            elif blank_match:
                # Add term to JSONL
                if term.get('obsolete', False):
                    pass  # Skip obsolete terms
                elif term.get('id', None):
                    fo.write("{}\n".format(json.dumps({'term': term})))

                    if term['name'] not in unique_names:
                        unique_names[term['name']] = 1
                    else:
                        log.error(f'Duplicate name in GO: {term["name"]}')

                term = {}

            elif term and keyval_match:
                key = keyval_match.group(1)
                val = keyval_match.group(2)

                if key == 'id':
                    term['src_id'] = val

                    if term['src_id'] == 'GO:0071753':
                        print('Checking ID')
                        debug = True
                    else:
                        debug = False

                    if check_parents(term['src_id'], complex_parent_id, parent_ids, debug):
                        term['entity_types'].append('Complex')

                elif key == 'name':
                    name_id = utils.get_prefixed_id(ns_prefix, val)
                    term['label'] = val
                    term['name'] = val
                    if len(name_id) > 80:
                        term['id'] = term['src_id'].replace('DOID', 'DO')
                        term['alt_ids'].append(name_id)
                        term['namespace_value'] = term['src_id'].replace('DO:', '')
                    else:
                        term['id'] = name_id
                        term['alt_ids'].append(term['src_id'].replace('DOID', 'DO'))
                        term['namespace_value'] = val

                elif key == 'is_obsolete':
                    # print('Obsolete', term['alt_ids'])
                    term['obsolete'] = True

                elif key == 'def':
                    term['description'] = val

                elif key == 'synonym':
                    matches = re.search('\"(.*?)\"', val)
                    if matches:
                        syn = matches.group(1).strip()
                        term['synonyms'].append(syn)
                    else:
                        log.warning(f'Unmatched synonym: {val}')

                elif key == 'namespace':
                    if 'biological_process' == val:
                        term['entity_types'].append('BiologicalProcess')
                    elif 'cellular_component' == val:
                        term['entity_types'].append('Location')
                    elif 'molecular_function' == val:
                        term['entity_types'].append('Activity')

                elif key == 'alt_id':
                    term['alt_ids'].append(val)

                elif key == 'is_a':
                    matches = re.match('(GO:\d+)\s', val)
                    if matches:
                        parent_id = matches.group(1)
                        term['parents'].append(f'{parent_id}')


def main():

    update_data_files()
    process_obo()


if __name__ == '__main__':
    main()

