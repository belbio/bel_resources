#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  chebi.py

"""

import sys
import re
import os
import tempfile
import json
import yaml
import pronto
import datetime
import copy
import gzip
from typing import List, Mapping, Any, Iterable

import tools.utils.utils as utils
from tools.utils.Config import config

import tools.setup_logging
import structlog
log = structlog.getLogger(__name__)

# Globals ###################################################################
namespace_key = 'chebi'  # namespace key into namespace definitions file
namespace_def = utils.get_namespace(namespace_key, config)
ns_prefix = namespace_def['namespace']

# FTP options
server = 'ftp.ebi.ac.uk'
source_data_fp = '/pub/databases/chebi/ontology/chebi.obo.gz'

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

    # Get ftp file - but not if local downloaded file is newer
    result = utils.get_ftp_file(server, source_data_fp, local_data_fp, days_old=update_cycle_days)
    return result


def process_obo(force: bool = False):

    # Terminology JSONL output filename
    data_fp = config["bel_resources"]["file_locations"]["data"]
    terms_fp = f'{data_fp}/namespaces/{namespace_key}.jsonl.gz'

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.info('Will not rebuild data file as it is newer than downloaded source file')
            return False

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
                    'entity_types': ['Abundance'],
                    'equivalences': [],
                    'alt_ids': [],
                }

            elif blank_match:
                # Add term to JSONL
                if term.get('id', None):
                    fo.write("{}\n".format(json.dumps({'term': term})))
                term = {}

            elif term and keyval_match:
                key = keyval_match.group(1)
                val = keyval_match.group(2)

                if key == 'id':
                    term['src_id'] = val

                elif key == 'name':
                    if val == 'albiglutide':
                        pass  # Duplicate is an obsolete record
                    elif val not in unique_names:
                        unique_names[val] = 1
                    else:
                        log.error(f'Duplicate name in CHEBI: {val}')

                    name_id = utils.get_prefixed_id(ns_prefix, val)
                    term['label'] = val
                    term['name'] = val
                    if len(name_id) > 80:
                        term['id'] = term['src_id']
                        term['alt_ids'].append(name_id)
                        term['namespace_value'] = term['src_id'].replace('CHEBI:', '')
                    else:
                        term['id'] = name_id
                        term['alt_ids'].append(term['src_id'])
                        term['namespace_value'] = val

                elif key == 'subset':
                    if val not in ['2_STAR', '3_STAR']:
                        term = {}
                        continue

                elif key == 'def':
                    term['description'] == val

                elif key == 'synonym':
                    matches = re.search('\"(.*?)\"', val)
                    if matches:
                        syn = matches.group(1)
                        term['synonyms'].append(syn)
                    else:
                        log.warning(f'Unmatched synonym: {val}')

                elif key == 'alt_id':
                    term['alt_ids'].append(val.strip())

                elif key == 'property_value':
                    matches = re.search('inchikey\s\"(.*?)\"', val)
                    if matches:
                        inchikey = matches.group(1)
                        term['equivalences'].append(f'INCHIKEY:{inchikey}')

                elif key == 'is_obsolete':
                    term = {}


def main():

    update_data_files()
    process_obo()


if __name__ == '__main__':
    main()
