#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  do.py

"""

import sys
import os
import re
import tempfile
import json
import yaml
import datetime
import copy
import gzip
import pronto

import tools.utils.utils as utils
from tools.utils.Config import config

import tools.setup_logging
import structlog
log = structlog.getLogger(__name__)

# Globals
namespace_key = 'do'
namespace_def = utils.get_namespace(namespace_key, config)
ns_prefix = namespace_def['namespace']

url = "http://purl.obolibrary.org/obo/doid.obo"

# Local data filepath setup
basename = os.path.basename(url)

if not re.search('\.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
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


def process_obo(force: bool = False):

    # Terminology JSONL output filename
    data_fp = config["bel_resources"]["file_locations"]["data"]
    terms_fp = f'{data_fp}/namespaces/{namespace_key}.jsonl.gz'

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.info('Will not rebuild data file as it is newer than downloaded source file')
            return False

    with gzip.open(local_data_fp, 'r') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        ont = pronto.Ontology(fi)
        unique_names = {}

        for ont_term in ont:
            if 'is_obsolete' in ont_term.other:
                continue

            src_id = ont_term.id.replace('DOID:', '')
            term = {
                'namespace': ns_prefix,
                'namespace_value': ont_term.name,
                'src_id': src_id,
                'id': utils.get_prefixed_id(ns_prefix, ont_term.name),
                'alt_ids': [utils.get_prefixed_id(ns_prefix, src_id)],
                'label': ont_term.name,
                'name': ont_term.name,
                'description': ont_term.desc,
                'synonyms': [],
                'children': [],
                'annotation_types': ['Disease'],
                'equivalences': [],
            }
            if ont_term.name not in unique_names:
                unique_names[ont_term.name] = 1
            else:
                log.error(f'Duplicate name in DO: {ont_term.name}')

            for syn in ont_term.synonyms:
                term['synonyms'].append(syn.desc)

            for c in ont_term.children:
                term['children'].append(c.id.replace('DOID:', ''))

            if 'xref' in ont_term.other:
                for xref in ont_term.other['xref']:
                    if re.match('MESH:', xref):
                        term['equivalences'].append(xref)

            # Add term to JSONL
            fo.write("{}\n".format(json.dumps({'term': term})))


def main():

    update_data_files()
    process_obo()


if __name__ == '__main__':
    main()
