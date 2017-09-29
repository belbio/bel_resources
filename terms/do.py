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
import logging
import logging.config

# Import local util module
sys.path.append("..")
import utils

# Globals
prefix = 'do'
namespace = utils.get_namespace(prefix)
ns_prefix = namespace['namespace']

terms_fp = f'../data/terms/{prefix}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

# http://www.informatics.jax.org/downloads/reports/MRK_List1.rpt (including withdrawn marker symbols)
# http://www.informatics.jax.org/downloads/reports/MRK_List2.rpt (excluding withdrawn marker symbols)
url = "http://purl.obolibrary.org/obo/doid.obo"
download_fp = f'../downloads/do_doid.obo.gz'

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


def process_obo():
    with gzip.open(download_fp, 'r') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({'metadata': terminology_metadata})))

        ont = pronto.Ontology(fi)
        unique_names = {}

        for ont_term in ont:
            if 'is_obsolete' in ont_term.other:
                continue

            src_id = ont_term.id.replace('DOID:', '')
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
                'annotation_type': ['Disease'],
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


def build_json(force: bool = False):
    """Build RGD namespace json load file

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

