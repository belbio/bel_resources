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
import logging
import logging.config

import tools.utils.utils as utils
from tools.utils.Config import config

# Globals ###################################################################
namespace_key = 'chebi'  # namespace key into namespace definitions file
namespace_def = utils.get_namespace(namespace_key)
ns_prefix = namespace_def['namespace']

# FTP options
server = 'ftp.ebi.ac.uk'
source_data_fp = '/pub/databases/chebi/ontology/chebi.obo.gz'

# Local data filepath setup
basename = os.path.basename(source_data_fp)
gzip_flag = False
if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
    gzip_flag = True
    basename = f'{basename}.gz'
local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{basename}'


def get_metadata():
    # Setup metadata info - mostly captured from namespace definition file which
    # can be overridden in belbio_conf.yaml file
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

    # Get ftp file - but not if local downloaded file is newer
    result = utils.get_ftp_file(server, source_data_fp, local_data_fp, gzip_flag=gzip_flag, days_old=update_cycle_days)


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
        unique_names = {}

        for ont_term in ont:

            # Skip 1_STAR and deleted entries - 1_STAR - are user submitted
            #   2_STAR are automatically submitted by an organization,
            #   3_STAR are checked by CHEBI curators

            subset = ont_term.other.get('subset', ['null'])[0]

            if subset not in ['2_STAR', '3_STAR']:
                continue

            name_id = utils.get_prefixed_id(ns_prefix, ont_term.name)

            term = {
                'namespace': ns_prefix,
                'src_id': ont_term.id,
                'id': name_id,
                'alt_ids': [ont_term.id],
                'label': ont_term.name,
                'name': ont_term.name,
                'description': ont_term.desc,
                'synonyms': [],
                'entity_types': ['Abundance'],
                'equivalences': [],
            }

            # Some chebi names are over 500 chars long
            if len(name_id) > 80:
                term['id'] = ont_term.id
                term['alt_ids'] = [name_id]

            if ont_term.name not in unique_names:
                unique_names[ont_term.name] = 1
            else:
                log.error(f'Duplicate name in CHEBI: {ont_term.name}')

            for syn in ont_term.synonyms:
                term['synonyms'].append(syn.desc)

            for pv in ont_term.other.get('property_value', []):
                match = re.search('inchikey\s+\"([A-Z\-]+)\"', pv)
                if match:
                    inchi_key = match.group(1)
                    term['equivalences'].append(f'INCHIKEY:{inchi_key}')

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
