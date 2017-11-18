#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  TEMPLATE.py

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
from typing import List, Mapping, Any, Iterable
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
namespace_key = 'REPLACEME'  # namespace key into namespace definitions file
namespace_def = utils.get_namespace(namespace_key)
ns_prefix = namespace_def['namespace']

# FTP options
server = 'REPLACEME'
source_data_fp = 'REPLACEME'  # may have multiple files to be downloaded
# Web file options
url = 'REPLACEME'  # may have multiple files to be downloaded

# Local data filepath setup
basename = os.path.basename(source_data_fp)
gzip_flag = False
if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
    gzip_flag = True
    basename = f'{basename}.gz'

# Pick one of the two following options
local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_{basename}'
# local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{basename}'  # if namespace_key already is prefixed to basename


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
    # Can override/hard-code update_cycle_days in each term collection file if desired

    # This is all customizable - but here are some of the most common options

    # Get ftp file - but not if local downloaded file is newer
    # result = utils.get_ftp_file(server, source_data_fp, local_data_fp, gzip_flag=gzip_flag, days_old=update_cycle_days)

    # Get web file - but not if local downloaded file is newer
    # result = utils.get_web_file(url, local_data_fp, gzip_flag=gzip_flag, days_old=update_cycle_days)


def build_json(force: bool = False):
    """Build term JSONL file"""

    # Terminology JSONL output filename
    terms_data = config["bel_resources"]["file_locations"]["terms_data"]
    terms_fp = f'{terms_data}/{namespace_key}.jsonl.gz'

    # used if you need a tmp dir to do some processing
    # tmpdir = tempfile.TemporaryDirectory()

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source files')
            return False

    with gzip.open(local_data_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        for row in fi:

            # review https://github.com/belbio/schemas/blob/master/schemas/terminology-0.1.0.yaml
            # for what should go in here
            term = {
                'namespace': ns_prefix,
                'src_id': '',
                'id': '',
                'alt_ids': [],
                'label': '',
                'name': '',
                'synonyms': copy.copy(list(set([]))),
                'entity_types': [],
                'equivalences': [],
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
