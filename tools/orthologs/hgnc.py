#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  hgnc.py

"""

import tempfile
import os
import re
import json
import yaml
import datetime
import gzip
import logging
import logging.config

import tools.utils.utils as utils
from tools.utils.Config import config

# Globals
prefix = 'hgnc'
ns_prefix = prefix.upper()
namespace = utils.get_namespace(prefix, config)

data_fp = config["bel_resources"]["file_locations"]["data"]
orthologs_fp = f'{data_fp}/orthologs/{prefix}.jsonl.gz'

tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = 'ftp.ebi.ac.uk'
remote_file = '/pub/databases/genenames/new/json/hgnc_complete_set.json'

orthologs_metadata = {
    "source": namespace['namespace'],
    "type": "ortholog",
    "src_url": namespace['src_url'],
    "description": namespace['description'] + ' orthologs',
    "version": dt,
}

# Local data filepath setup
basename = os.path.basename(remote_file)

if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
    basename = f'{basename}.gz'

local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{basename}'


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    # Update data file
    result = utils.get_ftp_file(server, remote_file, local_data_fp)

    changed = False
    if 'Downloaded' in result[1]:
        changed = True

    return changed


def build_json(force: bool = False):
    """Build HGNC namespace json load file

    Args:
        force (bool): build json result regardless of file mod dates
    Returns:
        None
    """

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(orthologs_fp, local_data_fp):
            log.info('Will not rebuild data file as it is newer than downloaded source file')
            return False

    with gzip.open(local_data_fp, 'rt') as fi, gzip.open(orthologs_fp, 'wt') as fo:

        fo.write("{}\n".format(json.dumps({'metadata': orthologs_metadata})))

        data = json.load(fi)

        hgnc_tax_id = 'TAX:9606'
        mgi_tax_id = 'TAX:10090'
        rgd_tax_id = 'TAX:10116'
        for doc in data['response']['docs']:

            # Skip unused entries
            if doc['status'] != 'Approved':
                continue

            subj_id = f"HGNC:{doc['symbol']}"

            for obj_id in doc.get('mgd_id', []):
                ortholog = {
                    'subject': {'id': subj_id, 'tax_id': hgnc_tax_id},
                    'object': {'id': obj_id, 'tax_id': mgi_tax_id},
                }

                # Add ortholog to JSONL
                fo.write("{}\n".format(json.dumps({'ortholog': ortholog})))

            for obj_id in doc.get('rgd_id', []):
                ortholog = {
                    'subject': {'id': subj_id, 'tax_id': hgnc_tax_id},
                    'object': {'id': obj_id, 'tax_id': rgd_tax_id},
                }

                # Add ortholog to JSONL
                fo.write("{}\n".format(json.dumps({'ortholog': ortholog})))


def main():

    update_data_files()
    build_json()


if __name__ == '__main__':
    # Setup logging
    global log
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging.config.dictConfig(config['logging'])
    log = logging.getLogger(f'{module_fn}-orthologs')

    main()
