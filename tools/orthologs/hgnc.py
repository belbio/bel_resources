#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  hgnc.py

"""

import sys
import tempfile
import os
import json
import yaml
import datetime
import gzip
import logging
import logging.config

from bel_lang.Config import config

# Import local util module
sys.path.append("..")
import utils

# Globals
prefix = 'hgnc'
ns_prefix = prefix.upper()
namespace = utils.get_namespace(prefix)

orthologs_fp = f'../data/orthologs/{prefix}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = 'ftp.ebi.ac.uk'
remote_file = '/pub/databases/genenames/new/json/hgnc_complete_set.json'
download_fp = '../downloads/hgnc_complete_set.json.gz'

orthologs_metadata = {
    "source": namespace['namespace'],
    "src_url": namespace['src_url'],
    "description": namespace['description'] + ' orthologs',
    "version": dt,
}


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    # Update data file
    result = utils.get_ftp_file(server, remote_file, download_fp, gzip_flag=True)

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
        if utils.file_newer(orthologs_fp, download_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source file')
            return False

    with gzip.open(download_fp, 'rt') as fi, gzip.open(orthologs_fp, 'wt') as fo:

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

    logging_conf_fn = f'{config["bel_resources"]["file_locations"]["root"]}/logging_conf.yml'
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}-orthologs')

    main()
