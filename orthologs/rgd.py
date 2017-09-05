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

module_fn = os.path.basename(__file__)
module_fn = module_fn.replace('.py', '')

# Setup logging
logging_conf_fn = '../logging-conf.yaml'
with open(logging_conf_fn, mode='r') as f:
    logging.config.dictConfig(yaml.load(f))
log = logging.getLogger(f'{module_fn}-orthologs')

# Import local util module
sys.path.append("..")
import utils

# Globals
prefix = 'rgd'
namespace = utils.get_namespace(prefix)

orthologs_fp = f'../data/orthologs/{prefix}.json.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = 'ftp.rgd.mcw.edu'
remote_file = '/pub/data_release/RGD_ORTHOLOGS.txt'
download_fp = '../downloads/rgd_RGD_ORTHOLOGS.txt.gz'

orthologs = {
    "source": namespace['namespace'],
    "src_url": namespace['src_url'],
    "description": namespace['description'] + ' orthologs',
    "version": dt,
    "orthologies": [],
}


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    # Update data file
    result = utils.get_ftp_file(server, remote_file, download_fp, gzipflag=True)

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

    with gzip.open(download_fp, 'rt') as f:
        for line in f:
            pass  # TODO

    with gzip.open(orthologs_fp, 'wt') as f:
        json.dump(orthologs, f, indent=4)


def main():

    update_data_files()
    build_json()


if __name__ == '__main__':
    main()
