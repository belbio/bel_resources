#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  hgnc.py

"""

import sys
import tempfile
import json
import datetime
import gzip
import logging

sys.path.append("..")
import utils

ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)8s %(name)s | %(message)s')
ch.setFormatter(formatter)

log = logging.getLogger('hgnc-orthology')
log.addHandler(ch)
log.setLevel(logging.WARNING)  # This toggles all the logging in your app

# Globals
hgnc_json = '../data/orthologs/hgnc.json.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
hgnc_orig = '../downloads/hgnc_complete_set.json'
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

orthologs = {
    "source": "HGNC",
    "src_url": "http://www.genenames.org",
    "description": "Human Gene Nomenclature Committee orthologies",
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

    server = 'ftp.ebi.ac.uk'
    remote_file = '/pub/databases/genenames/new/json/hgnc_complete_set.json'
    # filename = os.path.basename(rfile)

    result = utils.get_ftp_file(server, remote_file, hgnc_orig)

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
        if utils.file_newer(hgnc_json, hgnc_orig):
            return False

    with open(hgnc_orig, 'r') as f:
        hgnc_data = json.load(f)

    for doc in hgnc_data['response']['docs']:

        # Skip unused entries
        if doc['status'] != 'Approved':
            continue

        subj_id = f"HGNC:{doc['symbol']}"
        subj_tax_id = '9606'

        for obj_id in doc.get('mgd_id', []):
            orthologs['orthologies'].append(
                {
                    'subject': {'id': subj_id, 'tax_id': subj_tax_id},
                    'object': {'id': obj_id, 'tax_id': '10090'},
                }
            )

        for obj_id in doc.get('rgd_id', []):
            orthologs['orthologies'].append(
                {
                    'subject': {'id': subj_id, 'tax_id': subj_tax_id},
                    'object': {'id': obj_id, 'tax_id': '10116'},
                }
            )

    with gzip.open(hgnc_json, 'wt') as f:
        json.dump(orthologs, f, indent=4)


def main():
    build_json()
    quit()

    # Cannot detect changes as ftp server doesn't support it
    changed = update_data_files()

    if changed:
        build_json()


if __name__ == '__main__':
    main()
