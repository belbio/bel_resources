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
prefix = 'eg'
ns_prefix = prefix.upper()
namespace = utils.get_namespace(prefix)

species_namespace = utils.get_namespace('tax')
tax_ns_prefix = species_namespace['namespace']

orthologs_fp = f'../data/orthologs/{prefix}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = 'ftp.ncbi.nlm.nih.gov'
remote_file = '/gene/DATA/gene_group.gz'
download_fp = '../downloads/eg_gene_group.gz'

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
    result = utils.get_ftp_file(server, remote_file, download_fp, gzip_flag=False)

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
        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({'metadata': orthologs_metadata})))

        fi.__next__()  # skip header line

        for line in fi:
            (subj_tax_id, subj_gene_id, relationship, obj_tax_id, obj_gene_id) = line.rstrip().split('\t')
            if relationship != 'Ortholog':
                continue

            ortholog = {
                'subject': {'id': f'{ns_prefix}:{subj_gene_id}', 'tax_id': f'{tax_ns_prefix}:{subj_tax_id}'},
                'object': {'id': f'{ns_prefix}:{obj_gene_id}', 'tax_id': f'{tax_ns_prefix}:{obj_tax_id}'},
            }

            # Add ortholog to JSONL
            fo.write("{}\n".format(json.dumps({'ortholog': ortholog})))


def main():

    # Cannot detect changes as ftp server doesn't support MLSD cmd
    update_data_files()
    build_json()


if __name__ == '__main__':
    main()

