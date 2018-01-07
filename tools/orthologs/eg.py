#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  hgnc.py

"""

import tempfile
import os
import re
import json
import datetime
import gzip
import logging
import logging.config

from tools.utils.Config import config
import tools.utils.utils as utils

# Globals
prefix = 'eg'
ns_prefix = prefix.upper()
namespace = utils.get_namespace(prefix, config)

species_namespace = utils.get_namespace('tax', config)
tax_ns_prefix = species_namespace['namespace']

data_fp = config["bel_resources"]["file_locations"]["data"]
orthologs_fp = f'{data_fp}/orthologs/{prefix}.jsonl.gz'

tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = 'ftp.ncbi.nlm.nih.gov'
remote_file = '/gene/DATA/gene_group.gz'

orthologs_metadata = {
    "source": namespace['namespace'],
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
    # Setup logging
    global log
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging.config.dictConfig(config['logging'])
    log = logging.getLogger(f'{module_fn}-orthologs')

    main()

