#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  taxonomy.py

"""
import sys
import tarfile
import os
import gzip
import tempfile
import json
import yaml
import datetime
from typing import Mapping, List, Any

sys.path.append("..")
import utils

# Globals
taxonomy_json = '../data/taxonomy.json.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
taxonomy_orig = '../downloads/taxdump.tar.gz'
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

preferred_labels_fn = './taxonomy_labels.yaml'
with open(preferred_labels_fn, 'r') as f:
    preferred_labels = yaml.load(f, Loader=yaml.BaseLoader)  # Using BaseLoader to read keys as strings


def update_data_files() -> bool:
    """ Download data files if needed

    Args:

    Returns:
        bool: files updated = True, False if not
    """
    # Update data file
    server = 'ftp.ncbi.nih.gov'
    rfile = '/pub/taxonomy/taxdump.tar.gz'
    # filename = os.path.basename(rfile)

    result = utils.get_ftp_file(server, rfile, taxonomy_orig)

    changed = False
    if 'Downloaded' in result[1]:
        changed = True

    return changed


def build_json(force: bool = False):
    """Build taxonomy.json file

    Args:
        force (bool): build json result regardless of file mod dates
    Returns:
        None
    """

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(taxonomy_json, taxonomy_orig):
            return False

    taxonomy = {
        'src_url': 'https://www.ncbi.nlm.nih.gov/Taxonomy',
        'url_template': 'https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=Info&id=<ID>',
        'version': dt,
        'records': {}
    }
    # Extract to tmpdir
    # subprocess.call(f'/usr/bin/tar xvfz ../downloads/{filename} --directory {tmpdir}', shell=True)

    tar = tarfile.open(name=taxonomy_orig)
    tar.extractall(path=tmpdir.name, members=None, numeric_owner=False)
    tar.close()

    # print(f'Before nodes {datetime.datetime.now()}')
    with open(f'{tmpdir.name}/nodes.dmp', 'r') as f:
        for line in f:
            # line = line.rstrip('\t|\n')
            (tax_id, parent_tax_id, rank, embl_code, *rest) = line.split('\t|\t')
            taxonomy['records'][tax_id] = {
                'tax_id': tax_id,
                'parent_tax_id': parent_tax_id,
                'rank': rank,
                'scientific_name': '',
                'label': '',
                'names': [],
            }

            if embl_code:
                taxonomy['records'][tax_id]['embl_code'] = embl_code

    # print(f'Before names {datetime.datetime.now()}')
    with open(f'{tmpdir.name}/names.dmp', 'r') as f:
        for line in f:
            line = line.rstrip('\t|\n')
            (tax_id, name, unique_variant, name_type) = line.split('\t|\t')
            taxonomy['records'][tax_id]['names'].append({'name': name, 'type': name_type})
            if name_type == 'scientific name':
                taxonomy['records'][tax_id]['scientific_name'] = name
            elif name_type == 'genbank common name':
                taxonomy['records'][tax_id]['label'] = preferred_labels.get(tax_id, name)  # Override label if available

    # print(f'Before dump {datetime.datetime.now()}')
    with gzip.open(taxonomy_json, 'wt') as f:
        json.dump(taxonomy, f, indent=4)

    # print(f'After dump {datetime.datetime.now()}')


def main():
    changed = update_data_files()

    if changed:
        build_json()


if __name__ == '__main__':
    main()

