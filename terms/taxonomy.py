#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  taxonomy.py

This utility will download new versions of the taxonomy file
into the ../downloads directory.  It will read in taxonomy species
label overrides from the ./taxonomy_labels.yaml file and then
create a ../data/terms/taxonomy.jsonl.gz file that will act as the load
file for taxonomy data into Elasticsearch and ArangoDB
"""

import sys
import tarfile
import gzip
import os
import tempfile
import json
import yaml
import datetime
import logging
import logging.config

# Import local util module
sys.path.append("..")
import utils

# Globals
prefix = 'tax'
ns_prefix = prefix.upper()
namespace = utils.get_namespace(prefix)

taxonomy_fp = '../data/terms/taxonomy.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)

taxonomy_orig = '../downloads/taxdump.tar.gz'
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

preferred_labels_fn = './taxonomy_labels.yaml'
with open(preferred_labels_fn, 'r') as f:
    preferred_labels = yaml.load(f, Loader=yaml.BaseLoader)  # Using BaseLoader to read keys as strings

taxonomy_metadata = {
    "name": namespace['namespace'],
    "namespace": namespace['namespace'],
    "description": namespace['description'],
    'version': dt,
    "src_url": namespace['src_url'],
    "url_template": namespace['template_url'],
}


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
    log.debug('After update data files')

    changed = False
    if 'Downloaded' in result[1]:
        changed = True

    return changed


def build_taxonomy_tree(taxonomy):
    """Create taxonomy tree and export as separate file

    Separate file will be loaded by each of the protein term build scripts to
    use for their taxonomy_tree field.
    """

    # TODO

    return taxonomy


def build_json(force: bool = False):
    """Build taxonomy.json file

    Args:
        force (bool): build json result regardless of file mod dates
    Returns:
        None
    """

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(taxonomy_fp, taxonomy_orig):
            log.warning('Will not rebuild data file as it is newer than downloaded source file')
            return False

    # Extract to tmpdir
    # subprocess.call(f'/usr/bin/tar xvfz ../downloads/{filename} --directory {tmpdir}', shell=True)

    tar = tarfile.open(name=taxonomy_orig)
    tar.extractall(path=tmpdir.name, members=None, numeric_owner=False)
    tar.close()

    taxonomy = {}

    log.debug('Before nodes.dmp')
    # print(f'Before nodes {datetime.datetime.now()}')
    with open(f'{tmpdir.name}/nodes.dmp', 'r') as f:
        log.debug('Processing nodes.dmp file')
        for line in f:
            # line = line.rstrip('\t|\n')
            (tax_id, parent_id, rank, embl_code, *rest) = line.split('\t|\t')

            taxonomy[tax_id] = {
                'namespace': ns_prefix,
                'src_id': tax_id,
                'id': f'{ns_prefix}:{tax_id}',
                'parent_id': f'{ns_prefix}:{parent_id}',
                'children': [],
                'taxonomy_rank': rank,
                'name': '',
                'label': '',
                'synonyms': [],
                'taxonomy_names': [],
                'context_types': [],
            }

            if rank == 'species':
                taxonomy[tax_id]['context_types'].append('Species')

            if embl_code:
                taxonomy[tax_id]['embl_code'] = embl_code

    # Flip the hierarchy data structure
    for tax_id in taxonomy:
        if taxonomy[tax_id]['parent_id']:
            parent_id = taxonomy[tax_id]['parent_id'].replace('TAX:', '')
            if tax_id != '1':  # Skip root node so we don't make root a child of root
                taxonomy[parent_id]['children'].append(f'TAX:{tax_id}')

        del taxonomy[tax_id]['parent_id']

    taxonomy = build_taxonomy_tree(taxonomy)

    # Recursively create species_tree
    # print(f'Before names {datetime.datetime.now()}')
    log.debug('Before names.dmp')
    with open(f'{tmpdir.name}/names.dmp', 'r') as fi:
        log.debug('Processing names.dmp')
        for line in fi:
            line = line.rstrip('\t|\n')
            (tax_id, name, unique_variant, name_type) = line.split('\t|\t')

            if name not in taxonomy[tax_id]['synonyms']:
                taxonomy[tax_id]['synonyms'].append(name)

            taxonomy[tax_id]['taxonomy_names'].append({'name': name, 'type': name_type})

            if name_type == 'genbank common name':
                taxonomy[tax_id]['label'] = preferred_labels.get(tax_id, name)  # Override label if available

            elif name_type == 'scientific name':
                taxonomy[tax_id]['name'] = name
                if not taxonomy[tax_id]['label']:
                    taxonomy[tax_id]['label'] = name

    with gzip.open(taxonomy_fp, 'wt') as fo:

        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({'metadata': taxonomy_metadata})))

        for tax_id in taxonomy:
            # Add taxonomy record to JSONL
            fo.write("{}\n".format(json.dumps({'term': taxonomy[tax_id]})))


def main():

    update_data_files()
    build_json()


if __name__ == '__main__':

    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    # Setup logging
    logging_conf_fn = '../logging-conf.yaml'
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
    log = logging.getLogger(f'{module_fn}')

    main()

