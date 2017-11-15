#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  mesh.py

"""

import sys
import re
import os
import tempfile
import json
import yaml
import datetime
import gzip
import logging
import logging.config

# Import local util module
sys.path.append("..")
import utils

# Globals
prefix = 'mesh'
namespace = utils.get_namespace(prefix)
ns_prefix = namespace['namespace']

terms_fp = f'../data/terms/{prefix}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()
year = datetime.date.today().year

# TODO - figure out how to add the children attributes - complicated by the fact that only
#        some terms are used and not others and terms have multiple locations on DAG
#        An approach would be to process this after the MESH hierarchy is created.
#        Need to add the tree numbers as a dictionary split by the decimal point (use nested_dict python module for this)

# Tree example in MESH Browser ('MeSH Tree Structures' tab)
# ftp://nlmpubs.nlm.nih.gov/online/mesh/MESH_FILES/meshtrees/mtrees2017.bin

server = "nlmpubs.nlm.nih.gov"
remote_file = f'/online/mesh/MESH_FILES/asciimesh/d{year}.bin'
remote_file_concepts = f'/online/mesh/MESH_FILES/asciimesh/c{year}.bin'
download_fp = f'../downloads/mesh_d{year}.bin.gz'
download_concepts_fp = f'../downloads/mesh_c{year}.bin.gz'

terminology_metadata = {
    "name": namespace['namespace'],
    "namespace": namespace['namespace'],
    "description": namespace['description'],
    "version": dt,
    "src_url": namespace['src_url'],
    "url_template": namespace['template_url'],
}

chemicals_ST = ('T116', 'T195', 'T123', 'T122', 'T118', 'T103', 'T120',
             'T104', 'T200', 'T111', 'T196', 'T126', 'T131', 'T125',
             'T129', 'T130', 'T197', 'T119', 'T124', 'T114', 'T109',
             'T115', 'T121', 'T192', 'T110', 'T127',)


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    result_concepts = utils.get_ftp_file(server, remote_file_concepts, download_concepts_fp, gzipflag=True)
    result = utils.get_ftp_file(server, remote_file, download_fp, gzipflag=True)

    changed = False
    if 'Downloaded' in result[1] or 'Downloaded' in result_concepts[1]:
        changed = True

    return changed


def process_types(mesh_id, mns, sts):

    global chemicals_ST
    entity_types = []
    context_types = []

    if mns:  # Description records
        for mn in mns:
            if re.match('A', mn) and not re.match('A11', mn):
                context_types.append('Anatomy')
            if re.match('A11', mn) and not re.match('A11.284', mn):
                context_types.append('Cell')
            if re.match('A11.284', mn):
                entity_types.append('Location')
                context_types.append('CellStructure')
            if re.match('C|F', mn):  # Original OpenBEL was C|F03 - Charles Hoyt suggested C|F
                context_types.append('Disease')
            if re.match('G', mn) and not re.match('G01|G15|G17', mn):
                entity_types.append('BiologicalProcess')
            if re.match('D', mn):
                entity_types.append('Abundance')

    elif sts:  # Concepts
        flag = 0
        for st in sts:
            if st in chemicals_ST:
                flag = 1
                break
        if flag:
            entity_types.append('Abundance')

    return (entity_types, context_types)


def process_synonyms(syns):

    new_syns = set()
    for syn in syns:
        match = re.match('(.*?)\|.*(\|EQV\|)?', syn)
        if match and match.group(2):
            new_syns.add(match.group(1))
        elif not match:
            new_syns.add(syn)

    return list(new_syns)


def build_json(force: bool = False):
    """Build MESH namespace json load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, download_fp):
            log.warning('Will not rebuild data file as it is newer than downloaded source file')
            return False

    with gzip.open(download_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({'metadata': terminology_metadata})))

        mesh_id, mns, sts, mh, desc, entity_types, context_types, syns = None, [], [], None, None, [], [], []
        for line in fi:
            if re.match('^\s*$', line):
                (entity_types, context_types) = process_types(mesh_id, mns, sts)
                syns = process_synonyms(syns)
                term = {
                    'namespace': ns_prefix,
                    'src_id': mesh_id,
                    'id': utils.get_prefixed_id(ns_prefix, mh),
                    'alt_ids': [utils.get_prefixed_id(ns_prefix, mesh_id)],
                    'label': mh,
                    'name': mh,
                    'description': desc,
                    'entity_types': copy.copy(entity_types),
                    'context_types': copy.copy(context_types),
                    'synonyms': copy.copy(syns),
                }

                # only save terms that have x_types
                if entity_types or context_types:
                    # Add term to JSONL
                    fo.write("{}\n".format(json.dumps({'term': term})))

                mesh_id, mns, sts, mh, desc, entity_types, context_types, syns = None, [], [], None, None, [], [], []
                continue

            match = re.match('^MH\s=\s(.*?)\s*$', line)
            if match:
                mh = match.group(1)
                continue

            match = re.match('^MN\s=\s((\w).*?)\s*$', line)
            if match:
                mns.append(match.group(1))
                continue

            match = re.match('^UI\s=\s(.*?)\s*$', line)
            if match:
                mesh_id = match.group(1)
                continue

            match = re.match('^MS\s=\s(.*?)\s*$', line)
            if match:
                desc = match.group(1)
                continue

            match = re.match('^(ENTRY|PRINT ENTRY|SY)\s=\s(.*?)\s*$', line)
            if match:
                syns.append(match.group(2))
                continue

            match = re.match('^ST\s=\s(\w+)\s*$', line)
            if match:
                sts.append(match.group(1))
                continue


def main():

    # Setup logging
    global log
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging_conf_fn = '../logging-conf.yaml'
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}-terms')

    # Cannot detect changes as ftp server doesn't support MLSD cmd
    update_data_files()
    build_json()


if __name__ == '__main__':
    main()



