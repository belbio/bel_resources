#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  swissprot.py

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
from typing import List, Mapping, Any
import logging
import logging.config

# Import local util module
sys.path.append("..")
import utils

# Globals
prefix = 'sp'
namespace = utils.get_namespace(prefix)
ns_prefix = namespace['namespace']

terms_fp = f'../data/terms/{prefix}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

# file documentation:  http://web.expasy.org/docs/userman.html
# 500Mb ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz
# 50Gb ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_trembl.dat.gz

server = 'ftp.uniprot.org'
remote_file = '/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz'
download_fp = f'../downloads/sp_uniprot_sprot.dat.gz'

terminology_metadata = {
    "name": namespace['namespace'],
    "namespace": namespace['namespace'],
    "description": namespace['description'],
    "version": dt,
    "src_url": namespace['src_url'],
    "url_template": namespace['template_url'],
}


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    result = utils.get_ftp_file(server, remote_file, download_fp)

    changed = False
    if 'Downloaded' in result[1]:
        changed = True

    return changed


def process_record(record: List[str]) -> Mapping[str, Any]:
    """Process SwissProt Dat file record

    Args:
        record (List[str]): array of swissprot dat file for one protein

    Returns:
        Mapping[str, Any]: term record for namespace
    """

    equivalences = []
    accessions = []
    de = ''
    gn = ''
    for line in record:

        # Get ID
        match = re.match('^ID\s+(\w+)', line)
        if match:
            sp_id = match.group(1)

        # Get accessions
        if re.match('^AC', line):
            ac_line = re.sub('^AC\s+', '', line).rstrip()
            accessions.extend(ac_line.split('; '))

        # Get Taxonomy ID
        match = re.match('^OX\s+NCBI_TaxID=(\d+)', line)
        if match:
            tax_id = match.group(1)

        # Get Gene name
        match = re.match('^GN\s+Name=(\d+)', line)
        if match:
            tax_id = match.group(1)

        # Get Equivalences
        match = re.match('^DR\s+(\w+);\s(\w+);\s([\w\-]+)\.', line)
        if match:
            (db, db_id, extra) = match.group(1, 2, 3)
            if db == 'HGNC':
                equivalences.append(f'{db}:{extra}')
                print(equivalences)
            elif db == 'MGI':
                equivalences.append(f'{db}:{extra}')
                print(equivalences)
            if db == 'RGD':
                equivalences.append(f'{db}:{extra}')
                print(equivalences)
            elif db == 'GeneID':
                equivalences.append(f'EG:{db_id}')
                print(equivalences)

        if re.match('^DE\s+', line):
            de += line.replace('DE', '').strip()
        if re.match('^GN\s+', line):
            gn += line.replace('GN', '').strip()

    synonyms = []
    name = None
    full_name = None
    # GN - gene names processing
    log.debug(f'AC: {accessions[0]}')
    log.debug(f'GN {gn}')
    gn = re.sub(' {.*?}', '', gn, flags=re.S)
    match = re.search('Name=(.*?)[;{]+', gn)
    if match:
        name = match.group(1)
        log.debug(f'Gene_name {name}')
    match = re.search('Synonyms=(.*?);', gn)
    if match:
        syns = match.group(1)
        synonyms.extend(syns.split(', '))
        log.debug(f'Syns: {synonyms}')

    match = re.search('ORFNames=(.*?);', gn)
    if match:
        syns = match.group(1)
        orfnames = syns.split(', ')
        synonyms.extend(orfnames)
        log.debug(f'Syns: {synonyms}')
        if not name:
            name = orfnames[0]

    # DE - name processing
    log.debug(f'DE {de}')
    de = re.sub(' {.*?}', '', de, flags=re.S)
    match = re.search('RecName:(.*?;)\s*(\w+:)?', de)
    if match:
        recname_grp = match.group(1)
        match_list = re.findall('\s*(\w+)=(.*?);', recname_grp)
        for key, val in match_list:
            if key == 'Full':
                full_name = val
            if not name and key == 'Short':
                name = val

            log.debug(f'DE RecName Key: {key}  Val: {val}')
        if not name and full_name:  # Use long name for protein name if all else fails
            name = full_name

    match = re.search('AltName:(.*?;)\s*\w+:', de, flags=0)
    if match:
        altname_grp = match.group(1)
        match_list = re.findall('\s*(\w+)=(.*?);', altname_grp)
        for key, val in match_list:
            if key in ['Full', 'Short']:
                synonyms.append(val)
            log.debug(f'DE AltName Key: {key}  Val: {val}')

    if not name:
        name = accessions[0]

    if not full_name:
        full_name = "Not available"

    term = {
        'namespace': ns_prefix,
        'src_id': accessions[0],
        'id': utils.get_prefixed_id(ns_prefix, accessions[0]),
        'label': name,
        'name': name,
        'description': full_name,
        'species': f'TAX:{tax_id}',
        'entity_types': ['Gene', 'RNA', 'Protein'],
        'synonyms': copy.copy(synonyms),
        'equivalences': copy.copy(equivalences),
    }

    alt_ids = []
    for alt_id in accessions[1:] + [sp_id]:
        alt_ids.append(utils.get_prefixed_id(ns_prefix, alt_id))

    term['alt_ids'] = copy.copy(alt_ids)

    log.debug('Term:\n', json.dumps(term, indent=4))
    return term


def build_json(force: bool = False):
    """Build Swissprot namespace jsonl load file

    Args:
        force (bool): build json result regardless of file mod dates

    Returns:
        None
    """

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, download_fp):
            return False

    with gzip.open(download_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:
        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({'metadata': terminology_metadata})))

        record = []
        for line in fi:
            record.append(line)
            if re.match('^//', line):
                term = process_record(record)
                fo.write("{}\n".format(json.dumps({'term': term})))
                record = []


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

