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

import tools.utils.utils as utils
from tools.utils.Config import config

# Globals
namespace_key = 'sp'
namespace_def = utils.get_namespace(namespace_key)
ns_prefix = namespace_def['namespace']

terms_fp = f'../data/terms/{namespace_key}.jsonl.gz'
tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

# file documentation:  http://web.expasy.org/docs/userman.html
# 500Mb ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz
# 50Gb ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_trembl.dat.gz

server = 'ftp.uniprot.org'
source_data_fp = '/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz'

# Local data filepath setup
basename = os.path.basename(source_data_fp)
gzip_flag = False
if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
    gzip_flag = True
    basename = f'{basename}.gz'
local_data_fp = f'{config["bel_resources"]["file_locations"]["downloads"]}/{namespace_key}_{basename}'


def get_metadata():
    # Setup metadata info - mostly captured from namespace definition file which
    # can be overridden in belbio_conf.yaml file
    dt = datetime.datetime.now().replace(microsecond=0).isoformat()
    metadata = {
        "name": namespace_def['namespace'],
        "namespace": namespace_def['namespace'],
        "description": namespace_def['description'],
        "version": dt,
        "src_url": namespace_def['src_url'],
        "url_template": namespace_def['template_url'],
    }

    return metadata


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    update_cycle_days = config['bel_resources']['update_cycle_days']

    result = utils.get_ftp_file(server, source_data_fp, local_data_fp, gzip_flag=gzip_flag, days_old=update_cycle_days)

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

    terms_data = config["bel_resources"]["file_locations"]["terms_data"]
    species_labels_fn = f'{terms_data}/tax_labels.json.gz'
    with gzip.open(species_labels_fn, 'r') as fi:
        species_label = json.load(fi)

    for line in record:

        # Get ID
        match = re.match('^ID\s+(\w+);?', line)
        if match:
            sp_id = match.group(1)

        # Get accessions
        if re.match('^AC', line):
            ac_line = re.sub('^AC\s+', '', line).rstrip()
            ac_line = re.sub(';$', '', ac_line)
            ac_line = re.sub(';\s+', ';', ac_line)
            accessions.extend(ac_line.split(';'))

        # Get Taxonomy ID
        match = re.match('^OX\s+NCBI_TaxID=(\d+)', line)
        if match:
            tax_src_id = match.group(1)
            tax_id = f'TAX:{tax_src_id}'

        # Get Equivalences
        match = re.match('^DR\s+(\w+);\s(\w+);\s([\w\-]+)\.', line)
        if match:
            (db, db_id, extra) = match.group(1, 2, 3)
            if db == 'HGNC':
                equivalences.append(f'{db}:{extra}')
                # print(equivalences)
            elif db == 'MGI':
                equivalences.append(f'{db}:{extra}')
                # print(equivalences)
            if db == 'RGD':
                equivalences.append(f'{db}:{extra}')
                # print(equivalences)
            elif db == 'GeneID':
                equivalences.append(f'EG:{db_id}')
                # print(equivalences)

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
        'species_id': tax_id,
        'species_label': species_label.get(tax_id, None),
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

    # Terminology JSONL output filename
    terms_data = config["bel_resources"]["file_locations"]["terms_data"]
    terms_fp = f'{terms_data}/{namespace_key}.jsonl.gz'

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(terms_fp, local_data_fp):
            return False

    with gzip.open(local_data_fp, 'rt') as fi, gzip.open(terms_fp, 'wt') as fo:

        # Header JSONL record for terminology
        metadata = get_metadata()
        fo.write("{}\n".format(json.dumps({'metadata': metadata})))

        record = []
        for line in fi:
            record.append(line)
            if re.match('^//', line):
                term = process_record(record)
                fo.write("{}\n".format(json.dumps({'term': term})))
                record = []


def main():

    update_data_files()
    build_json()


if __name__ == '__main__':
    # Setup logging
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging_conf_fn = config['bel_resources']['file_locations']['logging_conf_fn']
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}-terms')

    main()

