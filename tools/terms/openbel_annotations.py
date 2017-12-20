#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  program.py <customer>

"""
import sys
import os.path
import gzip
import copy
import json
import yaml
import re

import logging
import logging.config

import tools.utils.utils as utils
from tools.utils.Config import config


def update_data_files() -> bool:

    annofiles = []

    for key in config["bel_resources"]["openbel"]["openbel_annotation_sources"]:
        url = config["bel_resources"]["openbel"]["openbel_annotation_sources"][key]
        basename = os.path.basename(url)
        gzip_flag = False
        if not re.search('.gz$', basename):  # we basically gzip everything retrieved that isn't already gzipped
            gzip_flag = True
            basename = f'{basename}.gz'
        local_fn = f'{config["bel_resources"]["file_locations"]["downloads"]}/{basename}'

        annofiles.append((local_fn, url))
        update_cycle_days = config['bel_resources']['update_cycle_days']
        utils.get_web_file(url, local_fn, gzip_flag=gzip_flag, days_old=update_cycle_days)

    return annofiles


def add_metadata():

    metadata = {
        "Evidence and Conclusion Ontology": {
            "namespace": 'ECO',
        },
        "Cell Line Ontology (CLO)": {
            "namespace": "CLO"
        },
        "Experimental Factor Ontology (EFO)": {
            "namespace": "EFO"
        },
        "Uberon": {
            "namespace": "UBERON",
        },
        "Cell Ontology (CL)": {
            "namespace": "CL"
        }
    }

    return metadata


def read_annofile(annofile):

    anno = {'Values': []}
    with gzip.open(annofile, 'rt') as fi:
        for line in fi:
            section_match = re.match('^\[(\w+)\]', line)
            keyval_match = re.match('^(\w+?)=(.*)$', line)
            blank_match = re.match('^\s*$', line)
            val_match = re.match('^([\w\_\d].*\|.*)', line)
            if section_match:
                section = section_match.group(1)
                if section != 'Values':
                    anno[section] = {}

            elif keyval_match:
                key = keyval_match.group(1)
                val = keyval_match.group(2)
                if key not in anno[section]:
                    anno[section][key] = []
                anno[section][key].append(val)

            elif blank_match:
                pass

            elif val_match:
                val = val_match.group(1)
                (value, vid) = val.split('|')
                vid = vid.replace('_', ':')

                # ns_match = re.match('^([A-Z]+)_(.*)$', vid)
                # if ns_match:
                #     namespace = ns_match.group(1)
                #     _id = ns_match.group(2)

                anno[section].append({'id': f'{vid}', 'value': value})

    return anno


def build_json(annofiles):

    additional_metadata = add_metadata()

    for annofile, anno_src_url in annofiles:
        print('Processing belanno', os.path.basename(annofile))

        anno_dict = read_annofile(annofile)

        idx_len = len(anno_dict['Citation']['NameString'])

        annotation_type = anno_dict['AnnotationDefinition']['Keyword'][0]
        for idx in range(idx_len):

            metadata = {}
            namespace = additional_metadata[anno_dict['Citation']['NameString'][idx]]['namespace']
            terms_filename = f'{config["bel_resources"]["file_locations"]["terms_data"]}/{namespace}_belanno.jsonl.gz'
            metadata = {
                'name': anno_dict['Citation']['NameString'][idx],
                'namespace': namespace,
                'description': f"{anno_dict['Citation']['DescriptionString'][idx]}. NOTE: Converted from OpenBEL belanno file {anno_src_url}",
                'src_url': anno_dict['Citation']['ReferenceURL'][idx],
                'version': anno_dict['Citation']['PublishedVersionString'][idx],
            }

            terms = []
            for value in anno_dict['Values']:
                if re.match(f'^{namespace}', value['id']):
                    vid = value['id']
                    val = value['value']
                    src_id = vid.replace(f'{namespace}:', '')
                    term = {
                        "namespace": namespace,
                        "src_id": src_id,
                        "id": utils.get_prefixed_id(namespace, val),
                        "label": val,
                        "name": val,
                        "annotation_types": [annotation_type],
                        "alt_ids": [vid]
                    }
                    terms.append(copy.deepcopy(term))

            with gzip.open(terms_filename, 'wt') as fo:
                fo.write("{}\n".format(json.dumps({'metadata': metadata})))

                for term in terms:
                    fo.write("{}\n".format(json.dumps({'term': term})))


def main():
    annofiles = update_data_files()
    build_json(annofiles)


if __name__ == '__main__':
    # Setup logging
    global log
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging_conf_fn = f'{config["bel_resources"]["file_locations"]["root"]}/logging_conf.yml'
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}-terms')

    main()
