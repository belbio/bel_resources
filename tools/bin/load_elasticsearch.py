#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  load_terms.py <customer>

"""

import elasticsearch
import elasticsearch.helpers
from elasticsearch import Elasticsearch
from typing import Iterable, Mapping, Any, List
import glob
import time
import datetime
import copy
import gzip
import json
import yaml
import logging
import logging.config
import click
import timy

import tools.utils.utils as utils
from tools.utils.Config import config
import bel.db.elasticsearch

from timy.settings import (
    timy_config,
    TrackingMode
)
timy_config.tracking_mode = TrackingMode.LOGGING


def collect_term_datasets() -> List[str]:
    """Collect all term datasets"""

    glob_query = f"{config['bel_resources']['file_locations']['data']}/namespaces/*.jsonl.gz"
    files = glob.glob(glob_query, recursive=False)
    return files


def get_terms(term_fn: str, index_name: str) -> Iterable[Mapping[str, Any]]:
    """Generator of Term records to load into Elasticsearch"""

    species_list = config['bel_resources'].get('species_list', [])

    with gzip.open(term_fn, 'rt') as f:

        metadata = f.__next__()
        if 'metadata' not in metadata:
            log.error(f'Missing metadata entry for {term_fn}')

        with timy.Timer() as timer:
            counter = 0
            step = 10000
            for line in f:
                counter += 1
                if counter % step == 0:
                    timer.track(f'Yielded {counter} terms')
                    # log.info(f"Yielded {counter} terms {timer.track()}")

                term = json.loads(line)['term']

                # Filter species if enabled in config
                species_id = term.get('species_id', None)
                if species_list and species_id and species_id not in species_list:
                    continue

                name = None
                if term.get('name', None) and term.get('label', None) and term['name'] == term['label']:
                    name = term['name']
                elif term.get('name', None) and term.get('label', None):
                    name = [term['name'], term['label']]

                all_term_ids = set()
                for term_id in [term['id']] + term.get('alt_ids', []):
                    all_term_ids.add(term_id)
                    all_term_ids.add(utils.lowercase_term_id(term_id))

                term['alt_ids'] = copy.copy(list(all_term_ids))

                # create completions
                contexts = {
                    "species_id": term.get('species', []),
                    "entity_types": term.get('entity_types', []),
                    "context_types": term.get('context_types', []),
                }

                term['completions'] = []
                term['completions'].append({"input": term['id'], "weight": 10, "contexts": contexts})

                if name:
                    term['completions'].append({"input": name, "weight": 10, "contexts": contexts})

                if 'synonyms' in term:
                    term['completions'].append({"input": term['synonyms'], "weight": 3, "contexts": contexts})
                if 'alt_ids' in term:
                    term['completions'].append({"input": term['alt_ids'], "weight": 1, "contexts": contexts})

                yield {
                    '_op_type': 'index',
                    '_index': index_name,
                    '_type': 'term',
                    '_id': term['id'],
                    '_source': copy.deepcopy(term)
                }


@click.command()
@click.argument('namespaces', nargs=-1)
@click.option('-d', '--delete/--no-delete', default=False, help='Delete existing terms index')
@click.option('-i', '--index_name', default='terms_blue', help='Use this name for index.  Default is "terms_blue"')
def main(namespaces, delete, index_name, all):
    """Load Namespaces

    Load the given namespace prefixes (<prefix>.jsonl.gz) from the bel_resources/data/namespaces directory
    If namespaces is empty, load all namespaces found.
    """

    if delete:
        es = bel.db.elasticsearch.get_client(delete=True)
    else:
        es = bel.db.elasticsearch.get_client()

    files = []
    if namespaces:
        for ns in namespaces:
            ns_file = f"{config['bel_resources']['file_locations']['data']}/namespaces/{ns}.jsonl.gz"
            files.append(ns_file)
    else:
        files = collect_term_datasets()

    for fn in sorted(files):
        log.info(f'Loading {fn}')
        terms = get_terms(fn, index_name)
        bel.db.elasticsearch.bulk_load_terms(es, terms, index_name)


if __name__ == '__main__':

    # Setup logging
    global log

    logging.config.dictConfig(config['logging'])
    log = logging.getLogger(__name__)

    main()

