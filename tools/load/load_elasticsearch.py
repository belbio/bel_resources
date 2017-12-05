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
from bel_lang.Config import config

from timy.settings import (
    timy_config,
    TrackingMode
)
timy_config.tracking_mode = TrackingMode.LOGGING

# Globals
server = config['bel_api']['servers']['elasticsearch']
es = Elasticsearch([server], send_get_body_as='POST')

# TODO - start of using index aliases for managing updating Elasticsearch
today_str = datetime.date.today().strftime("%Y-%m-%d")
index_name = 'terms_' + today_str


def collect_term_datasets() -> List[str]:
    """Collect all term datasets"""

    glob_query = f"{config['bel_resources']['file_locations']['terms_data']}/*.jsonl.gz"
    files = glob.glob(glob_query, recursive=False)
    return files


def get_terms(term_fn: str, index_name: str) -> Iterable[Mapping[str, Any]]:
    """Generator of Term records to load into Elasticsearch"""

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


def load_term_dataset(term_fn: str, index_name: str):
    """Load term datasets using Elasticsearch bulk load

    Args:
        term_fn (str): terminology filename to load
        index_name (str): terms index name to use (so it can be aliased to 'terms')
    """
    terms = (get_terms(term_fn, index_name))

    chunk_size = 200

    try:
        results = elasticsearch.helpers.bulk(es, terms, chunk_size=chunk_size)

        # elasticsearch.helpers.parallel_bulk(es, terms, chunk_size=chunk_size, thread_count=4)
        if len(results[1]) > 0:
            log.error('Bulk load errors {}'.format(results))
    except elasticsearch.ElasticsearchException as e:
        log.error('Indexing error: {}\n'.format(e))


class FuncTimer():
    """ Convenience class to time function calls

    Use via the "with" keyword ::

        with Functimer("Expensive Function call"):
            foo = expensiveFunction(bar)

    A timer will be displayed in the current logger as `"Starting expensive function call ..."`
    then when the code exits the with statement, the log will mention `"Finished expensive function call in 28.42s"`

    By default, all FuncTimer log messages are written at the `logging.DEBUG` level. For info-level messages, set the
    `FuncTimer.info`  argument to `True`::

        with Functimer("Expensive Function call",info=True):
            foo = expensiveFunction(bar)
    """

    def __init__(self, funcName, info=False):

        self.funcName = funcName
        self.infoLogLevel = True

    def __enter__(self):
        log.debug("Starting {} ...".format(self.funcName))
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start
        log.info("{} over in {}s".format(self.funcName, self.interval).capitalize())


@click.command()
@click.argument('namespaces', nargs=-1)
@click.option('--index_name', default='terms_blue', help='Use this name for index.  Default is "terms_blue"')
@click.option('--runall/--no-runall', default=False, help="Load all namespaces")
def main(namespaces, index_name, runall):
    """Load Namespaces

    Load the given namespace prefixes (<prefix>.jsonl.gz) from the data/terms directory
    """

    files = []
    if runall:
        files = collect_term_datasets()
    else:
        for ns in namespaces:
            ns_file = f"{config['bel_resources']['file_locations']['terms_data']}/{ns}.jsonl.gz"
            files.append(ns_file)

    for fn in sorted(files):
        log.info(f'Loading {fn}')
        with FuncTimer(f'load terms {fn}', info=True):
            load_term_dataset(fn, index_name)


if __name__ == '__main__':

    # Setup logging
    global log
    logging_conf_fn = config['bel_resources']['file_locations']['logging_conf_fn']
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(name='load_elasticsearch')

    files = collect_term_datasets()

    main()

