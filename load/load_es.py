#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  load_terms.py <customer>

"""

import elasticsearch
import elasticsearch.helpers
from elasticsearch import Elasticsearch
import glob
import time
import datetime
import copy
import gzip
import json
import yaml
import logging
import logging.config

# Globals
es_conn = 'http://localhost:9200'
es = Elasticsearch([es_conn], send_get_body_as='POST')

today_str = datetime.date.today().strftime("%Y-%m-%d")

term_dir = "../data/terms/*jsonl*"

index_name = 'terms_' + today_str


def collect_term_datasets():
    files = glob.glob(term_dir, recursive=False)
    return files


def get_terms(term_fn):

    with gzip.open(term_fn, 'rt') as f:

        metadata = f.__next__()
        if 'metadata' not in metadata:
            log.error(f'Missing metadata entry for {term_fn}')

        count = 0
        for line in f:
            count += 1
            term = json.loads(line)['term']

            name = None
            if term.get('name', None) and term.get('label', None) and term['name'] == term['label']:
                name = term['name']
            elif term.get('name', None) and term.get('label', None):
                name = [term['name'], term['label']]

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
                '_index': 'terms',
                '_type': 'term',
                '_id': term['id'],
                '_source': copy.deepcopy(term)
            }


def load_term_dataset(term_fn):
    terms = (get_terms(term_fn))

    chunk_size = 200

    try:
        results = elasticsearch.helpers.bulk(es, terms, chunk_size=chunk_size)
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
        self.infoLogLevel = info

    def __enter__(self):
        if self.infoLogLevel:
            log.info("Starting {} ...".format(self.funcName))
        else:
            log.debug("Starting {} ...".format(self.funcName))
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start
        if self.infoLogLevel:
            log.info("{} over in {}s".format(self.funcName, self.interval).capitalize())
        else:
            log.debug("{} over in {}s".format(self.funcName, self.interval).capitalize())


def main():

    files = collect_term_datasets()
    # print(files)
    import re

    for fn in files:
        if re.search('belns', fn) or re.search('belns', fn):  # or re.search('taxonomy', fn):

            print(f'Starting {fn}')
            with FuncTimer(f'load terms {fn}', info=True):
                load_term_dataset(fn)


if __name__ == '__main__':

    # Setup logging
    global log
    logging_conf_fn = '../logging-conf.yaml'
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(name=__name__)

    files = collect_term_datasets()

    main()

