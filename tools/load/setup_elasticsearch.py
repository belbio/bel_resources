#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  program.py <customer>

"""

from elasticsearch import Elasticsearch
import yaml
import logging
import logging.config
import click

from tools.utils.Config import config

# Globals
server = config['bel_api']['servers']['elasticsearch']
es = Elasticsearch([server], send_get_body_as='POST')

mapping_term_fn = f"{config['bel_resources']['file_locations']['tools']}/load/setup_elasticsearch.yaml"

with open(mapping_term_fn, 'r') as f:
    mapping_term = yaml.load(f)


def index_exists(index_name):
    """
    Input: index -- index to check for existence

    """
    return es.indices.exists(index=index_name)


def create_terms_index(clean, index_name):

    if clean and index_exists(index_name):
        r = es.indices.delete(index=index_name)
        r = es.indices.create(index=index_name, body=mapping_term)
        log.debug('Index create result: ', r)
    else:
        r = es.indices.create(index=index_name, body=mapping_term)
        log.debug('Index create result: ', r)


@click.command()
@click.option('--clean/--no-clean', default=False, help="Remove indexes and re-create them")
@click.option('--index_name', default='terms_blue', help='Use this name for index.')
def main(clean, index_name):
    """Setup Elasticsearch term indexes

    This will by default only create the indexes and run the term index mapping
    if the indexes don't exist.  The --clean option will force removal of the
    index if it exists.

    The index_name will be aliased to the index 'terms' when it's ready
    """

    create_terms_index(clean, index_name)


if __name__ == '__main__':
    # Setup logging
    global log

    logging_conf_fn = config['bel_resources']['file_locations']['logging_conf_fn']
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger('setup_elasticsearch')

    main()

