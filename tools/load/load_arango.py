#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Load

"""

import os
import sys
import yaml
import glob
import gzip
import json
import logging
import logging.config
import timy

from tools.utils.Config import config
import bel_db.arangodb as arangodb

db_name = 'bel'
ortholog_node_coll_name = 'ortholog_nodes'
ortholog_edge_coll_name = 'ortholog_edges'
equiv_node_coll_name = 'equivalence_nodes'
equiv_edge_coll_name = 'equivalence_edges'


def orthologs_iter():
    """Ortholog node and edge iterator"""

    files = glob.glob(f"{config['bel_resources']['file_locations']['data']}/orthologs/*.jsonl*")
    species_list = config['bel_resources'].get('species_list', [])

    for fn in files:
        log.info(f"Starting ortholog fn: {fn}")
        with gzip.open(fn, 'rt') as fi:
            for line in fi:
                edge = json.loads(line)
                if 'metadata' in edge:
                    source = edge['metadata']['source']
                    print('Source', source)
                    continue

                if 'ortholog' in edge:
                    edge = edge['ortholog']
                    subj_tax_id = edge['subject']['tax_id']
                    obj_tax_id = edge['object']['tax_id']

                    # Skip if species not listed in species_list
                    if species_list and subj_tax_id and subj_tax_id not in species_list:
                        continue
                    if species_list and obj_tax_id and obj_tax_id not in species_list:
                        continue

                    # Converted to ArangoDB legal chars for _key
                    subj_key = arangodb.arango_id_to_key(edge['subject']['id'])
                    subj_id = edge['subject']['id']

                    # Converted to ArangoDB legal chars for _key
                    obj_key = arangodb.arango_id_to_key(edge['object']['id'])
                    obj_id = edge['object']['id']

                    # Subject node
                    yield (ortholog_node_coll_name, {'_key': subj_key, 'name': subj_id, 'tax_id': edge['subject']['tax_id']})
                    # Object node
                    yield (ortholog_node_coll_name, {'_key': obj_key, 'name': obj_id, 'tax_id': edge['object']['tax_id']})

                    arango_edge = {
                        '_from': f"{ortholog_node_coll_name}/{subj_key}",
                        '_to': f"{ortholog_node_coll_name}/{obj_key}",
                        'type': 'ortholog_to',
                        'source': source,
                    }

                    yield (ortholog_edge_coll_name, arango_edge)


def equivalences_iter():
    """Equivalence node and edge iterator"""

    species_list = config['bel_resources'].get('species_list', [])
    files = glob.glob(f"{config['bel_resources']['file_locations']['data']}/terms/*.jsonl*")
    for fn in files:
        log.info(f"Starting equivalence fn: {fn}")
        with gzip.open(fn, 'r') as fi:
            for line in fi:
                term = json.loads(line)

                # skip if not term record (e.g. is a metadata record)
                if 'term' not in term:
                    continue

                term = term['term']
                # start new file if equivalences are not available in term record
                if 'equivalences' not in term:
                    break

                species_id = term.get('species_id', None)
                # Skip if species not listed in species_list
                if species_list and species_id and species_id not in species_list:
                    continue

                source = term['namespace']
                term_id = term['id']
                term_key = arangodb.arango_id_to_key(term_id)

                (ns, val) = term_id.split(':', maxsplit=1)

                yield (equiv_node_coll_name, {'_key': term_key, 'name': term_id, 'namespace': ns})

                for eqv in term['equivalences']:
                    (ns, val) = eqv.split(':', maxsplit=1)
                    eqv_key = arangodb.arango_id_to_key(eqv)
                    yield (equiv_node_coll_name, {'_key': eqv_key, 'name': eqv, 'namespace': ns})
                    arango_edge = {
                        '_from': f"{equiv_node_coll_name}/{term_key}",
                        '_to': f"{equiv_node_coll_name}/{eqv_key}",
                        'type': 'equivalent_to',
                        'source': source,
                    }
                    yield (equiv_edge_coll_name, arango_edge)


def main():

    client = arangodb.get_client()
    belterms_db = arangodb.get_belterms_handle(client)

    arangodb.batch_load_docs(belterms_db, equivalences_iter())
    arangodb.batch_load_docs(belterms_db, orthologs_iter())


if __name__ == '__main__':

    # Setup logging
    global log

    logging.config.dictConfig(config['logging'])
    log = logging.getLogger(name='load_arangodb')

    main()

