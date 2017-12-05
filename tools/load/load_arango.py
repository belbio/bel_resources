#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  program.py <customer>

"""

from arango import ArangoClient, ArangoError
import os
import sys
import yaml
import glob
import gzip
import json
import logging
import logging.config
import timy

from bel_lang.Config import config

# Import local util module
sys.path.append("..")
import utils

term_fp = "../data/terms"
equivalence_term_files = [
    f"{term_fp}/hgnc.jsonl.gz",
    f"{term_fp}/mgi.jsonl.gz",
    f"{term_fp}/rgd.jsonl.gz",
    f"{term_fp}/sp.jsonl.gz",
    f"{term_fp}/chembl.jsonl.gz",
]

client = ArangoClient(
    protocol='http',
    host='localhost',
    port=8529,
    username='',
    password='',
    enable_logging=True,
)

db_name = 'bel'
ortholog_node_coll_name = 'ortholog_nodes'
ortholog_edge_coll_name = 'ortholog_edges'
equiv_node_coll_name = 'equivalence_nodes'
equiv_edge_coll_name = 'equivalence_edges'


def clean_db(db_name):
    try:
        client.delete_database(db_name)
    except ArangoError as e:
        pass


def get_collections():

    # Create a new database named "bel"
    try:
        db = client.create_database(db_name)
    except ArangoError as ae:
        db = client.db(db_name)
    except Exception as e:
        log.error('Error creating database', e)

    try:
        ortholog_nodes = db.create_collection(ortholog_node_coll_name)
        ortholog_edges = db.create_collection(ortholog_edge_coll_name)
        ortholog_nodes.add_hash_index('tax_id', sparse=True)
    except ArangoError as ae:
        ortholog_nodes = db.collection(ortholog_node_coll_name)
        ortholog_edges = db.collection(ortholog_edge_coll_name)
    except Exception as e:
        log.error('Exception ', e)

    try:
        equiv_nodes = db.create_collection(equiv_node_coll_name)
        equiv_edges = db.create_collection(equiv_edge_coll_name)
        equiv_nodes.add_hash_index('namespace', sparse=True)
    except ArangoError as ae:
        equiv_nodes = db.collection(equiv_node_coll_name)
        equiv_edges = db.collection(equiv_edge_coll_name)
    except Exception as e:
        log.error('Exception ', e)

    return(ortholog_nodes, ortholog_edges, equiv_nodes, equiv_edges)


def get_edge_collections():

    # Create a new database named "bel"
    try:
        db = client.create_database(db_name)
    except ArangoError as ae:
        db = client.db(db_name)
    except Exception as e:
        log.error('Error creating database', e)

    try:
        orthologs = db.create_graph('orthologs')
        ortholog_nodes = orthologs.create_vertex_collection(ortholog_node_coll_name)
        ortholog_edges = orthologs.create_edge_definition(
            name=ortholog_edge_coll_name,
            from_collections=[ortholog_node_coll_name],
            to_collections=[ortholog_node_coll_name],
        )
    except ArangoError as ae:
        orthologs = db.graph('orthologs')
        ortholog_nodes = orthologs.vertex_collection(ortholog_node_coll_name)
        ortholog_nodes.add_hash_index('tax_id', sparse=True)
        ortholog_edges = orthologs.edge_collection(ortholog_edge_coll_name)
    except Exception as e:
        log.error('Exception ', e)

    try:
        equiv = db.create_graph('equivalences')
        equiv_nodes = equiv.create_vertex_collection(equiv_node_coll_name)
        equiv_nodes.add_hash_index('namespace', sparse=True)
        equiv_edges = equiv.create_edge_definition(
            name=equiv_edge_coll_name,
            from_collections=[equiv_node_coll_name],
            to_collections=[equiv_node_coll_name],
        )
    except ArangoError as ae:
        equiv = db.graph('equivalences')
        equiv_nodes = equiv.vertex_collection(equiv_node_coll_name)
        equiv_edges = equiv.edge_collection(equiv_edge_coll_name)
    except Exception as e:
        log.error('Exception ', e)

    return(ortholog_nodes, ortholog_edges, equiv_nodes, equiv_edges)


def load_orthologs_1x(ortholog_nodes, ortholog_edges):
    """Load orthologs"""

    source = ""
    files = glob.glob('../data/orthologs/hgnc*.jsonl*')
    for fn in files:
        with gzip.open(fn, 'rt') as fi:
            for line in fi:
                edge = json.loads(line)
                if 'metadata' in edge:
                    source = edge['metadata']['source']
                elif 'ortholog' in edge:
                    edge = edge['ortholog']
                    s_id = edge['subject']['id']
                    o_id = edge['object']['id']
                    s_id = utils.arango_id_to_key(s_id)
                    o_id = utils.arango_id_to_key(o_id)

                    try:
                        ortholog_nodes.insert({'_key': s_id, 'tax_id': edge['subject']['tax_id']})
                    except ArangoError as ae:
                        pass

                    try:
                        ortholog_nodes.insert({'_key': o_id, 'tax_id': edge['object']['tax_id']})
                    except ArangoError as ae:
                        pass

                    try:
                        ortholog_edges.insert(
                            {
                                '_from': f"{ortholog_node_coll_name}/{edge['subject']['id']}",
                                '_to': f"{ortholog_node_coll_name}/{edge['object']['id']}",
                                'source': source,
                            }
                        )
                    except ArangoError as ae:
                        pass


def load_orthologs_bulk(ortholog_nodes, ortholog_edges):
    files = glob.glob('../data/orthologs/hgnc*.jsonl*')
    for fn in files:
        nodes = []
        edges = []
        cnt = {}
        with timy.Timer() as timer:
            with gzip.open(fn, 'rt') as fi:
                for line in fi:
                    edge = json.loads(line)
                    if 'metadata' in edge:
                        source = edge['metadata']['source']
                    elif 'ortholog' in edge:
                        edge = edge['ortholog']
                        cnt[edge['subject']['id']] = 1
                        cnt[edge['object']['id']] = 1
                        nodes.append({'_key': edge['subject']['id'], 'tax_id': edge['subject']['tax_id']})
                        nodes.append({'_key': edge['object']['id'], 'tax_id': edge['object']['tax_id']})
                        edges.append(
                            {
                                '_from': f"{ortholog_node_coll_name}/{edge['subject']['id']}",
                                '_to': f"{ortholog_node_coll_name}/{edge['object']['id']}",
                                'source': source,
                            }
                        )

            timer.track('Before import')

            ortholog_nodes.import_bulk(nodes, halt_on_error=False, on_duplicate="ignore")
            ortholog_edges.import_bulk(edges, halt_on_error=False, on_duplicate="ignore")
            print('Number of unique nodes: ', len(cnt), ' Num of nodes: ', len(nodes))


def create_ortholog_arangoimp_files():

    nodes_fn = './ortholog_nodes.jsonl'
    edges_fn = './ortholog_edges.jsonl'

    nodes = {}

    with open(edges_fn, 'wt') as foe:
        files = glob.glob('../data/orthologs/*.jsonl*')
        for fn in files:
            log.info(f"Starting ortholog fn: {fn}")
            with gzip.open(fn, 'rt') as fi:
                for line in fi:
                    edge = json.loads(line)
                    if 'metadata' in edge:
                        source = edge['metadata']['source']
                    elif 'ortholog' in edge:
                        edge = edge['ortholog']
                        nodes[edge['subject']['id']] = {'_key': edge['subject']['id'], 'tax_id': edge['subject']['tax_id']}
                        nodes[edge['object']['id']] = {'_key': edge['object']['id'], 'tax_id': edge['object']['tax_id']}
                        arango_edge = {
                            '_from': f"{ortholog_node_coll_name}/{edge['subject']['id']}",
                            '_to': f"{ortholog_node_coll_name}/{edge['object']['id']}",
                            'type': 'ortholog_to',
                            'source': source,
                        }

                        foe.write("{}\n".format(json.dumps(arango_edge)))

    with open(nodes_fn, 'wt') as fon:
        for node in nodes:
            fon.write("{}\n".format(json.dumps(nodes[node])))

    # Add --server.username root --server.password xxx if you the database is pw protected
    # The cat "" | takes care of the password prompt if you aren't using a password
    print(f'time arangoimp --server.authentication false --server.database "bel" --file {nodes_fn} --type json --overwrite true --collection {ortholog_node_coll_name} --create-collection true --progress true')
    print(f'time arangoimp --server.authentication false --server.database "bel" --file {edges_fn} --type json --overwrite true --collection {ortholog_edge_coll_name} --create-collection true --create-collection-type edge --progress true')


def create_equivalence_arangoimp_files():

    nodes_fn = './equiv_nodes.jsonl'
    edges_fn = './equiv_edges.jsonl'

    nodes = {}
    with open(edges_fn, 'wt') as foe:
        for fn in equivalence_term_files:
            log.info(f"Starting equivalence fn: {fn}")
            with gzip.open(fn, 'rt') as fi:
                for line in fi:
                    term = json.loads(line)

                    if 'metadata' in term:
                        continue  # Skip metadata entry
                    term = term['term']
                    source = term['namespace']

                    term_id = term['id']
                    (ns, val) = term_id.split(':', maxsplit=1)
                    nodes[term_id] = {'_key': term_id, 'namespace': ns}
                    for eqv in term['equivalences']:
                        (ns, val) = eqv.split(':', maxsplit=1)
                        nodes[eqv] = {'_key': eqv, 'namespace': ns}
                        arango_edge = {
                            '_from': f"{equiv_node_coll_name}/{term_id}",
                            '_to': f"{equiv_node_coll_name}/{eqv}",
                            'type': 'equivalent_to',
                            'source': source,
                        }
                        foe.write("{}\n".format(json.dumps(arango_edge)))

    with open(nodes_fn, 'wt') as fon:
        for node in nodes:
            fon.write("{}\n".format(json.dumps(nodes[node])))

    # Add --server.username root --server.password xxx if you the database is pw protected
    # The cat "" | takes care of the password prompt if you aren't using a password
    print(f'time arangoimp --server.authentication false --server.database "bel" --file {nodes_fn} --type json --overwrite true --collection {equiv_node_coll_name} --create-collection true --progress true')
    print(f'time arangoimp --server.authentication false --server.database "bel" --file {edges_fn} --type json --overwrite true --collection {equiv_edge_coll_name} --create-collection true --create-collection-type edge --progress true')


def index_collections():

    # TODO
    # index tax_id in ortholog_nodes
    # index namespace in equiv_nodes
    pass


def main():

    # clean_db(db_name)

    # (ortholog_nodes, ortholog_edges, equiv_nodes, equiv_edges) = get_collections()

    # load_orthologs_bulk(ortholog_nodes, ortholog_edges)

    # create_ortholog_arangoimp_files()

    create_equivalence_arangoimp_files()


if __name__ == '__main__':
    # Setup logging
    global log
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace('.py', '')

    logging_conf_fn = f'{config["bel_resources"]["file_locations"]["root"]}/logging_conf.yml'
    with open(logging_conf_fn, mode='r') as f:
        logging.config.dictConfig(yaml.load(f))
        log = logging.getLogger(f'{module_fn}')

    main()

