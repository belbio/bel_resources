#!/bin/bash

time arangoimp --server.authentication false --server.database "bel" --file ./ortholog_nodes.jsonl --type json --overwrite true --collection ortholog_nodes --create-collection true --progress true

time arangoimp --server.authentication false --server.database "bel" --file "./ortholog_edges.jsonl" --type json --overwrite true --collection ortholog_edges --create-collection true --create-collection-type edge --progress true

time arangoimp --server.authentication false --server.database "bel" --file ./equiv_nodes.jsonl --type json --overwrite true --collection equivalence_nodes --create-collection true --progress true

time arangoimp --server.authentication false --server.database "bel" --file "./equiv_edges.jsonl" --type json --overwrite true --collection equivalence_edges --create-collection true --create-collection-type edge --progress true
