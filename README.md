# Resource tools

These scripts will create standardized load files for BEL Resources so they can be loaded into the BELBio API.

## Setup

Make sure you have Python 3.6+ on your machine and update the setup.sh script to use it
in the virtualenv command

Run `./setup.sh`

You will need to use `source .venv/bin/activate` to start the virtualenv

*Using Docker for Mac - need to set Docker to use 4Gb of Memory in order to allow
Elasticsearch to use 2Gb of Heap/Ram (see docker-compose.yml for elasticsearch Java options)*
otherwise Elasticsearch will crash with a 137 error (which means out of memory).

Have to use local directory storage on Mac for Docker for elasticsearch and arangodb
due to 60Gb limitation on all Docker images/dockervolumes/etc.

## Using these tools

*TODO* -- create a Makefile to run all of the update scripts - all update scripts
should use a general configuration file to determine if they run or not

Run the scripts found in the following directories to update the load files:

* terms - namespace and annotation load files
* orthologs
* taxonomy - NCBI species taxonomy
* gene_backbone - gene -> mRNA -> protein
* named_complexes - named protein complex members

## Process flow

### Step 1
Each script will download the datafiles needed from the source data repositories such as Uniprot, EntrezGene, GO, etc. Where possible, they will only re-download if the source data file is newer than the one you have downloaded. As a fallback if the source data file modification date cannot be determined, the script will only re-download a file if it is more than a week since it was last downloaded.

### Step 2
The script will extract files if need from a tar file into a temp directory

### Step 3
Script will process source data files and generate a standardized load file following the appropriate schemas found in the bel_api for loading namespace/annotation terms

### Step 4  *TODO*
Upload the load files using the BELBio API with the upload

## Notes

### Elasticsearch term indexing stats

Almost 18M terms in search index.  It takes 6.6Gb of diskspace.

| Namespace  | Count      |
| ---------- | ---------- |
| EG | 15,428,000 |
| TAX | 1,645,046 |
| SP | 555,426 |
| CHEBI | 115,240 |
| MGI | 56,181 |
| RGD | 44,973 |
| HGNC | 41,231 |
| MESH | 18,187 |
| DO | 8,507 |


### Orthology ArangoDB arangoimp load times

In docker container on Mac Laptop - limited to 4 cpus and 4Gb RAM.  Example
load command below:

    time cat "" | arangoimp --server.database "bel" --file "ortholog_edges.jsonl" --type json --overwrite true --collection "ortholog_edges" --create-collection true --progress true

* Loaded 3M orthology nodes in 90 seconds
* Loaded 3M orthology edges in 45 seconds
