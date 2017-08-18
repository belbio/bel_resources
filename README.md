# Resource tools

These scripts will create standardized load files for BEL Resources so they can be loaded into the BELBio API.

## Setup

Make sure you have Python 3.6+ on your machine and update the setup.sh script to use it
in the virtualenv command

Run `./setup.sh`

You will need to use `source .venv/bin/activate` to start the virtualenv

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
