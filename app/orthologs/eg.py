#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  hgnc.py

"""

import datetime
import gzip
import json
import os
import re
import tempfile

import app.settings as settings
import app.utils as utils
import structlog

log = structlog.getLogger(__name__)

# Globals
prefix = "eg"
namespace = settings.NAMESPACE_DEFINITIONS[prefix]

species_namespace = settings.NAMESPACE_DEFINITIONS["tax"]

data_fp = settings.DATA_DIR
orthologs_fp = f"{data_fp}/orthologs/{prefix}.jsonl.gz"

# Human, mouse, rat, zebrafish
orthologs_hmrz_fp = f"{data_fp}/orthologs/{prefix}_hmrz.jsonl.gz"
hmrz_species = ["TAX:9606", "TAX:10090", "TAX:10116", "TAX:7955"]

tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = "ftp.ncbi.nlm.nih.gov"
remote_file = "/gene/DATA/gene_orthologs.gz"

orthologs_metadata = {
    "source": namespace["namespace"],
    "src_url": namespace["src_url"],
    "type": "ortholog",
    "description": namespace["description"] + " orthologs",
    "version": dt,
}


# Local data filepath setup
basename = os.path.basename(remote_file)

if not re.search(
    ".gz$", basename
):  # we basically gzip everything retrieved that isn't already gzipped
    basename = f"eg_{basename}.gz"

local_data_fp = f"{settings.DOWNLOAD_DIR}/{basename}"


def update_data_files() -> bool:
    """ Download data files if needed

    Args:
        None
    Returns:
        bool: files updated = True, False if not
    """

    # Update data file
    result = utils.get_ftp_file(server, remote_file, local_data_fp)

    changed = False
    if "Downloaded" in result[1]:
        changed = True

    return changed


def build_json(force: bool = False):
    """Build HGNC namespace json load file

    Args:
        force (bool): build json result regardless of file mod dates
    Returns:
        None
    """

    # Don't rebuild file if it's newer than downloaded source file
    if not force:
        if utils.file_newer(orthologs_fp, local_data_fp):
            log.info("Will not rebuild data file as it is newer than downloaded source file")
            return False

    with gzip.open(local_data_fp, "rt") as fi, gzip.open(orthologs_fp, "wt") as fo:
        # Header JSONL record for terminology
        fo.write("{}\n".format(json.dumps({"metadata": orthologs_metadata})))

        fi.__next__()  # skip header line

        for line in fi:
            (
                subj_tax_id,
                subj_gene_id,
                relationship,
                obj_tax_id,
                obj_gene_id,
            ) = line.rstrip().split("\t")
            if relationship != "Ortholog":
                continue

            ortholog = {
                "subject": {
                    "id": f"{namespace['namespace']}:{subj_gene_id}",
                    "tax_id": f"{species_namespace['namespace']}:{subj_tax_id}",
                },
                "object": {
                    "id": f"{namespace['namespace']}:{obj_gene_id}",
                    "tax_id": f"{species_namespace['namespace']}:{obj_tax_id}",
                },
            }

            # Add ortholog to JSONL
            fo.write("{}\n".format(json.dumps({"ortholog": ortholog})))


def build_hmr_json():
    """Extract Human, Mouse and Rat from EG into a new file """

    with gzip.open(orthologs_fp, "rt") as fi, gzip.open(orthologs_hmrz_fp, "wt") as fo:
        for line in fi:
            doc = json.loads(line)
            if "metadata" not in doc:
                if (
                    doc["ortholog"]["subject"]["tax_id"] in hmrz_species
                    and doc["ortholog"]["object"]["tax_id"] in hmrz_species
                ):
                    fo.write("{}\n".format(json.dumps(doc)))
            elif "metadata" in doc:
                fo.write("{}\n".format(json.dumps(doc)))


def main():

    # Cannot detect changes as ftp server doesn't support MLSD cmd
    update_data_files()
    build_json()
    build_hmr_json()


if __name__ == "__main__":
    main()
