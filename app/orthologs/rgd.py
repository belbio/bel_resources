#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  hgnc.py

"""

import datetime
import gzip
import json
import logging
import os
import re
import tempfile

import yaml

import app.settings as settings
import app.utils as utils

# Globals
prefix = "rgd"
namespace = settings.NAMESPACE_DEFINITIONS[prefix]

data_fp = settings.DATA_DIR
orthologs_fp = f"{data_fp}/orthologs/{prefix}.jsonl.gz"

tmpdir = tempfile.TemporaryDirectory(suffix=None, prefix=None, dir=None)
dt = datetime.datetime.now().replace(microsecond=0).isoformat()

server = "ftp.rgd.mcw.edu"
remote_file = "/pub/data_release/RGD_ORTHOLOGS.txt"

orthologs = {
    "source": namespace["namespace"],
    "src_url": namespace["src_url"],
    "description": namespace["description"] + " orthologs",
    "version": dt,
    "orthologies": [],
}

# Local data filepath setup
basename = os.path.basename(remote_file)

if not re.search(
    ".gz$", basename
):  # we basically gzip everything retrieved that isn't already gzipped
    basename = f"{basename}.gz"

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

    with gzip.open(local_data_fp, "rt") as f:
        for line in f:
            pass  # TODO

    with gzip.open(orthologs_fp, "wt") as f:
        json.dump(orthologs, f, indent=4)


def main():

    update_data_files()
    build_json()


if __name__ == "__main__":
    # Setup logging
    global log
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace(".py", "")

    log = logging.getLogger(__name__)

    main()
