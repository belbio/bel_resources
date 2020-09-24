#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  virtuals.py

"""
import copy
import datetime
import gzip
import json
import os
import re
import sys
import tempfile
from pathlib import Path

import structlog
import yaml

import app.settings as settings
import app.setup_logging
import typer
from app.common.resources import get_metadata, get_species_labels
from app.schemas.main import Term
from typer import Option

log = structlog.getLogger("virtuals.py")


def build_json():
    """Build term json load files for all virtual and identifier.org namespaces

    Returns:
        None
    """

    for key in settings.NAMESPACE_DEFINITIONS:
        doc = settings.NAMESPACE_DEFINITIONS[key]
        if doc["namespace_type"] in ["virtual", "identifers_org"]:
            resource_fn = f"{settings.DATA_DIR}/namespaces/{key}.jsonl.gz"

            with gzip.open(resource_fn, "wt") as fo:
                # Header JSONL record for terminology
                metadata = get_metadata(doc)
                fo.write("{}\n".format(json.dumps({"metadata": metadata})))


def main():
    """Build virtual namespace specification files
    
    Basically - these are namespace.jsonl.gz files with just the metadata entry
    """

    build_json()


if __name__ == "__main__":
    typer.run(main)
