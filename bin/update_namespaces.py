#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  collect_namespaces.py

"""

import glob
import json
import os
import re
import subprocess

import app.settings as settings


def process_namespaces():

    files = glob.glob(f"{settings.appdir}/namespaces/*.py")
    tax_fn = [fn for fn in files if re.search("tax.py", fn)][0]
    files = [fn for fn in files if not re.search("tax.py|__init__|TEMPL", fn)]

    # Have to run Taxonomy first as some subsequent namespaces depend on tax_labels.json.gz
    print(f"Running {tax_fn}")
    subprocess.call(tax_fn)

    for fn in files:
        # Check if file is executable
        if os.access(fn, os.X_OK):
            print(f"Running {fn}")
            subprocess.call(fn)
        else:
            print(f"Skipping: not executable {fn}")


def main():
    process_namespaces()


if __name__ == "__main__":
    main()
