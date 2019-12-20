#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  collect_orthologs.py

"""

import glob
import json
import os
import re
import subprocess

import app.settings as settings


def process_orthologs():

    files = glob.glob(f"{settings.appdir}/orthologs/*.py")
    # Skip files that are not to be used
    files = [fn for fn in files if not re.search("__init__|TEMPL", fn)]

    for fn in files:
        # Check if file is executable
        if os.access(fn, os.X_OK):
            print(f"Running {fn}")
            subprocess.call(fn)
        else:
            print(f"Skipping: not executable {fn}")


def main():
    process_orthologs()


if __name__ == "__main__":
    main()
