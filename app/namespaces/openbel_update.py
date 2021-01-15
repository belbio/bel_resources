#!/usr/bin/env python
# -*- coding: utf-8-*-

"""
Usage: $ {1: program}.py
"""

import json
import gzip
import glob
import shutil
import re


def namespace_quoting(string: str) -> str:
    """Normalize NSArg ID and Label

    If needs quotes (only if it contains whitespace, comma or ')' ), make sure
    it is quoted, else remove quotes

    Also escape any internal double quotes
    """

    # Remove quotes if exist
    match = re.match(r'\s*"(.*)"\s*$', string)
    if match:
        string = match.group(1)

    string = string.strip()  # remove external whitespace

    string = string.replace('"', '"')  # quote internal double quotes

    # quote only if it contains whitespace, comma, ! or ')'
    if re.search(r"[),\!\s]", string):
        return f'"{string}"'

    return string


files = glob.glob("/data/bel_resources/resources_v2/namespaces/tmp/*_bel*.jsonl.gz")

for belfile in files:

    belfile_new = belfile.replace("/tmp", "")

    print("BEL file", belfile_new)

    with gzip.open(belfile, "rb") as fi, gzip.open(belfile_new, "wt") as fo:
        for doc in fi:
            doc = json.loads(doc)
            if "term" in doc:
                alt_key = f"{doc['term']['namespace']}:{namespace_quoting(doc['term']['name'])}"
                if alt_key != doc["term"]["key"]:
                    doc["term"]["alt_keys"] = [alt_key]

            fo.write(f"{json.dumps(doc)}\n")


def main():
    pass


if __name__ == "__main__":
    main()
