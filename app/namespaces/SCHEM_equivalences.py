#!/usr/bin/env python
# -*- coding: utf-8-*-

"""
Usage: $ {1: program}.py
"""

import gzip
import json
import re

prefix = "SCHEM"
equivalences_fn = f"/data/bel_resources/downloads/{prefix}_equivalences.txt"
ns_fn = f"/data/bel_resources/data/namespaces/{prefix}_belns.jsonl.gz"
ns2_fn = f"/data/bel_resources/data/namespaces/{prefix}_equiv_belns.jsonl.gz"


def collect_equivalences(fn):
    """Collect equivalences from filename"""

    equivalences = {}

    with open(fn, "r") as f:
        for line in f:
            if line.startswith("#") or line.startswith("ID"):
                continue

            (
                id,
                altids,
                label,
                synonyms,
                description,
                type_,
                species,
                xref,
                obsolete,
                parents,
                children,
            ) = line.split("\t")

            # print(f"id: {id} label: {label}  xref: {xref}")
            # print(f"xref: {xref}")
            if "," in xref:
                print("Problem with xref", xref)

            if xref:
                if re.search('[,"\s\(\)]+', label):
                    label.strip().strip('"').strip()
                    label = f'"{label}"'

                xref = xref.replace("MESHC", "MESH")

                equivalences[f"{prefix}:{label}"] = xref

    return equivalences


def add_equivalences(equivalences):

    with gzip.open(ns_fn, "rt") as fin, gzip.open(ns2_fn, "wt") as fout:
        for line in fin:
            r = json.loads(line)
            if "term" not in r:
                fout.write(line)
                continue

            if r["term"]["id"] in equivalences:
                r["term"]["equivalences"] = [equivalences[r["term"]["id"]]]

            fout.write(f"{json.dumps(r)}\n")


def main():
    equivalences = collect_equivalences(equivalences_fn)
    add_equivalences(equivalences)


if __name__ == "__main__":
    main()
