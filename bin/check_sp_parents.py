#!/usr/bin/env python 
# -*- coding: utf-8-*-

"""
Usage: Check if Swissprot protein has multiple gene parents like IFNA1 and IFNA13
"""

import gzip
import json


def process_sp():

    sp_fn = "/data/bel_resources/data/namespaces/sp_hmrz.jsonl.gz"
    f = gzip.open(sp_fn)
    next(f)
    for line in f:
        r = json.loads(line)
        if r["term"]["species_id"] != "TAX:9606":
            continue
        equivalences = r["term"]["equivalences"]
        eg_counts = len([e for e in equivalences if e.startswith('EG')])
        hgnc_counts = len([e for e in equivalences if e.startswith('HGNC')])
        if eg_counts > 1 or hgnc_counts > 1:
            print(json.dumps(r, indent=4))


def main():
    process_sp()


if __name__ == '__main__':
   main()
