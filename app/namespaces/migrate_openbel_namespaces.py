#!/usr/bin/env python 
# -*- coding: utf-8-*-

"""
Usage: $ {1: program}.py

Migrate namespace files created from OpenBEL namespace/annotation files to BEP8 format
"""

import gzip
import json
import app.settings as settings
import shutil
import copy
from app.common.text import quote_id

files = [
    "AFFX_belns.jsonl.gz",
    "CLO_belanno.jsonl.gz",
    "CL_belanno.jsonl.gz",
    "ECO_belanno.jsonl.gz",
    "EFO_belanno.jsonl.gz",
    "SCHEM_belns.jsonl.gz",
    "SCOMP_belns.jsonl.gz",
    "SDIS_belns.jsonl.gz",
    "SFAM_belns.jsonl.gz",
    "UBERON_belanno.jsonl.gz",
]


def migrate_file(fn):

    fp = f"{settings.DATA_DIR}/namespaces/{fn}"
    fp_old = f"{settings.DATA_DIR}/namespaces/old/{fn}"
    
    new_list = []
    with gzip.open(fp_old, 'rt') as f:
        for line in f:
            r = json.loads(line)
            n = {"term": {}}
            if 'term' in r:
                namespace = r["term"]["namespace"]

                label = r["term"]["namespace_value"]
                if r["term"].get("src_id", False):
                    id = r["term"]["src_id"]
                else:
                    id = label

                n['term']['key'] = f"{r['term']['namespace']}:{quote_id(id)}"
                n["term"]["id"] = id
                n["term"]["namespace"] = namespace
                
                if id != label:
                    n["term"]["label"] = label

                n["term"]["name"] = r["term"].get("name", "")
                n["term"]["annotation_types"] = r["term"].get("annotation_types", [])
                n["term"]["entity_types"] = r["term"].get("entity_types", [])

                new_list.append(copy.deepcopy(n))

            elif 'metadata' in r:
                r["metadata"].pop("src_url", "")
                r["metadata"]["source_url"] = ""
                new_list.append(copy.deepcopy(r))

    with gzip.open(fp, 'wt') as f:
        for line in new_list:
            f.write("{}\n".format(json.dumps(line)))


def main():
    
    for fn in files:
        migrate_file(fn)


if __name__ == '__main__':
   main()
