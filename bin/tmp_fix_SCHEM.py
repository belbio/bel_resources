#!/usr/bin/env python3
# -*- coding: utf-8-*-

"""
Usage: $ {1: program}.py
"""

old_fn = "/data/bel_resources/archive/data/namespaces/SCOMP_belns.jsonl"
new_fn = "/data/bel_resources/resources_v2/namespaces/SCOMP_belns.jsonl"
updated_fn = "/data/bel_resources/resources_v2/namespaces/SCOMP_belns_updated.jsonl"
import json

equivalences = {}
with open(old_fn, "r") as f:
    for line in f:
        doc = json.loads(line)
        if doc.get("term", False) and "equivalences" in doc["term"]:
            tid = doc["term"]["id"]
            equivalences[tid] = doc["term"]["equivalences"]


with open(new_fn, "r") as fin, open(updated_fn, "w") as fout:
    for line in fin:
        doc = json.loads(line)
        if "term" in doc:
            tid = doc["term"]["key"]
            if tid in equivalences:
                doc["term"]["equivalence_keys"] = equivalences[tid]

        fout.write(f"{json.dumps(doc)}\n")
