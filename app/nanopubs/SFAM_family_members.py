#!/usr/bin/env python
# -*- coding: utf-8-*-

"""
Usage: $ {1: program}.py
"""

import json

fn = "/data/bel_resources/downloads/SFAM_protein_families.bel"
sfam_nanopub_fn = "/data/bel_resources/data/nanopubs/SFAM_protein_families.json"

file_metadata = {
    "metadata": {
        "Name": "OpenBEL SFAM hasMembers",
        "Description": "OpenBEL SFAM hasMember relationship assertions",
        "Version": "20150611",
        "Copyright": "Copyright (c) 2011-2015, Selventa. All Rights Reserved.",
        "Authors": "Selventa",
        "Licenses": "Creative Commons CC-BY License",
        "ContactInfo": "Unsupported",
    }
}

nanopub = {
    "nanopub": {
        "id": "SELV_SFAM",
        "schema_uri": "https://raw.githubusercontent.com/belbio/schemas/master/schemas/nanopub_bel-1.1.0.yaml",
        "type": {"name": "BEL", "version": "2.1.1"},
        "annotations": [{"type": "Species", "label": "human", "id": "TAX:9606"},],
        "citation": {
            "uri": "https://github.com/OpenBEL/openbel-framework-resources/blob/latest/resource/protein-families.bel"
        },
        "assertions": [],
        "metadata": {"collections": ["SFAM members"]},
    }
}


def process_members():

    with open(fn, "r") as f:
        for line in f:
            if not line.startswith("p(SFAM"):
                continue

            (s, o) = line.split(" hasMember ")

            nanopub["nanopub"]["assertions"].append(
                {"subject": s, "relation": "hasMember", "object": o}
            )

    with open(sfam_nanopub_fn, "w") as f:
        json.dump([file_metadata, nanopub], f, indent=4)


def main():
    process_members()


if __name__ == "__main__":
    main()
