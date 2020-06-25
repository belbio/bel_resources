#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  gene2protein.py

Generate backbone edges g() transcribedTo r() and
                        m() and r() translatedTo p()
"""

import gzip
import json
from string import Template

import structlog

import app.settings as settings

log = structlog.getLogger(__name__)

bel_version = "2.1.1"

eg_datafile = f"{settings.DATA_DIR}/namespaces/eg.jsonl.gz"
backbone_fn = f"{settings.DATA_DIR}/backbone/eg_backbone_nanopubs.jsonl.gz"
backbone_hmrz_fn = f"{settings.DATA_DIR}/backbone/eg_backbone_nanopubs_hmrz.jsonl.gz"

hmrz_species = ["TAX:9606", "TAX:10090", "TAX:10116", "TAX:7955"]


def process_backbone():

    # count = 0
    with gzip.open(eg_datafile, "rt") as fi, gzip.open(backbone_fn, "wt") as fo, gzip.open(
        backbone_hmrz_fn, "wt"
    ) as fz:

        metadata = {}
        for line in fi:
            term = json.loads(line)
            if "metadata" in term:
                metadata = term["metadata"]
                print("Metadata", metadata)

                src_template_url = Template(metadata["template_url"])
                continue

            key = term["term"]["key"]
            species_key = term["term"]["species_key"]
            species_label = term["term"]["species_label"]

            assertions = []
            entity_types = term["term"]["entity_types"]
            if "Protein" in entity_types:
                assertions.append(
                    {"subject": f"g({key})", "relation": "transcribedTo", "object": f"r({key})",}
                )
                assertions.append(
                    {"subject": f"r({key})", "relation": "translatedTo", "object": f"p({key})",}
                )

            elif "RNA" in entity_types or "Micro_RNA" in entity_types:
                assertions.append(
                    {"subject": f"g({key})", "relation": "transcribedTo", "object": f"r({key})",}
                )
            else:
                continue

            nanopub = {
                "type": {"name": "BEL", "version": bel_version},
                "citation": {"uri": src_template_url.safe_substitute(id=term["term"]["id"])},
                "assertions": assertions,
                "annotations": [{"type": "Species", "id": species_key, "label": species_label}],
                "metadata": {"gd_status": "finalized", "nanopub_type": "backbone"},
            }

            fo.write(f'{{"nanopub": {json.dumps(nanopub)}}}\n')

            if species_key in hmrz_species:
                fz.write(f'{{"nanopub": {json.dumps(nanopub)}}}\n')


def main():
    process_backbone()


if __name__ == "__main__":
    main()
