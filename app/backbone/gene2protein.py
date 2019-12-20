#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  gene2protein.py

Generate backbone edges g() transcribedTo r() and
                        m() and r() translatedTo p()
"""

import gzip
import json

import app.settings as settings
import structlog

log = structlog.getLogger(__name__)

eg_datafile = f"{settings.DATA_DIR}/namespaces/eg.jsonl.gz"
backbone_fn = f"{settings.DATA_DIR}/backbone/eg_backbone_nanopubs.jsonl.gz"
backbone_hmrz_fn = f"{settings.DATA_DIR}/backbone/eg_backbone_nanopubs_hmrz.jsonl.gz"

hmrz_species = ["TAX:9606", "TAX:10090", "TAX:10116", "TAX:7955"]

src_url = "https://www.ncbi.nlm.nih.gov/gene/"


def process_backbone():

    # count = 0
    with gzip.open(eg_datafile, "rt") as fi, gzip.open(backbone_fn, "wt") as fo, gzip.open(
        backbone_hmrz_fn, "wt"
    ) as fz:

        for line in fi:
            term = json.loads(line)
            # Skip metadata record
            if "term" not in term:
                continue

            term_id = term["term"]["id"]
            src_id = term["term"]["src_id"]
            # label = term['term']['label']
            species = term["term"]["species_id"]
            species_label = term["term"]["species_label"]

            assertions = []
            entity_types = term["term"]["entity_types"]
            if "Protein" in entity_types:
                assertions.append(
                    {
                        "subject": f"g({term_id})",
                        "relation": "transcribedTo",
                        "object": f"r({term_id})",
                    }
                )
                assertions.append(
                    {
                        "subject": f"r({term_id})",
                        "relation": "translatedTo",
                        "object": f"p({term_id})",
                    }
                )

            elif "RNA" in entity_types or "Micro_RNA" in entity_types:
                assertions.append(
                    {
                        "subject": f"g({term_id})",
                        "relation": "transcribedTo",
                        "object": f"r({term_id})",
                    }
                )
            else:
                continue

            nanopub = {
                "type": {"name": "BEL", "version": "2.0.0"},
                "citation": {"uri": f"{src_url}{src_id}"},
                "assertions": assertions,
                "annotations": [{"type": "Species", "id": species, "label": species_label}],
                "metadata": {"gd_status": "finalized", "nanopub_type": "backbone"},
            }

            if species in hmrz_species:
                fz.write(f'{{"nanopub": {json.dumps(nanopub)}}}\n')

            fo.write(f'{{"nanopub": {json.dumps(nanopub)}}}\n')


def main():
    process_backbone()


if __name__ == "__main__":
    main()
