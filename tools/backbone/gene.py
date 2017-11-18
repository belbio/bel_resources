#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  backbone.py <customer>

Generate backbone g() transcribedTo r() or m() and r() translatedTo p()

"""

import json
import gzip

datafile = '../data/terms/eg.jsonl.gz'
backbone_fn = '../data/backbone/gene_backbone.jsonl.gz'


def process_backbone():
    # count = 0
    with gzip.open(datafile, 'rt') as fi, gzip.open(backbone_fn, 'wt') as fo:

        for line in fi:
            term = json.loads(line)
            if 'terminology' in term:
                namespace = term['terminology']['namespace']
                continue

            # print(term)
            term_id = term['term']['id']
            # label = term['term']['label']
            species = term['term']['species']
            edges = []
            entity_types = term['term']['entity_types']
            if 'Protein' in entity_types:
                edges.append(
                    {
                        'subject': f'g({namespace}:{term_id})',
                        'relation': 'transcribedTo',
                        'object': f'r({namespace}:{term_id})',
                        # 'subject_lbl': f'g({label})',
                        # 'object_lbl': f'r({label})',
                        'species': f'TAX:{species}',
                    }
                )
                edges.append(
                    {
                        'subject': f'r({namespace}:{term_id})',
                        'relation': 'translatedTo',
                        'object': f'p({namespace}:{term_id})',
                        # 'subject_lbl': f'r({label})',
                        # 'object_lbl': f'p({label})',
                        'species': f'TAX:{species}',
                    }
                )

            elif 'RNA' in entity_types or 'Micro_RNA' in entity_types:
                edges.append(
                    {
                        'subject': f'g({namespace}:{term_id})',
                        'relation': 'transcribedTo',
                        'object': f'r({namespace}:{term_id})',
                        # 'subject_lbl': f'g({label})',
                        # 'object_lbl': f'r({label})',
                        'species': f'TAX:{species}',
                    }
                )

            for edge in edges:
                fo.write('{}\n'.format(json.dumps(edge)))
                # count += 1
                # if count > 10:
                #     quit()


def main():
    process_backbone()


if __name__ == '__main__':
    main()

