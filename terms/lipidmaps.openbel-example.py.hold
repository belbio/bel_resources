#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Usage:  program.py <customer>

  program.py  <customer>
  program.py (-h | --help)
  program.py --version

Options:
  -h --help     Show help.
  --version     Show version.
"""
import re
import copy
import json
import csv

sdf_fn = 'LMSDFDownload28Jun15/LMSDFDownload28Jun15FinalAll.sdf'


def read_sdf():
    tmpd = {}
    db = {}
    keys = set()
    with open(sdf_fn, 'r') as f:
        for line in f:
            matchkeys = re.match('>\s<(.*?)>', line, flags=0)
            matchend = re.match('\$\$\$\$', line, flags=0)
            if matchkeys:
                # print('2 Line: ', line)
                key = matchkeys.group(1)
                tmpd[key] = []
                for line in f:
                    # print('1 Line: ', line)
                    if re.match('\s*$', line, flags=0):
                        break
                    # tmpd[key].append(line.strip())  # in case there are multiple lines per key
                    tmpd[key] = line.strip()

            if matchend:
                # print('3 Line: ', line)
                # print('DumpVar:\n', json.dumps(tmpd, indent=4))
                keys.update(tmpd.keys())
                db[tmpd['LM_ID']] = copy.copy(tmpd)
                tmpd = {}

                # break  # testing

    # Tested to see if there are multiple lines/values after each key '< <KEY>'
    # for did in db:
    #     for key in db[did]:
    #         if len(db[did][key]) > 1:
    #             print('Id: {}  Key: {}'.format(did, key))

    with open('test.json', 'w') as f:
        json.dump(db, f, indent=4)

    key_list = sorted(keys)

    with open('lipidmaps.csv', 'w') as csvfile:
        w = csv.writer(csvfile)
        w.writerow(key_list)
        for did in db:
            row = []
            for v in key_list:
                row.append(db[did].get(v, 'None'))
            w.writerow(row)


def main():
    read_sdf()


if __name__ == '__main__':
    main()

