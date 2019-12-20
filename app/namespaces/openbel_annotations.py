#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  program.py <customer>

"""
import copy
import gzip
import json
import logging
import os.path
import re
import sys

import yaml

import app.settings as settings
import app.utils as utils

openbel_annotation_sources = {
    "Anatomy": "http://resources.openbel.org/belframework/20150611/annotation/anatomy.belanno",
    "Cell": "http://resources.openbel.org/belframework/20150611/annotation/cell.belanno",
    "CellLine": "http://resources.openbel.org/belframework/20150611/annotation/cell-line.belanno",
    "Eco": "http://belief-demo.scai.fraunhofer.de/openbel/repository/annotation/evidence.belanno",
}


def update_data_files() -> bool:

    annofiles = []

    for key in openbel_annotation_sources:
        url = openbel_annotation_sources[key]
        basename = os.path.basename(url)

        if not re.search(
            ".gz$", basename
        ):  # we basically gzip everything retrieved that isn't already gzipped
            basename = f"{basename}.gz"

        local_fn = f"{settings.DOWNLOAD_DIR}/{basename}"

        annofiles.append((local_fn, url))

        (changed, msg) = utils.get_web_file(url, local_fn, days_old=settings.UPDATE_CYCLE_DAYS)
        log.info(msg)

    return annofiles


def add_metadata():

    metadata = {
        "Evidence and Conclusion Ontology": {"namespace": "ECO", "version": "20160414",},
        "Cell Line Ontology (CLO)": {"namespace": "CLO", "version": "2163",},
        "Experimental Factor Ontology (EFO)": {"namespace": "EFO", "version": "260",},
        "Uberon": {"namespace": "UBERON", "version": "2015-05-25",},
        "Cell Ontology (CL)": {"namespace": "CL", "version": "2015-05-12",},
    }

    return metadata


def read_annofile(annofile):

    anno = {"Values": []}
    with gzip.open(annofile, "rt") as fi:
        for line in fi:
            section_match = re.match("^\[(\w+)\]", line)
            keyval_match = re.match("^(\w+?)=(.*)$", line)
            blank_match = re.match("^\s*$", line)
            val_match = re.match("^([\w\_\d].*\|.*)", line)
            if section_match:
                section = section_match.group(1)
                if section != "Values":
                    anno[section] = {}

            elif keyval_match:
                key = keyval_match.group(1)
                val = keyval_match.group(2)
                if key not in anno[section]:
                    anno[section][key] = []
                anno[section][key].append(val)

            elif blank_match:
                pass

            elif val_match:
                val = val_match.group(1)
                (value, vid) = val.split("|")
                vid = vid.replace("_", ":")

                # ns_match = re.match('^([A-Z]+)_(.*)$', vid)
                # if ns_match:
                #     namespace = ns_match.group(1)
                #     _id = ns_match.group(2)

                anno[section].append({"id": f"{vid}", "value": value})

    return anno


def build_json(annofiles):

    additional_metadata = add_metadata()

    for annofile, anno_src_url in annofiles:
        print("Processing belanno", os.path.basename(annofile))

        anno_dict = read_annofile(annofile)

        idx_len = len(anno_dict["Citation"]["NameString"])

        annotation_type = anno_dict["AnnotationDefinition"]["Keyword"][0]
        for idx in range(idx_len):

            metadata = {}
            namespace = additional_metadata[anno_dict["Citation"]["NameString"][idx]]["namespace"]
            version = additional_metadata[anno_dict["Citation"]["NameString"][idx]]["version"]
            terms_filename = f"{settings.DATA_DIR}/namespaces/{namespace}_belanno.jsonl.gz"
            metadata = {
                "name": anno_dict["Citation"]["NameString"][idx],
                "type": "namespace",
                "namespace": namespace,
                "description": f"{anno_dict['Citation']['DescriptionString'][idx]}. NOTE: Converted from OpenBEL belanno file {anno_src_url}",
                "src_url": anno_dict["Citation"]["ReferenceURL"][idx],
                "version": version,
            }

            terms = []
            for value in anno_dict["Values"]:
                if re.match(f"^{namespace}", value["id"]):
                    vid = value["id"]
                    val = value["value"]
                    src_id = vid.replace(f"{namespace}:", "")
                    term = {
                        "namespace": namespace,
                        "namespace_value": val,
                        "src_id": src_id,
                        "id": utils.get_prefixed_id(namespace, val),
                        "label": val,
                        "name": val,
                        "annotation_types": [annotation_type],
                        "entity_types": [],
                        "alt_ids": [vid],
                    }
                    if annotation_type == "Cell":
                        term["entity_types"].append(annotation_type)
                    terms.append(copy.deepcopy(term))

            with gzip.open(terms_filename, "wt") as fo:
                fo.write("{}\n".format(json.dumps({"metadata": metadata})))

                for term in terms:
                    fo.write("{}\n".format(json.dumps({"term": term})))


def main():
    annofiles = update_data_files()
    build_json(annofiles)


if __name__ == "__main__":
    # Setup logging
    global log
    module_fn = os.path.basename(__file__)
    module_fn = module_fn.replace(".py", "")

    log = logging.getLogger(f"__name__")

    main()
