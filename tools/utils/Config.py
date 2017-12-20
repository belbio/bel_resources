#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re

from bel_db.Config import config

# Enhance config dictionary with file_paths
#    following functions support this
def get_current_module_dir() -> str:
    """Get current module filepath"""

    cur_module_fp = os.path.dirname(os.path.realpath(__file__))
    return cur_module_fp


def get_root_filepath() -> str:
    """Get root filepath

    root filepath is 2 levels up from where this Config.py file is located
    """

    cmd = get_current_module_dir()
    bel_resources_root = os.path.dirname(os.path.dirname(cmd))

    return bel_resources_root


def add_filepaths(config):
    """Add filepaths to config dictionary for bel_resources"""

    root_fp = get_root_filepath()
    config['bel_resources']['file_locations']['root'] = root_fp

    for loc in config['bel_resources']['file_locations']:
        loc_val = config['bel_resources']['file_locations'][loc]
        if not re.match('/', loc_val):
            config['bel_resources']['file_locations'][loc] = f'{root_fp}/{loc_val}'

    return config


def get_config(config):

    config = add_filepaths(config)
    return config


def main():
    global config

    # get_version()
    # quit()

    config = get_config(config)
    import json
    print('DumpVar:\n', json.dumps(config, indent=4))


if __name__ == '__main__':
    main()

else:
    config = get_config(config)
