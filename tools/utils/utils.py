#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  program.py <customer>

"""

import ftplib
import urllib.request
import os
import re
import gzip
import shutil
import datetime
import yaml
from typing import Tuple, Mapping, Any

import logging
log = logging.getLogger(__name__)

# Automatically-creating-directories-with-file-output
# https://stackoverflow.com/questions/12517451/automatically-creating-directories-with-file-output
# os.makedirs(os.path.dirname(filename), exist_ok=True)


# from ftpsync.targets import FsTarget
# from ftpsync.ftp_target import FtpTarget
# from ftpsync.synchronizers import DownloadSynchronizer


# def sync_data():
#     """Synchronize data from remote ftp server"""

#     local = FsTarget("../downloads")
#     remote = FtpTarget("/pub/taxonomy/taxdump.tar.gz", "ftp.ncbi.nih.gov")
#     opts = {"force": False, "delete_unmatched": False, "verbose": 3, "dry_run": True}
#     s = DownloadSynchronizer(local, remote, opts)
#     s.run()


def get_namespace(prefix: str, config: Mapping[str, Any]) -> Mapping[str, Any]:
    """Get namespace info

    Args:
        prefix (str): prefix or key of namespaces.yml file

    Returns:
        Mapping[str, Any]: namespace information as dictionary
    """

    namespaces_fn = config['bel_resources']['file_locations']['namespaces_definition']
    with open(namespaces_fn, 'r') as f:
        namespaces = yaml.load(f)

    return namespaces[prefix]


def get_prefixed_id(ns_prefix, term_id):
    """Prepend namespace prefix on id adding quotes if necessary"""

    if re.search('[),\s]', term_id):  # only if it contains whitespace, comma or ')'
        return f'{ns_prefix}:"{term_id}"'
    else:
        return f'{ns_prefix}:{term_id}'


def needs_quotes(namespace_value: str) -> bool:
    """Check if we need quotes around namespace value string"""

    if re.search('[),\s]', namespace_value):  # only if it contains whitespace, comma or ')'
        return True
    return False


def lowercase_term_id(term_id: str) -> str:
    """Lowercase the term value (not the namespace prefix)

    Args:
        term_id (str): term identifier with namespace prefix, e.g. MESH:Atherosclerosis

    Returns:
        str: lowercased, e.g. MESH:atherosclerosis
    """
    (ns, val) = term_id.split(':', maxsplit=1)
    term_id = f'{ns}:{val.lower()}'

    return term_id


def timestamp_to_date(ts: int) -> str:
    """Convert system timestamp to date string YYYMMDD"""

    fmt = "%Y%m%d"
    return datetime.datetime.fromtimestamp(ts).strftime(fmt)


def file_newer(check_file: str, base_file: str) -> bool:
    """Is check_file newer than base_file?

    Args:
        check_file (str): file to check
        base_file (str): file to compare against

    Returns:
        bool: True if file is newer
    """
    if os.path.isfile(check_file):
        cf_modtime_ts = os.path.getmtime(check_file)
        bf_modtime_ts = os.path.getmtime(base_file)
    else:
        return False

    return cf_modtime_ts > bf_modtime_ts


def get_web_file(url: str, lfile: str, days_old: int = 7, gzip_flag: bool = False, force: bool = False) -> Tuple[bool, str]:
    """ Get Web file only if

    Args:
        url (str): file url
        lfile (str): local file path
        days_old (int): how many days old local file is before re-downloading
        gzip_flag (bool): gzip downloaded file, default False
        force (boolean): whether to force downloading file even if it's not newer than already downloaded file

    Returns:
        (boolean, str): tuple with success for get and a message with result information
    """

    lmod_date = "19000101"
    if os.path.exists(lfile) and not force:
        modtime_ts = os.path.getmtime(lfile)
        lmod_date = timestamp_to_date(modtime_ts)

        check_date = (datetime.datetime.now() - datetime.timedelta(days=days_old)).strftime("%Y%m%d")

        if lmod_date > check_date:
            log.warning(f"{lfile} < week old - won't retrieve, filemod date unavailable")
            return (False, f"{lfile} < week old - won't retrieve, filemod date unavailable")

    # Download the file from `url` and save it locally under `file_name`:
    if gzip_flag:
        with urllib.request.urlopen(url) as response, gzip.open(lfile, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
            return True, response

    else:
        with urllib.request.urlopen(url).geturl() as response, open(lfile, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
            return True, response.status

    return False, 'Could not download file'


def get_ftp_file(server: str, rfile: str, lfile: str, days_old: int = 7, gzip_flag: bool = False, force: bool = False) -> Tuple[bool, str]:
    """Get FTP file only if newer than already downloaded file

    Args:
        server (str): ftp server name
        rfile (str): remote file path
        lfile (str): local file path
        days_old (int): how many days old local file is before re-downloading - only used if can't determine remote file mod date
        gzip_flag (bool): gzip downloaded file, default False
        force (bool): whether to force downloading file even if it's not newer than already downloaded file

    Returns:
        (boolean, str): tuple with success for get and a message with result information
    """

    path = os.path.dirname(rfile)

    filename = os.path.basename(rfile)

    lmod_date = "19000101"
    if os.path.exists(lfile):
        modtime_ts = os.path.getmtime(lfile)
        lmod_date = timestamp_to_date(modtime_ts)

    ftp = ftplib.FTP(host=server)
    ftp.login()
    ftp.cwd(path)

    # Only download file if it's newer than what is saved
    rmod_date = "19010101"
    try:
        reply = str(ftp.sendcmd('MDTM ' + filename)).split()
        reply_code = int(reply[0])
        if reply_code == 213:
            rmod_date = reply[1][:8]

        if not force:
            if lmod_date >= rmod_date:
                return True, 'Remote file is not newer than local file'

    except Exception as e:
        log.warning(f'Cannot get file mod date by sending MDTM command.')
        check_date = (datetime.datetime.now() - datetime.timedelta(days=days_old)).strftime("%Y%m%d")

        if lmod_date > check_date:
            log.warning(f"{lfile} < week old - won't retrieve, filemod date unavailable")
            return True, f"{lfile} < week old - won't retrieve, filemod date unavailable"

    # use gzip's open() if gzip flag is set else use the python built-in open()
    file_open_function = gzip.open if gzip_flag else open

    with file_open_function(lfile, mode='wb') as f:
        try:
            ftp.retrbinary(f'RETR {filename}', f.write)
            ftp.quit()
            return True, f'Downloaded {filename}'
        except Exception as e:
            ftp.quit()
            log.error(f'Could not download {filename}')
            return False, f'Error downloading file: {e}'


def main():

    res = file_newer('./data/terms/hgnc.json', './downloads/hgnc_complete_set.json')
    # print(res)

    server = 'ftp.ncbi.nih.gov'
    rfile = '/pub/taxonomy/taxdump.tar.gz'
    lfile = './downloads/taxdump.tar.gz'

    # server = 'ftp.ebi.ac.uk'
    # rfile = '/pub/databases/chembl/ChEMBLdb/latest/chembl_23_sqlite.tar.gz'
    # lfile = './downloads/chembl_23_sqlite.tar.gz'
    #
    # server = 'ftp.ebi.ac.uk'
    # rfile = '/pub/databases/chebi/ontology/chebi.obo.gz'
    # lfile = './downloads/chebi.obo.gz'
    #
    # server = 'ftp.uniprot.org'
    # rfile = '/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.dat.gz'
    # lfile = './downloads/uniprot_sprot.dat.gz'

    result = get_ftp_file(server, rfile, lfile, force=True)
    print(result)


if __name__ == '__main__':
    main()
