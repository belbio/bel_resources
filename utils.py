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
import logging
import yaml
from typing import Tuple, Mapping, Any

log = logging.getLogger(__name__)

# Globals
namespaces_fn = '../namespaces.yaml'

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


def arango_id_to_key(_id):
    """Remove illegal chars from potential arangodb _key (id)

    Args:
        _id (str): id to be used as arangodb _key

    Returns:
        (str): _key value with illegal chars removed
    """

    key = re.sub("[^a-zA-Z0-9\_\-\:\.\@\(\)\+\,\=\;\$\!\*\'\%]+", '_', _id)
    if len(key) > 254:
        log.error(f'Arango _key cannot be longer than 254 chars: Len={len(key)}  Key: {key}')
    return key


def get_namespace(prefix: str) -> Mapping[str, Any]:
    """Get namespace info

    Args:
        prefix (str): prefix or key of namespaces.yaml file

    Returns:
        Mapping[str, Any]: namespace information as dictionary
    """
    with open(namespaces_fn, 'r') as f:
        namespaces = yaml.load(f)

    return namespaces[prefix]


def get_prefixed_id(ns_prefix, term_id):
    """Prepend namespace prefix on id adding quotes if necessary"""

    if ' ' in term_id:
        return f'{ns_prefix}:"{term_id}"'
    else:
        return f'{ns_prefix}:{term_id}'


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


def get_web_file(url: str, lfile: str, days_old: int = 7, gzipflag: bool = False, force: bool = False) -> Tuple[bool, str]:
    """ Get Web file only if

    Args:
        url (str): file url
        lfile (str): local file path
        days_old (int): how many days old local file is before re-downloading
        gzipflag (bool): gzip downloaded file, default False
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
            log.warning("Local source file is less than a week old - won't retrieve (can't get actual remote file mod date)")
            return (False, "Local file is less than a week old - won't retrieve (can't get actual remote file mod date)")

    # Download the file from `url` and save it locally under `file_name`:
    if gzipflag:
        with urllib.request.urlopen(url) as response, gzip.open(lfile, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
            return True, response

    else:
        with urllib.request.urlopen(url).geturl() as response, open(lfile, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)
            return True, response.status

    return False, 'Could not download file'


def get_ftp_file(server: str, rfile: str, lfile: str, days_old: int = 7, gzipflag: bool = False, force: bool = False) -> Tuple[bool, str]:
    """Get FTP file only if newer than already downloaded file

    Args:
        server (str): ftp server name
        rfile (str): remote file path
        lfile (str): local file path
        days_old (int): how many days old local file is before re-downloading - only used if can't determine remote file mod date
        gzipflag (bool): gzip downloaded file, default False
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
    #    depends on FTP server supporting MLSD command (RFC 3659)
    try:
        dirinfo = ftp.mlsd(facts=['size', 'modify'])

        for f in dirinfo:
            if f[0] == filename:
                rmod_date = f[1]['modify'][0:8]  # get only date portion of modification date
                break
        else:
            ftp.quit()
            return(False, 'File is missing')

        if not force:
            if lmod_date >= rmod_date:
                return (True, 'Remote file is not newer than local file')

    except Exception as e:
        log.warning(f'Cannot get dirinfo - no support for MLSD cmd - Error {e}')
        # log.error(f'Cannot get directory information on file modification date {e_resp}')
        check_date = (datetime.datetime.now() - datetime.timedelta(days=days_old)).strftime("%Y%m%d")

        if lmod_date > check_date:
            log.warning("Local source file is less than a week old - won't retrieve (can't get actual remote file mod date)")
            return (True, "Local file is less than a week old - won't retrieve (can't get actual remote file mod date)")

    if gzipflag:
        with gzip.open(lfile, mode='wb') as f:
            try:
                ftp.retrbinary(f'RETR {filename}', f.write)
                ftp.quit()
                return (True, f'Downloaded {filename}')
            except Exception as e:
                ftp.quit()
                return(False, f'Error downloading file: {e}')
    else:
        with open(lfile, mode='wb') as f:
            try:
                ftp.retrbinary(f'RETR {filename}', f.write)
                ftp.quit()
                return (True, f'Downloaded {filename}')
            except Exception as e:
                ftp.quit()
                return(False, f'Error downloading file: {e}')


def main():

    res = file_newer('./data/terms/hgnc.json', './downloads/hgnc_complete_set.json')
    print(res)
    quit()
    server = 'ftp.ncbi.nih.gov'
    rfile = '/pub/taxonomy/taxdump.tar.gz'
    lfile = './downloads/taxdump.tar.gz'
    result = get_ftp_file(server, rfile, lfile, force=True)
    print(result)


if __name__ == '__main__':
    main()

