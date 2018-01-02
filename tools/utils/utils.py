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
import requests
from dateutil import parser

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
    """ Get Web file only if last modified header is more than given days_old or if local file older than remote file

    Args:
        url (str): file url
        lfile (str): local file path
        days_old (int): how many days old local file is before re-downloading
        gzip_flag (bool): gzip downloaded file, default False
        force (boolean): whether to force downloading file even if it's not newer than already downloaded file

    Returns:
        (boolean, str): tuple with success for get and a message with result information
    """

    need_download = False
    rmod_date = None
    lmod_date = None

    if not os.path.exists(lfile) or force:  # local file doesn't exist or force is set - download needed
        need_download = True
    else:  # local file exists AND not forced, so check the remote counterpart for their last modified time and compare
        try:
            r = requests.get(url)
            last_modified = r.headers['Last-Modified']
            rmod_date_parsed = parser.parse(last_modified)
            rmod_date_local = rmod_date_parsed.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)
            rmod_date = rmod_date_local.strftime('%Y%m%d')
        except requests.ConnectionError:
            log.warning('Cannot connect to the given URL.')
            print('Cannot connect to the given URL.')
        except KeyError:
            log.warning('The request does not have a last modified header.')
            print('The request does not have a last modified header.')
        finally:
            local_file_mtime_ts = os.path.getmtime(lfile)
            lmod_date = timestamp_to_date(local_file_mtime_ts)

    if not need_download:  # still not sure whether to download or not - need to check/compare rmod date and lmod date
        if rmod_date is None:  # if the remote file modified date cannot be found, compare with the days_old variable
            check_date = (datetime.datetime.now() - datetime.timedelta(days=days_old)).strftime("%Y%m%d")
            if lmod_date > check_date:
                msg = f'{lfile} < {days_old} days old; will not re-download (remote file mtime unavailable).'
                log.warning(msg)
                return False, msg
            else:
                need_download = True
        if rmod_date > lmod_date:
            need_download = True

    if need_download:

        file_open_fn = gzip.open if gzip_flag else open
        file_name = f'{lfile}.gz' if gzip_flag else lfile

        with urllib.request.urlopen(url) as response, file_open_fn(file_name, 'wb') as out_file:
            shutil.copyfileobj(response, out_file)

        msg = f'Remote file downloaded as {file_name}.'
        return True, msg
    else:
        msg = f'No download needed; remote file is not newer than local file {lfile}.'
        log.warning(msg)
        return False, msg


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
        if reply_code == 213:  # 213 code denotes a successful usage of MDTM, and is followed by the timestamp
            rmod_date = reply[1][:8]  # we only need the first 8 digits of timestamp: YYYYMMDD - discard HHMMSS

        if not force:
            if lmod_date >= rmod_date:
                return False, 'Remote file is not newer than local file'

    except Exception as e:
        log.warning(f'{e}: Cannot get file mod date by sending MDTM command.')
        check_date = (datetime.datetime.now() - datetime.timedelta(days=days_old)).strftime("%Y%m%d")

        if lmod_date > check_date:
            log.warning(f"{lfile} < week old - won't retrieve, filemod date unavailable")
            return False, f"{lfile} < week old - won't retrieve, filemod date unavailable"

    # use gzip's open() if gzip flag is set else use the python built-in open()
    file_open_function = gzip.open if gzip_flag else open
    file_name = f'{lfile}.gz' if gzip_flag else lfile

    with file_open_function(file_name, mode='wb') as f:
        try:
            print(f'Downloading {filename}...')
            ftp.retrbinary(f'RETR {filename}', f.write)
            ftp.quit()
            msg = f'Downloaded {filename}'
            return True, msg
        except Exception as e:
            ftp.quit()
            error = f'Could not download {filename}: {e}'
            log.error(error)
            return False, error


def get_newest_version_filename(regex: str, server_host: str, server_path: str, group_num: int) -> str:
    """Get the name of the first file matching the regex string at the specified FTP server directory

        Args:
            regex (str): regex string to match
            server_host (str): ftp server name
            server_path (str): remote file path
            group_num (int): regex group to match and return

        Returns:
            str: string that matches the group specified in the regex; could be version number or any other info wanted
        """
    ftp = ftplib.FTP(host=server_host)
    ftp.login()
    ftp.cwd(server_path)

    files = ftp.nlst()

    for f in files:  # for each file, see if regex matches. if matches, return this file.
        reg_match = re.match(regex, f)
        if reg_match:
            try:
                grouped_string = reg_match.group(group_num)
                return grouped_string
            except Exception as e:
                continue

    return ''


def main():

    # res = file_newer('./data/terms/hgnc.json', './downloads/hgnc_complete_set.json')
    # print(res)

    test_url = 'https://stackoverflow.com/questions/19979518/what-is-pythons-heapq-module'
    r = get_web_file(test_url, './downloads/test2.html', gzip_flag=True)
    print(r)

    # server = 'ftp.ncbi.nih.gov'
    # rfile = '/pub/taxonomy/taxdump.tar.gz'
    # lfile = './downloads/taxdump.tar.gz'
    #
    # result = get_ftp_file(server, rfile, lfile)
    # print(result)


if __name__ == '__main__':
    main()
