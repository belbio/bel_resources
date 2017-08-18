#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Usage:  program.py <customer>

"""

import ftplib
import os
import datetime
import logging
from typing import Tuple

log = logging.getLogger('utils')

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


def get_ftp_file(server: str, rfile: str, lfile: str, force: bool = False) -> Tuple[bool, str]:
    """Get FTP file only if newer than already downloaded file

    Args:
        server (str): ftp server name
        rfile (str): remote file path
        lfile (str): local file path
        force (boolean): whether to force downloading file even if it's not newer than already downloaded file

    Returns:
        (boolean, str): tuple with success for get and a message with result information
    """

    path = os.path.dirname(rfile)
    filename = os.path.basename(rfile)

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
        one_week_ago = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y%m%d")

        if lmod_date > one_week_ago:
            return (True, "Local file is less than a week old - can't get remote file mod date")

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

