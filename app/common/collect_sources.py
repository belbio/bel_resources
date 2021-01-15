import datetime
import ftplib
import gzip
import os
import pathlib
import re
import shutil
import urllib.request
from pathlib import Path
from typing import Any, Mapping, Tuple
from urllib.parse import urlparse

import requests
import structlog
import yaml
from dateutil import parser

import app.settings as settings
from app.common.text import timestamp_to_date

log = structlog.get_logger()


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


def get_web_file(
    url: str,
    download_fn: str,
    days_old: int = settings.UPDATE_CYCLE_DAYS,
    force_download: bool = False,
) -> Tuple[bool, str]:
    """ Get Web file only if last modified header is more than given days_old or if local file older than remote file

    Args:
        url (str): file url
        lfile (str): local file path
        days_old (int): how many days old local file is before re-downloading
        force (boolean): whether to force downloading file even if it's not newer than already downloaded file

    Returns:
        (boolean, str): tuple with success for get and a message with result information
    """

    need_download = False
    rmod_date = None
    lmod_date = None

    if (
        not os.path.exists(download_fn) or force_download
    ):  # local file doesn't exist or force is set - download needed
        need_download = True
    else:  # local file exists AND not forced, so check the remote counterpart for their last modified time and compare
        try:
            r = requests.get(url)
            last_modified = r.headers.get("Last-Modified", False)
            if last_modified:
                rmod_date_parsed = parser.parse(last_modified)
                rmod_date_local = rmod_date_parsed.replace(tzinfo=datetime.timezone.utc).astimezone(
                    tz=None
                )
                rmod_date = rmod_date_local.strftime("%Y%m%d")
        except requests.ConnectionError:
            log.warning("Cannot connect to the given URL.")
        finally:
            local_file_mtime_ts = os.path.getmtime(download_fn)
            lmod_date = timestamp_to_date(local_file_mtime_ts)

    if (
        not need_download
    ):  # still not sure whether to download or not - need to check/compare rmod date and lmod date
        if (
            rmod_date is None
        ):  # if the remote file modified date cannot be found, compare with the days_old variable
            check_date = (datetime.datetime.now() - datetime.timedelta(days=days_old)).strftime(
                "%Y%m%d"
            )
            if lmod_date > check_date:
                msg = f"{download_fn} < {days_old} days old; will not re-download (remote file mtime unavailable)."
                return False, msg
            else:
                need_download = True
        elif rmod_date > lmod_date:
            need_download = True

    if need_download:

        if not re.search("\.gz$", url):
            file_open_fn = gzip.open
        else:
            file_open_fn = open

        with urllib.request.urlopen(url) as response, file_open_fn(download_fn, "wb") as out_file:
            shutil.copyfileobj(response, out_file)

        msg = f"Remote file downloaded as {download_fn}."
        return True, msg
    else:
        msg = f"No download needed; remote file is not newer than local file {download_fn}."
        return False, msg


def get_ftp_file(
    url: str,
    download_fn: str,
    days_old: int = settings.UPDATE_CYCLE_DAYS,
    force_download: bool = False,
) -> Tuple[bool, str]:
    """Get FTP file only if newer than already downloaded file

    Args:
        url: ftpurl to download
        download_fn: local filename of remote source data file
        days_old (int): how many days old local file is before re-downloading - only used if can't determine remote file mod date
        force_download (bool): whether to force downloading file even if it's not newer than already downloaded file

    Returns:
        (changed, msg): tuple download filename and whether the file has been changed vs previous download
    """

    p = urlparse(url)
    host = p.hostname
    path_str = p.path
    path_obj = pathlib.Path(path_str)
    path_dir = path_obj.parent
    filename = path_obj.name

    compress_flag = False
    if not filename.endswith(".gz"):
        compress_flag = True

    local_fn_date = "19000101"
    if os.path.exists(download_fn):
        modtime_ts = os.path.getmtime(download_fn)
        local_fn_date = timestamp_to_date(modtime_ts)

    # Only download file if it's newer than what is saved
    rmod_date = "19010101"

    ftp = ftplib.FTP(host=host)
    try:
        ftp.login()

        ftp.cwd(str(path_dir))
        reply = str(ftp.sendcmd("MDTM " + filename)).split()
        reply_code = int(reply[0])
        if (
            reply_code == 213
        ):  # 213 code denotes a successful usage of MDTM, and is followed by the timestamp
            remote_mod_date = reply[1][
                :8
            ]  # we only need the first 8 digits of timestamp: YYYYMMDD - discard HHMMSS

        if local_fn_date >= remote_mod_date and not force_download:
            changed = False
            return (changed, "Remote file is not newer than local file")

        if compress_flag:
            file_open_fn = gzip.open
        else:
            file_open_fn = open

        # Retrieve and save file
        if compress_flag:
            with gzip.open(download_fn, "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)
        else:
            with open(download_fn, "wb") as f:
                ftp.retrbinary(f"RETR {filename}", f.write)

        msg = "Downloaded file"
        changed = True
        return (changed, msg)

    except Exception as e:
        now = datetime.datetime.now()
        check_date = (now - datetime.timedelta(days=days_old)).strftime("%Y%m%d")

        if local_fn_date > check_date:
            changed = False
            return (
                changed,
                f"{download_fn} < week old - won't retrieve, filemod date unavailable",
            )
        else:
            changed = False
            msg = f"Could not download file: {str(e)}"
            return (changed, msg)

    finally:
        ftp.quit()


def get_chembl_version(url) -> str:
    """Get the name of the first file matching the regex string at the specified FTP server directory

        Args:
            regex (str): regex string to match
            server_host (str): ftp server name
            server_path (str): remote file path
            group_num (int): regex group to match and return

        Returns:
            str: string that matches the group specified in the regex; could be version number or any other info wanted
    """

    p = urlparse(url)
    host = p.hostname
    path_str = p.path
    path_obj = pathlib.Path(path_str)
    path_dir = path_obj.parent
    filename = path_obj.name

    ftp = ftplib.FTP(host=host)
    ftp.login()
    ftp.cwd(path_str)

    files = ftp.nlst()

    for f in files:  # for each file, see if regex matches. if matches, return this file.
        print("F", f)
        matches = re.match("chembl_(\d+)_sqlite.tar.gz", f)
        if matches:
            return matches.group(1)

    return False


def get_mesh_version(url) -> str:
    """Get the name of the first file matching the regex string at the specified FTP server directory

        Args:
            regex (str): regex string to match
            server_host (str): ftp server name
            server_path (str): remote file path
            group_num (int): regex group to match and return

        Returns:
            str: string that matches the group specified in the regex; could be version number or any other info wanted
    """

    p = urlparse(url)
    host = p.hostname
    path_str = p.path
    path_obj = pathlib.Path(path_str)
    path_dir = path_obj.parent
    filename = path_obj.name

    ftp = ftplib.FTP(host=host)
    ftp.login()
    ftp.cwd(path_str)

    files = ftp.nlst()

    for f in files:  # for each file, see if regex matches. if matches, return this file.
        matches = re.match("d(\d+).bin", f)
        if matches:
            return matches.group(1)

    return False


def send_mail(mail_to: str, subject: str, msg: str, mail_from: str = settings.MAIL_FROM):
    """Send mail using MailGun API"""

    if not (settings.MAIL_API and settings.MAIL_API_KEY):
        return False

    data = {
        "to": mail_to,
        "from": mail_from,
        "subject": subject,
        "text": msg,
    }

    request = requests.post(
        f"{settings.MAIL_API}/messages", auth=("api", settings.MAIL_API_KEY), data=data,
    )
    return request
