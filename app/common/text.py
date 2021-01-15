import datetime
import re


def strip_quotes(string: str) -> str:
    """Remove surrounding quotes and escape any internal double quotes"""

    # Remove quotes if exist
    match = re.match(r'\s*"(.*)"\s*$', string)
    if match:
        string = match.group(1)

    string = string.strip()  # remove external whitespace
    string = string.replace('"', '"')  # quote internal double quotes

    return string


def quote_id(string):
    """Quote ID"""

    if re.search(r"[),\!\s]", string):
        return f'"{string}"'
    else:
        return string


def timestamp_to_date(ts: int) -> str:
    """Convert system timestamp to date string YYYMMDD"""

    fmt = "%Y%m%d"
    return datetime.datetime.fromtimestamp(ts).strftime(fmt)


def dt_now() -> str:
    """Now as datetime string, e.g. 2020-06-15T14:30:37"""

    return datetime.datetime.now().replace(microsecond=0).isoformat()
