import inspect
import os

import yaml
from dotenv import load_dotenv

load_dotenv()

appdir = os.path.split(os.path.abspath(inspect.getfile(inspect.currentframe())))[0]
rootdir = os.path.split(appdir)[0]

RESOURCES_DIR = os.getenv("BELRES_RESOURCES_DIR", default=f"{rootdir}/resources")

DATA_DIR = os.getenv("BELRES_DATA_DIR")
DOWNLOAD_DIR = os.getenv("BELRES_DOWNLOAD_DIR")

UPDATE_CYCLE_DAYS = os.getenv("UPDATE_CYCLE_DAYS", default=7)

MAIL_API = os.getenv("BELRES_MAIL_API")
MAIL_API_KEY = os.getenv("BELRES_MAIL_API_KEY")
MAIL_FROM = os.getenv("BELRES_MAIL_FROM")
MAIL_NOTIFY_EMAIL = os.getenv("BELRES_NOTIFY_EMAIL")


# Computed Settings

NAMESPACE_DEFINITIONS = yaml.load(
    open(f"{RESOURCES_DIR}/namespaces.yml", "r").read(), Loader=yaml.SafeLoader
)

TAXONOMY_LABELS = yaml.load(
    open(f"{RESOURCES_DIR}/taxonomy_labels.yml", "r").read(), Loader=yaml.SafeLoader
)

# TODO Not currently used - need to update _hmrz subset to use this for the species_filter
SPECIES_FILTER = os.getenv("BELRES_SPECIES_FILTER", default=[])
if SPECIES_FILTER:
    SPECIES_FILTER = [species.strip() for species in SPECIES_FILTER.split(",")]
