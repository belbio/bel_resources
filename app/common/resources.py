import datetime
import gzip
import json

import app.settings as settings
from app.common.text import dt_now
from app.schemas.main import ResourceMetadata, Term


def get_metadata(namespace_def, version: str = None):
    """Get namespace metadata"""

    # Setup metadata info - mostly captured from namespace definition file which
    # can be overridden in belbio_conf.yml file

    if not version:
        version = dt_now()

    metadata = ResourceMetadata(
        name=namespace_def["namespace"],
        type="namespace",
        namespace=namespace_def["namespace"],
        description=namespace_def["description"],
        version=version,
        source_url=namespace_def["source_url"],
        template_url=namespace_def["template_url"],
    )

    return metadata.dict()


def get_orthologs_metadata(namespace_def, version: str = None):
    """Get ortholgo metadata"""

    # Setup metadata info - mostly captured from namespace definition file which
    # can be overridden in belbio_conf.yml file

    if not version:
        version = dt_now()

    metadata = ResourceMetadata(
        name=namespace_def["namespace"],
        type="orthologs",
        description=namespace_def["description"],
        version=version,
        source_url=namespace_def["src_url"],

    )

    return metadata.dict()


def get_species_labels():
    """Get species labels with overrides from TAXONOMY_LABELS setting"""

    species_labels_fn = f"{settings.DATA_DIR}/namespaces/tax_labels.json.gz"
    with gzip.open(species_labels_fn, "r") as fi:
        species_labels = json.load(fi)

    species_labels.update(settings.TAXONOMY_LABELS)

    return species_labels
