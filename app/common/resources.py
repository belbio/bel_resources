import datetime
import gzip
import json

import app.settings as settings
from app.common.text import dt_now
from app.schemas.main import Namespace, Term


def get_metadata(namespace_def, version: str = None) -> dict:
    """Get namespace metadata"""

    # Setup metadata info - mostly captured from namespace definition file which
    # can be overridden in belbio_conf.yml file

    if not version:
        version = dt_now()

    metadata = Namespace(
        name=namespace_def["name"],
        namespace=namespace_def["namespace"],
        description=namespace_def["description"],
        resource_type="namespace",
        namespace_type=namespace_def["namespace_type"],
        version=version,
        source_name=namespace_def["source_name"],
        source_url=namespace_def["source_url"],
        template_url=namespace_def.get("template_url", ""),
        example_url=namespace_def.get("example_url", None),
        identifiers_org=namespace_def.get("identifiers_org", False),
        identifiers_org_namespace=namespace_def.get("identifiers_org_namespace", ""),
    )

    if namespace_def.get("entity_types", False):
        metadata.entity_types = namespace_def["entity_types"]

    if namespace_def.get("annotation_types", False):
        metadata.annotation_types = namespace_def["annotation_types"]

    return metadata.dict(skip_defaults=True)


def get_species_labels():
    """Get species labels with overrides from TAXONOMY_LABELS setting"""

    species_labels_fn = f"{settings.DATA_DIR}/namespaces/tax_labels.json.gz"
    with gzip.open(species_labels_fn, "r") as fi:
        species_labels = json.load(fi)

    species_labels.update(settings.TAXONOMY_LABELS)

    return species_labels
