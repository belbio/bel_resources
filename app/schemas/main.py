import enum
from typing import Any, List, Mapping, Optional, Union

from pydantic import BaseModel, Field, HttpUrl

Key = str  # namespace:id


class EntityTypesEnum(str, enum.Enum):
    """Namespace Entity types"""

    Abundance = "Abundance"
    Protein = "Protein"
    RNA = "RNA"
    Micro_RNA = "Micro_RNA"
    Gene = "Gene"
    Complex = "Complex"
    BiologicalProcess = "BiologicalProcess"
    Pathology = "Pathology"
    Activity = "Activity"
    Variant = "Variant"
    ProteinModification = "ProteinModification"
    AminoAcid = "AminoAcid"
    Location = "Location"
    Species = "Species"
    All = "All"


class AnnotationTypesEnum(str, enum.Enum):
    """Namespace entity annotation types"""

    Anatomy = "Anatomy"
    Cell = "Cell"
    CellLine = "CellLine"
    CellStructure = "CellStructure"
    Disease = "Disease"
    Species = "Species"
    All = "All"


class Entity(BaseModel):
    """Namespace entity record"""

    namespace: str
    id: str
    label: str
    key: Key = Field(..., description="Primary key (e.g. ns:id) of this entity")

    name: str
    description: Optional[str]

    species_id: Optional[str]
    species_label: Optional[str]

    entity_types: Optional[List[EntityTypesEnum]]
    annotation_types: Optional[List[AnnotationTypesEnum]]

    alt_keys: List[Key] = Field([], description="Alternate Keys for Entity")
    equivalences: List[Key] = Field([], description="Equivalences in other namespaces")
    children: List[Key] = Field([], description="Child entities of this entity using primary key")
    obsolete_ids: List[Key] = Field([], description="Obsolete IDs for this entity")

    synonyms: List[str] = Field([], description="Synonyms of this entity")


class ResourceTypesEnum(str, enum.Enum):
    """Resource Types"""

    namespace = "namespace"
    orthologs = "orthologs"
    nanopubs = "nanopubs"


class ResourceMetadata(BaseModel):
    """Resource file metadata"""

    name: str = Field(..., description="Name of resource file")
    resource_type: ResourceTypesEnum
    description: str = ""
    version: str = ""
    source_name: str = ""
    source_url: str = ""
    template_url: Optional[str]


# Needs to stay synced with bel.schemas.terms.Namespace
class Namespace(BaseModel):
    """Namespace Info"""

    name: str
    namespace: str = Field("", description="Namespace prefix - such as EG for EntrezGene")
    description: str = Field("", description="Namespace description")

    resource_type: str = "namespace"  # needed to distinguish BEL resource types
    namespace_type: str = Field(
        ...,
        description="[complete, virtual, identifiers_org] - complete type contains individual term records and is full-featured, virtual types only have [id_regex, species_key, annotation_types, entity_types] if available or if enabled - just the basic info from identifiers.org and defaults to all annotation and entity types",
    )

    version: Optional[str] = None

    source_name: str = Field("", description="Source name for namespace")
    source_url: HttpUrl = Field("", description="Source url for namespace")

    entity_types: List[EntityTypesEnum] = []
    annotation_types: List[AnnotationTypesEnum] = []
    species_key: Key = Field("", description="Species key for this namespace")

    id_regex: str = Field(
        "", description="If identifiers_org=True, get id_regex from identifiers_org"
    )

    template_url: str = Field(
        "",
        description="Url template for terms - replace the {$id} with the namespace id for the term url",
    )
    example_url: Optional[HttpUrl] = None

    # Use url = https://registry.api.identifiers.org/restApi/namespaces/search/findByPrefix?prefix=<namespace>, e.g taxonomy or reactome
    identifiers_org: bool = Field(
        False,
        description="Identifiers.org namespace - if True - this is only a namespace definition without term records",
    )
    identifiers_org_namespace: str = Field("")


class Term(BaseModel):
    """Namespace term record"""

    key: str = Field("", description="Namespace:ID of term")
    namespace: str = ""
    id: str = ""
    label: str = ""

    name: str = ""
    description: str = ""
    synonyms: List[str] = []

    alt_keys: List[Key] = Field(
        [],
        description="Create Alt ID nodes/equivalence_keys (to support other database equivalences using non-preferred Namespace IDs)",
    )
    child_keys: List[Key] = Field(
        [], description="Some hierarchical namespaces only track child terms"
    )
    parent_keys: List[Key] = Field(
        [], description="Some hierarchical namespaces only track parent terms"
    )
    obsolete_keys: List[Key] = []
    equivalence_keys: List[Key] = []

    species_key: Key = ""
    species_label: str = ""

    entity_types: List[EntityTypesEnum] = []
    annotation_types: List[AnnotationTypesEnum] = []


class Orthologs(BaseModel):
    """Ortholog equivalences - subject and object arbitrarily assigned by lexical ordering"""

    subject_key: Key
    subject_species_key: Key
    object_key: Key
    object_species_key: Key
