import enum
from typing import Any, List, Mapping, Optional, Union

from pydantic import BaseModel, Field

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


class AnnotationTypesEnum(str, enum.Enum):
    """Namespace entity annotation types"""

    Anatomy = "Anatomy"
    Cell = "Cell"
    CellLine = "CellLine"
    CellStructure = "CellStructure"
    Disease = "Disease"
    Species = "Species"


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
    type: ResourceTypesEnum
    namespace: Optional[str]
    description: str = ""
    version: str = ""
    source_url: str = ""
    template_url: Optional[str]


class Term(BaseModel):
    """Namespace term record"""

    key: str = Field("", description="Namespace:ID of term")
    namespace: str = ""
    id: str = ""
    label: str = ""

    name: str = ""
    description: str = ""
    synonyms: List[str] = []

    alt_keys: List[Key] = Field([], description="Create Alt ID nodes/equivalence_keys (to support other database equivalences using non-preferred Namespace IDs)")
    child_keys: List[Key] = Field([], description="Some hierarchical namespaces only track child terms")
    parent_keys: List[Key] = Field([], description="Some hierarchical namespaces only track parent terms")
    obsolete_keys: List[Key] = []
    equivalence_keys: List[Key] = []

    species_key: Key = ""
    species_label: str = ""

    entity_types: List[EntityTypesEnum] = []
    annotation_types: List[AnnotationTypesEnum] = []
