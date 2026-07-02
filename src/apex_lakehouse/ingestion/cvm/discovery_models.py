from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Mapping
from urllib.parse import urlparse
from uuid import UUID, uuid4

from apex_lakehouse.time import Competence
from apex_lakehouse.types import JsonDict


class CvmDataset(str, Enum):
    INFORME_DIARIO = "informe_diario"
    CADASTRO_FUNDOS = "cadastro_fundos"
    PERFIL_MENSAL = "perfil_mensal"


class DiscoveryDecision(str, Enum):
    NEW = "new"
    CHANGED = "changed"
    ALREADY_KNOWN = "already_known"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class CvmSourceArtifact:
    dataset_name: CvmDataset
    source_url: str
    file_name: str
    discovered_at: datetime
    source_system: str = "cvm"
    competence: Competence | None = None
    business_date: date | None = None
    source_last_modified_at: datetime | None = None
    content_type: str | None = None
    metadata: JsonDict = field(default_factory=dict)

    @property
    def source_path(self) -> str:
        return urlparse(self.source_url).path
    
    @property
    def period_label(self) -> str | None:
        if self.competence is not None:
            return str(self.competence)
        
        if self.business_date is not None:
            return self.business_date.isoformat()
        
        return None
    

@dataclass(frozen=True)
class CvmDiscoveryCandidate:
    artifact: CvmSourceArtifact
    known_source_file_id: UUID | None = None
    known_file_hash: str | None = None
    known_status: str | None = None


@dataclass(frozen=True)
class CvmDiscoveryResult:
    artifact: CvmSourceArtifact
    decision: DiscoveryDecision
    reason: str
    discovery_run_id: UUID = field(default_factory=uuid4)
    known_source_file_id: UUID | None = None
    details: JsonDict = field(default_factory=dict)


def build_discovery_details(
    base: Mapping[str, object] | None = None,
    **extra: object,
) -> JsonDict:

    payload: JsonDict = dict(base or {})
    payload.update(extra)
    return payload
