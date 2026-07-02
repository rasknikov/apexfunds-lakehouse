from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from apex_lakehouse.ingestion.cvm.discovery_models import CvmDataset, DiscoveryDecision
from apex_lakehouse.ingestion.cvm.discovery_source import CvmSourceListing
from apex_lakehouse.ingestion.cvm.discovery_state import CvmDiscoveryStateRepository
from apex_lakehouse.ingestion.cvm.discovery_workflow import CvmDiscoveryBatch, CvmDiscoveryWorkflow
from apex_lakehouse.ingestion.cvm.discovery_service import CvmDiscoveryService, KnownArtifactState


def test_discover_dataset_builds_batch_from_source_and_known_state() -> None:
    discovered_at = datetime(2026, 7, 2, 10, 0, 0)
    source = type("SourceStub", (), {})()
    source.list_dataset = lambda dataset_name, listed_at: [
        CvmSourceListing(
            dataset_name=dataset_name,
            source_url="https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv",
            file_name="cad_fi.csv",
            listed_at=listed_at,
        )
    ]

    state_repository = type("StateStub", (), {})()
    state_repository.get_known_artifacts = lambda source_urls: [
        KnownArtifactState(
            source_url=source_urls[0],
            file_name="cad_fi.csv",
            status="failed",
            source_file_id=uuid4(),
        )
    ]

    workflow = CvmDiscoveryWorkflow(
        source=source,  # type: ignore[arg-type]
        state_repository=state_repository,  # type: ignore[arg-type]
        service=CvmDiscoveryService(),
    )

    batch = workflow.discover_dataset(CvmDataset.CADASTRO_FUNDOS, discovered_at=discovered_at)

    assert isinstance(batch, CvmDiscoveryBatch)
    assert batch.dataset_name is CvmDataset.CADASTRO_FUNDOS
    assert len(batch.listings) == 1
    assert len(batch.results) == 1
    assert batch.results[0].decision is DiscoveryDecision.CHANGED


def test_discover_many_returns_one_batch_per_dataset() -> None:
    discovered_at = datetime(2026, 7, 2, 10, 0, 0)
    workflow = CvmDiscoveryWorkflow(
        source=type("SourceStub", (), {"list_dataset": lambda self, dataset_name, listed_at: []})(),  # type: ignore[arg-type]
        state_repository=type("StateStub", (), {"get_known_artifacts": lambda self, source_urls: []})(),  # type: ignore[arg-type]
        service=CvmDiscoveryService(),
    )

    batches = workflow.discover_many(
        [CvmDataset.CADASTRO_FUNDOS, CvmDataset.INFORME_DIARIO],
        discovered_at=discovered_at,
    )

    assert [batch.dataset_name for batch in batches] == [
        CvmDataset.CADASTRO_FUNDOS,
        CvmDataset.INFORME_DIARIO,
    ]
