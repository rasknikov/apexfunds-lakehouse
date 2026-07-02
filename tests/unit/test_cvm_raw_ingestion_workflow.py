from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from apex_lakehouse.ingestion.cvm.discovery_models import (
    CvmDataset,
    CvmDiscoveryResult,
    CvmSourceArtifact,
    DiscoveryDecision,
)
from apex_lakehouse.ingestion.cvm.raw_ingestion_workflow import (
    CvmRawIngestionWorkflow,
    CvmRawWorkflowResult,
)


def test_ingest_discovery_result_persists_records_for_new_artifact() -> None:
    source_file_record = object()
    ingestion_state_record = object()
    ingestion_result = type(
        "IngestionResultStub",
        (),
        {
            "source_file_record": source_file_record,
            "ingestion_state_record": ingestion_state_record,
        },
    )()

    repository = type(
        "RepositoryStub",
        (),
        {
            "__init__": lambda self: setattr(self, "calls", []),
            "upsert_source_file": lambda self, record: self.calls.append(("source", record)),
            "upsert_ingestion_state": lambda self, record: self.calls.append(("state", record)),
        },
    )()
    captured_requests = []
    service = type(
        "ServiceStub",
        (),
        {"ingest": lambda self, request: captured_requests.append(request) or ingestion_result},
    )()
    pipeline_run_id = uuid4()
    known_source_file_id = uuid4()
    discovery_result = CvmDiscoveryResult(
        artifact=CvmSourceArtifact(
            dataset_name=CvmDataset.INFORME_DIARIO,
            source_url="https://dados.cvm.gov.br/file.zip",
            file_name="file.zip",
            discovered_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        ),
        decision=DiscoveryDecision.NEW,
        reason="new file",
        known_source_file_id=known_source_file_id,
        details={"origin": "module-4"},
    )

    workflow = CvmRawIngestionWorkflow(
        repository=repository,  # type: ignore[arg-type]
        service=service,  # type: ignore[arg-type]
    )

    result = workflow.ingest_discovery_result(
        discovery_result,
        updated_by="test-suite",
        pipeline_run_id=pipeline_run_id,
    )

    assert isinstance(result, CvmRawWorkflowResult)
    assert result.persisted is True
    assert result.ingestion_result is ingestion_result
    assert repository.calls == [
        ("source", source_file_record),
        ("state", ingestion_state_record),
    ]
    assert captured_requests[0].artifact == discovery_result.artifact
    assert captured_requests[0].pipeline_run_id == pipeline_run_id
    assert captured_requests[0].known_source_file_id == known_source_file_id
    assert captured_requests[0].metadata["discovery_decision"] == "new"
    assert captured_requests[0].metadata["origin"] == "module-4"


def test_ingest_discovery_result_skips_known_artifact() -> None:
    repository = type(
        "RepositoryStub",
        (),
        {
            "__init__": lambda self: setattr(self, "calls", []),
            "upsert_source_file": lambda self, record: self.calls.append(("source", record)),
            "upsert_ingestion_state": lambda self, record: self.calls.append(("state", record)),
        },
    )()
    service = type(
        "ServiceStub",
        (),
        {"ingest": lambda self, request: (_ for _ in ()).throw(AssertionError("should not ingest"))},
    )()
    discovery_result = CvmDiscoveryResult(
        artifact=CvmSourceArtifact(
            dataset_name=CvmDataset.CADASTRO_FUNDOS,
            source_url="https://dados.cvm.gov.br/cad_fi.csv",
            file_name="cad_fi.csv",
            discovered_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
        ),
        decision=DiscoveryDecision.ALREADY_KNOWN,
        reason="already registered",
    )

    workflow = CvmRawIngestionWorkflow(
        repository=repository,  # type: ignore[arg-type]
        service=service,  # type: ignore[arg-type]
    )

    result = workflow.ingest_discovery_result(
        discovery_result,
        updated_by="test-suite",
    )

    assert result.persisted is False
    assert result.ingestion_result is None
    assert repository.calls == []


def test_ingest_many_returns_one_result_per_discovery_result() -> None:
    artifact = CvmSourceArtifact(
        dataset_name=CvmDataset.CADASTRO_FUNDOS,
        source_url="https://dados.cvm.gov.br/cad_fi.csv",
        file_name="cad_fi.csv",
        discovered_at=datetime(2024, 1, 15, 9, 0, 0, tzinfo=timezone.utc),
    )
    service = type(
        "ServiceStub",
        (),
        {
            "ingest": lambda self, request: type(
                "IngestionResultStub",
                (),
                {
                    "source_file_record": object(),
                    "ingestion_state_record": object(),
                },
            )()
        },
    )()
    repository = type(
        "RepositoryStub",
        (),
        {
            "upsert_source_file": lambda self, record: None,
            "upsert_ingestion_state": lambda self, record: None,
        },
    )()

    workflow = CvmRawIngestionWorkflow(
        repository=repository,  # type: ignore[arg-type]
        service=service,  # type: ignore[arg-type]
    )

    results = workflow.ingest_many(
        [
            CvmDiscoveryResult(
                artifact=artifact,
                decision=DiscoveryDecision.NEW,
                reason="new",
            ),
            CvmDiscoveryResult(
                artifact=artifact,
                decision=DiscoveryDecision.CHANGED,
                reason="retry",
            ),
        ],
        updated_by="test-suite",
    )

    assert len(results) == 2
    assert all(result.persisted for result in results)
