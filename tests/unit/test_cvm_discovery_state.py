from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from apex_lakehouse.ingestion.cvm.discovery_state import CvmDiscoveryStateRepository


def test_get_known_artifacts_maps_repository_rows() -> None:
    source_file_id = uuid4()
    repository = type("RepositoryStub", (), {})()
    repository.get_source_files_by_urls = lambda **kwargs: [
        {
            "source_url": "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv",
            "file_name": "cad_fi.csv",
            "status": "ingested",
            "source_file_id": source_file_id,
            "file_hash": "abc123",
            "source_last_modified_at": datetime(2026, 7, 2, 10, 0, 0),
        }
    ]

    state_repository = CvmDiscoveryStateRepository(repository)  # type: ignore[arg-type]
    artifacts = state_repository.get_known_artifacts(
        ["https://dados.cvm.gov.br/dados/FI/CAD/DADOS/cad_fi.csv"]
    )

    assert len(artifacts) == 1
    assert artifacts[0].source_url.endswith("cad_fi.csv")
    assert artifacts[0].source_file_id == source_file_id
    assert artifacts[0].status == "ingested"
    assert artifacts[0].file_hash == "abc123"
