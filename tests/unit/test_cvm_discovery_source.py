from __future__ import annotations

from datetime import datetime

from apex_lakehouse.ingestion.cvm.discovery_models import CvmDataset
from apex_lakehouse.ingestion.cvm.discovery_source import CvmDiscoverySource


def test_parse_listing_hrefs_extracts_anchor_targets() -> None:
    html = """
    <html>
      <body>
        <a href="../">../</a>
        <a href="inf_diario_fi_202401.zip">inf_diario_fi_202401.zip</a>
        <a href="subdir/">subdir/</a>
      </body>
    </html>
    """

    hrefs = CvmDiscoverySource.parse_listing_hrefs(html)

    assert hrefs == ["../", "inf_diario_fi_202401.zip", "subdir/"]


def test_list_dataset_filters_to_matching_artifacts() -> None:
    source = CvmDiscoverySource()
    source.fetch_index_html = lambda dataset_name: """
    <html>
      <body>
        <a href="../">../</a>
        <a href="inf_diario_fi_202401.zip">inf_diario_fi_202401.zip</a>
        <a href="perfil_mensal_fi_202401.csv">perfil_mensal_fi_202401.csv</a>
      </body>
    </html>
    """

    listings = source.list_dataset(
        CvmDataset.INFORME_DIARIO,
        listed_at=datetime(2026, 7, 2, 10, 0, 0),
    )

    assert len(listings) == 1
    assert listings[0].dataset_name is CvmDataset.INFORME_DIARIO
    assert listings[0].file_name == "inf_diario_fi_202401.zip"
    assert listings[0].source_url.endswith("/inf_diario_fi_202401.zip")


def test_list_datasets_concatenates_results() -> None:
    source = CvmDiscoverySource()
    listing_calls: list[CvmDataset] = []

    def _list_dataset(dataset_name: CvmDataset, *, listed_at: datetime):
        listing_calls.append(dataset_name)
        return []

    source.list_dataset = _list_dataset  # type: ignore[method-assign]

    source.list_datasets(
        [CvmDataset.INFORME_DIARIO, CvmDataset.CADASTRO_FUNDOS],
        listed_at=datetime(2026, 7, 2, 10, 0, 0),
    )

    assert listing_calls == [CvmDataset.INFORME_DIARIO, CvmDataset.CADASTRO_FUNDOS]
