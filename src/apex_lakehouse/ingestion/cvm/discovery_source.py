"""CVM public-source listing and HTML index parsing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin
from urllib.request import urlopen

from apex_lakehouse.exceptions import ExternalServiceError
from apex_lakehouse.ingestion.cvm.discovery_models import CvmDataset
from apex_lakehouse.ingestion.cvm.discovery_parser import detect_cvm_dataset, extract_file_name_from_url


CVM_DATASET_INDEX_URLS: dict[CvmDataset, str] = {
    CvmDataset.INFORME_DIARIO: "https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/",
    CvmDataset.CADASTRO_FUNDOS: "https://dados.cvm.gov.br/dados/FI/CAD/DADOS/",
    CvmDataset.PERFIL_MENSAL: "https://dados.cvm.gov.br/dados/FI/DOC/PERFIL_MENSAL/DADOS/",
}


@dataclass(frozen=True)
class CvmSourceListing:
    """One artifact listed in a CVM public index page."""

    dataset_name: CvmDataset
    source_url: str
    file_name: str
    listed_at: datetime


class _HrefCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return

        attributes = dict(attrs)
        href = attributes.get("href")
        if href:
            self.hrefs.append(href)


class CvmDiscoverySource:
    """List discoverable public artifacts from CVM index pages."""

    def __init__(self, dataset_index_urls: dict[CvmDataset, str] | None = None):
        self._dataset_index_urls = dataset_index_urls or CVM_DATASET_INDEX_URLS

    def get_dataset_index_url(self, dataset_name: CvmDataset) -> str:
        try:
            return self._dataset_index_urls[dataset_name]
        except KeyError as exc:
            raise ExternalServiceError(f"No CVM index URL configured for dataset {dataset_name.value}.") from exc

    def fetch_index_html(self, dataset_name: CvmDataset) -> str:
        index_url = self.get_dataset_index_url(dataset_name)
        try:
            with urlopen(index_url) as response:  # nosec: B310 - controlled public URL list
                return response.read().decode("utf-8", errors="replace")
        except Exception as exc:
            raise ExternalServiceError(
                f"Could not fetch CVM index page for dataset {dataset_name.value}: {index_url}"
            ) from exc

    def list_dataset(self, dataset_name: CvmDataset, *, listed_at: datetime) -> list[CvmSourceListing]:
        html = self.fetch_index_html(dataset_name)
        index_url = self.get_dataset_index_url(dataset_name)
        hrefs = self.parse_listing_hrefs(html)

        listings: list[CvmSourceListing] = []
        for href in hrefs:
            if not self._is_candidate_href(href):
                continue

            source_url = urljoin(index_url, href)
            file_name = extract_file_name_from_url(source_url)

            try:
                detected_dataset = detect_cvm_dataset(file_name)
            except Exception:
                continue

            if detected_dataset is not dataset_name:
                continue

            listings.append(
                CvmSourceListing(
                    dataset_name=dataset_name,
                    source_url=source_url,
                    file_name=file_name,
                    listed_at=listed_at,
                )
            )

        return listings

    def list_datasets(
        self,
        dataset_names: Iterable[CvmDataset],
        *,
        listed_at: datetime,
    ) -> list[CvmSourceListing]:
        listings: list[CvmSourceListing] = []
        for dataset_name in dataset_names:
            listings.extend(self.list_dataset(dataset_name, listed_at=listed_at))
        return listings

    @staticmethod
    def parse_listing_hrefs(html: str) -> list[str]:
        parser = _HrefCollector()
        parser.feed(html)
        return parser.hrefs

    @staticmethod
    def _is_candidate_href(href: str) -> bool:
        normalized = href.strip()
        if not normalized or normalized in {"../", "./"}:
            return False
        if normalized.endswith("/"):
            return False
        return normalized.lower().endswith((".zip", ".csv"))
