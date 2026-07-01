"""Shared exception types for the Apex Lakehouse platform."""


class ApexLakehouseError(Exception):
    """Base exception for project-specific failures."""


class ConfigurationError(ApexLakehouseError):
    """Raised when required configuration is missing or invalid."""


class ValidationError(ApexLakehouseError):
    """Raised when input data fails business or technical validation."""


class ExternalServiceError(ApexLakehouseError):
    """Raised when an external system cannot be reached or returns an invalid response."""


class DatasetNotFoundError(ApexLakehouseError):
    """Raised when a dataset or expected source artifact cannot be found."""


class IngestionStateError(ApexLakehouseError):
    """Raised when ingestion state is inconsistent with the requested operation."""