"""Custom exception classes."""

from datetime import datetime
from typing import Optional


class AutocurateException(Exception):
    """Base exception class for Autocurate application."""
    
    def __init__(
        self,
        detail: str,
        status_code: int = 500,
        error_code: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ):
        self.detail = detail
        self.status_code = status_code
        self.error_code = error_code or "INTERNAL_ERROR"
        self.timestamp = timestamp or datetime.utcnow()
        super().__init__(self.detail)


class FileValidationException(AutocurateException):
    """Exception raised when file validation fails."""
    
    def __init__(self, detail: str, error_code: str = "FILE_VALIDATION_ERROR"):
        super().__init__(
            detail=detail,
            status_code=400,
            error_code=error_code
        )


class DataProcessingException(AutocurateException):
    """Exception raised during data processing."""
    
    def __init__(self, detail: str, error_code: str = "DATA_PROCESSING_ERROR"):
        super().__init__(
            detail=detail,
            status_code=422,
            error_code=error_code
        )


class DomainDetectionException(AutocurateException):
    """Exception raised during domain detection."""
    
    def __init__(self, detail: str, error_code: str = "DOMAIN_DETECTION_ERROR"):
        super().__init__(
            detail=detail,
            status_code=422,
            error_code=error_code
        )


class LLMException(AutocurateException):
    """Exception raised during LLM operations."""
    
    def __init__(self, detail: str, error_code: str = "LLM_ERROR"):
        super().__init__(
            detail=detail,
            status_code=503,
            error_code=error_code
        )


class CacheException(AutocurateException):
    """Exception raised during cache operations."""
    
    def __init__(self, detail: str, error_code: str = "CACHE_ERROR"):
        super().__init__(
            detail=detail,
            status_code=503,
            error_code=error_code
        )


class DatasetNotFoundException(AutocurateException):
    """Exception raised when a dataset is not found."""
    
    def __init__(self, dataset_id: str):
        super().__init__(
            detail=f"Dataset not found: {dataset_id}",
            status_code=404,
            error_code="DATASET_NOT_FOUND"
        )