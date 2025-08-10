"""Upload-related Pydantic schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from enum import Enum


class ProcessingStatus(str, Enum):
    """Processing status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class FileValidationResponse(BaseModel):
    """Response schema for file validation."""
    is_valid: bool
    file_size: int
    estimated_rows: Optional[int] = None
    estimated_columns: Optional[int] = None
    detected_delimiter: Optional[str] = None
    detected_encoding: Optional[str] = None
    error_message: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


class UploadResponse(BaseModel):
    """Response schema for file upload."""
    dataset_id: str
    filename: str
    file_size: int
    status: ProcessingStatus
    message: str
    upload_time: datetime
    estimated_completion_time: Optional[datetime] = None
    processing_progress: Optional[float] = None


class ProcessingStatusResponse(BaseModel):
    """Response schema for processing status."""
    dataset_id: str
    status: ProcessingStatus
    message: str
    progress: Optional[float] = None
    created_at: datetime
    updated_at: datetime
    completion_time: Optional[datetime] = None
    error_details: Optional[str] = None


class DatasetInfo(BaseModel):
    """Basic dataset information."""
    dataset_id: str
    filename: str
    file_size: int
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    created_at: datetime
    status: ProcessingStatus