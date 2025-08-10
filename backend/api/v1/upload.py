"""File upload API endpoints."""

from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Optional
import logging
import asyncio
import uuid
from datetime import datetime

from backend.config import get_settings
from backend.schemas.upload import (
    UploadResponse,
    ProcessingStatus,
    FileValidationResponse
)
from backend.services.file_service import file_service
from backend.services.analytics_service import analytics_service
from backend.utils.exceptions import AutocurateException
from backend.utils.validation import validate_csv_file

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()


@router.post("/", response_model=UploadResponse)
async def upload_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    sample_size: Optional[int] = None
):
    """
    Upload and process a CSV file.
    
    Args:
        file: The CSV file to upload
        sample_size: Optional sample size for initial analysis
        
    Returns:
        Upload response with dataset ID and processing status
    """
    logger.info(f"Received file upload: {file.filename}")
    
    # Generate unique dataset ID
    dataset_id = str(uuid.uuid4())
    
    try:
        # Validate file
        validation_result = await validate_csv_file(file)
        if not validation_result.is_valid:
            raise AutocurateException(
                status_code=400,
                detail=f"Invalid CSV file: {validation_result.error_message}",
                error_code="INVALID_CSV"
            )
        
        # Save file
        file_path = await file_service.save_uploaded_file(file, dataset_id)
        logger.info(f"File saved to: {file_path}")
        
        # Start background processing
        sample_size = sample_size or settings.default_sample_size
        background_tasks.add_task(
            process_csv_async,
            dataset_id,
            file_path,
            sample_size
        )
        
        return UploadResponse(
            dataset_id=dataset_id,
            filename=file.filename,
            file_size=validation_result.file_size,
            status=ProcessingStatus.PROCESSING,
            message="File uploaded successfully. Processing started.",
            upload_time=datetime.utcnow(),
            estimated_completion_time=datetime.utcnow()  # Will be updated during processing
        )
        
    except AutocurateException:
        raise
    except Exception as e:
        logger.error(f"Error uploading file: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to upload file"
        )


@router.get("/{dataset_id}/status", response_model=ProcessingStatus)
async def get_processing_status(dataset_id: str):
    """
    Get the processing status of an uploaded dataset.
    
    Args:
        dataset_id: The unique identifier for the dataset
        
    Returns:
        Current processing status
    """
    try:
        status = await analytics_service.get_processing_status(dataset_id)
        if not status:
            raise HTTPException(
                status_code=404,
                detail="Dataset not found"
            )
        return status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting processing status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to get processing status"
        )


@router.delete("/{dataset_id}")
async def delete_dataset(dataset_id: str):
    """
    Delete a dataset and all associated files.
    
    Args:
        dataset_id: The unique identifier for the dataset
    """
    try:
        success = await file_service.delete_dataset(dataset_id)
        if not success:
            raise HTTPException(
                status_code=404,
                detail="Dataset not found"
            )
        
        return {"message": "Dataset deleted successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting dataset: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to delete dataset"
        )


@router.post("/validate", response_model=FileValidationResponse)
async def validate_file(file: UploadFile = File(...)):
    """
    Validate a CSV file without uploading it.
    
    Args:
        file: The CSV file to validate
        
    Returns:
        Validation result
    """
    try:
        validation_result = await validate_csv_file(file)
        return validation_result
        
    except Exception as e:
        logger.error(f"Error validating file: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to validate file"
        )


async def process_csv_async(dataset_id: str, file_path: str, sample_size: int):
    """
    Background task to process uploaded CSV file.
    
    Args:
        dataset_id: Unique identifier for the dataset
        file_path: Path to the uploaded file
        sample_size: Sample size for analysis
    """
    try:
        logger.info(f"Starting CSV processing for dataset {dataset_id}")
        
        # Update status to processing
        await analytics_service.update_processing_status(
            dataset_id,
            ProcessingStatus.PROCESSING,
            "Analyzing data structure and types..."
        )
        
        # Process the CSV file
        await analytics_service.process_csv_file(
            dataset_id=dataset_id,
            file_path=file_path,
            sample_size=sample_size
        )
        
        # Update status to completed
        await analytics_service.update_processing_status(
            dataset_id,
            ProcessingStatus.COMPLETED,
            "Processing completed successfully"
        )
        
        logger.info(f"CSV processing completed for dataset {dataset_id}")
        
    except Exception as e:
        logger.error(f"Error processing CSV for dataset {dataset_id}: {e}", exc_info=True)
        
        # Update status to failed
        try:
            await analytics_service.update_processing_status(
                dataset_id,
                ProcessingStatus.FAILED,
                f"Processing failed: {str(e)}"
            )
        except Exception as status_error:
            logger.error(f"Failed to update status: {status_error}")