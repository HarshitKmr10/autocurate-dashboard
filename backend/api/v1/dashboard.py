"""Dashboard API endpoints."""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import logging

from backend.services.analytics_service import analytics_service
from backend.schemas.upload import ProcessingStatus

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/{dataset_id}")
async def get_dashboard(dataset_id: str):
    """
    Get the generated dashboard configuration for a dataset.
    
    Args:
        dataset_id: The unique identifier for the dataset
        
    Returns:
        Dashboard configuration
    """
    try:
        # Check if processing is complete
        status = await analytics_service.get_processing_status(dataset_id)
        if not status:
            raise HTTPException(
                status_code=404,
                detail="Dataset not found"
            )
        
        if status.status != ProcessingStatus.COMPLETED:
            raise HTTPException(
                status_code=202,
                detail=f"Dashboard not ready. Status: {status.status}"
            )
        
        # Get analytics results
        results = await analytics_service.get_analytics_results(dataset_id)
        if not results:
            raise HTTPException(
                status_code=404,
                detail="Dashboard configuration not found"
            )
        
        return {
            "dataset_id": dataset_id,
            "dashboard_config": results.get("dashboard_config"),
            "domain_info": results.get("domain_info"),
            "profile": results.get("profile"),
            "generated_at": results.get("processed_at")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting dashboard: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to get dashboard"
        )


@router.get("/{dataset_id}/preview")
async def get_dashboard_preview(
    dataset_id: str,
    limit: int = Query(default=100, ge=1, le=1000)
):
    """
    Get a preview of the dashboard with sample data.
    
    Args:
        dataset_id: The unique identifier for the dataset
        limit: Number of rows to include in preview
        
    Returns:
        Dashboard preview with sample data
    """
    try:
        # Get dashboard configuration
        dashboard_response = await get_dashboard(dataset_id)
        
        # Get sample data
        sample_data = await analytics_service.get_data_sample(dataset_id, limit)
        
        return {
            **dashboard_response,
            "sample_data": sample_data,
            "preview_limit": limit
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting dashboard preview: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to get dashboard preview"
        )