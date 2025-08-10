"""Analytics API endpoints."""

from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, Dict, Any
import logging

from backend.services.analytics_service import analytics_service
from backend.services.file_service import file_service

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/{dataset_id}/query")
async def execute_query(
    dataset_id: str,
    query_data: Dict[str, Any] = Body(...)
):
    """
    Execute a data query on the dataset.
    
    Args:
        dataset_id: The unique identifier for the dataset
        query_data: Query configuration
        
    Returns:
        Query results
    """
    try:
        # Get file path
        file_path = await file_service.get_file_path(dataset_id)
        if not file_path:
            raise HTTPException(
                status_code=404,
                detail="Dataset file not found"
            )
        
        # Extract SQL query from request
        sql_query = query_data.get("sql")
        if not sql_query:
            raise HTTPException(
                status_code=400,
                detail="SQL query is required"
            )
        
        # Execute query
        results = await analytics_service.query_data(dataset_id, sql_query, file_path)
        
        return {
            "dataset_id": dataset_id,
            "query": sql_query,
            "results": results,
            "executed_at": "2024-01-01T00:00:00Z"  # Would be current timestamp
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing query: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Query execution failed: {str(e)}"
        )


@router.get("/{dataset_id}/profile")
async def get_data_profile(dataset_id: str):
    """
    Get the data profile for a dataset.
    
    Args:
        dataset_id: The unique identifier for the dataset
        
    Returns:
        Data profile information
    """
    try:
        results = await analytics_service.get_analytics_results(dataset_id)
        if not results:
            raise HTTPException(
                status_code=404,
                detail="Data profile not found"
            )
        
        return {
            "dataset_id": dataset_id,
            "profile": results.get("profile"),
            "domain_info": results.get("domain_info"),
            "profiled_at": results.get("processed_at")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data profile: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to get data profile"
        )


@router.get("/{dataset_id}/sample")
async def get_data_sample(
    dataset_id: str,
    limit: int = Query(default=100, ge=1, le=1000)
):
    """
    Get a sample of the dataset.
    
    Args:
        dataset_id: The unique identifier for the dataset
        limit: Number of rows to return
        
    Returns:
        Sample data
    """
    try:
        sample_data = await analytics_service.get_data_sample(dataset_id, limit)
        
        if not sample_data:
            raise HTTPException(
                status_code=404,
                detail="Dataset not found or no data available"
            )
        
        return {
            "dataset_id": dataset_id,
            "sample_data": sample_data,
            "limit": limit
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data sample: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="Failed to get data sample"
        )