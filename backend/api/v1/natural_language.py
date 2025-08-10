"""Natural language query API endpoints."""

from fastapi import APIRouter, HTTPException, Body
from typing import Dict, Any
import logging

from backend.core.llm.client import LLMClient
from backend.services.analytics_service import analytics_service

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/{dataset_id}/query")
async def parse_natural_language_query(
    dataset_id: str,
    query_data: Dict[str, Any] = Body(...)
):
    """
    Parse a natural language query and generate execution plan.
    
    Args:
        dataset_id: The unique identifier for the dataset
        query_data: Contains the natural language query
        
    Returns:
        Parsed query with execution plan
    """
    try:
        query = query_data.get("query", "").strip()
        if not query:
            raise HTTPException(
                status_code=400,
                detail="Query text is required"
            )
        
        # Get dataset information
        results = await analytics_service.get_analytics_results(dataset_id)
        if not results:
            raise HTTPException(
                status_code=404,
                detail="Dataset not found"
            )
        
        # Extract available columns and domain
        profile = results.get("profile", {})
        domain_info = results.get("domain_info", {})
        
        available_columns = [col["name"] for col in profile.get("columns", [])]
        domain = domain_info.get("domain", "generic")
        
        # Parse query using LLM
        llm_client = LLMClient()
        parsed_query = await llm_client.parse_natural_language_query(
            query, available_columns, domain
        )
        
        return {
            "dataset_id": dataset_id,
            "original_query": query,
            "parsed_query": parsed_query,
            "available_columns": available_columns,
            "domain": domain
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error parsing natural language query: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to parse query: {str(e)}"
        )


@router.post("/{dataset_id}/execute")
async def execute_natural_language_query(
    dataset_id: str,
    execution_data: Dict[str, Any] = Body(...)
):
    """
    Execute a parsed natural language query.
    
    Args:
        dataset_id: The unique identifier for the dataset
        execution_data: Contains the parsed query configuration
        
    Returns:
        Query execution results
    """
    try:
        # This is a placeholder for executing the parsed NL query
        # In a real implementation, this would convert the parsed query
        # into SQL and execute it using the analytics service
        
        chart_config = execution_data.get("chart_config", {})
        
        # For now, return a mock response
        return {
            "dataset_id": dataset_id,
            "execution_status": "success",
            "chart_config": chart_config,
            "message": "Natural language query execution is not yet implemented",
            "executed_at": "2024-01-01T00:00:00Z"
        }
        
    except Exception as e:
        logger.error(f"Error executing natural language query: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to execute query: {str(e)}"
        )