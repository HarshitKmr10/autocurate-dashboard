"""Analytics API endpoints."""

from fastapi import APIRouter, HTTPException, Query, Body
from typing import Optional, Dict, Any
import logging
from datetime import datetime

from backend.services.analytics_service import analytics_service
from backend.services.file_service import file_service

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/{dataset_id}/query")
async def execute_query(dataset_id: str, query_data: Dict[str, Any] = Body(...)):
    """
    Execute a data query on the dataset with comprehensive error handling.

    Args:
        dataset_id: The unique identifier for the dataset
        query_data: Query configuration

    Returns:
        Query results with sanitized data
    """
    try:
        logger.info(f"Executing query for dataset: {dataset_id}")

        # Validate inputs
        if not dataset_id or not isinstance(dataset_id, str):
            raise HTTPException(status_code=400, detail="Invalid dataset_id provided")

        # Get file path
        file_path = await file_service.get_file_path(dataset_id)
        if not file_path:
            logger.error(f"Dataset file not found for ID: {dataset_id}")
            raise HTTPException(status_code=404, detail="Dataset file not found")

        # Extract and validate SQL query
        sql_query = query_data.get("sql")
        if not sql_query or not isinstance(sql_query, str):
            raise HTTPException(
                status_code=400, detail="SQL query is required and must be a string"
            )

        # Basic SQL injection protection
        if not _validate_sql_safety(sql_query):
            logger.error(f"Potentially unsafe SQL query detected: {sql_query}")
            raise HTTPException(
                status_code=400, detail="Query contains potentially unsafe operations"
            )

        logger.info(f"Executing validated query: {sql_query[:100]}...")

        # Execute query with comprehensive error handling
        try:
            results = await analytics_service.query_data(
                dataset_id, sql_query, file_path
            )
        except Exception as query_error:
            logger.error(f"Query execution failed: {query_error}")

            # Provide user-friendly error messages based on error type
            error_message = _get_user_friendly_error_message(str(query_error))

            raise HTTPException(
                status_code=500, detail=f"Query execution failed: {error_message}"
            )

        # Validate results before returning
        if not isinstance(results, dict):
            logger.error("Query results are not in expected dictionary format")
            raise HTTPException(
                status_code=500, detail="Internal error: Invalid query result format"
            )

        # Ensure safe response structure
        safe_response = {
            "dataset_id": str(dataset_id),
            "query": str(sql_query),
            "results": results,
            "executed_at": datetime.utcnow().isoformat(),
        }

        # Final validation for JSON serialization
        try:
            import json

            json.dumps(safe_response, default=str)
        except (ValueError, TypeError) as json_error:
            logger.error(f"Response contains non-JSON serializable data: {json_error}")
            # Return a safe error response
            return {
                "dataset_id": str(dataset_id),
                "query": str(sql_query),
                "results": {
                    "columns": [],
                    "data": [],
                    "row_count": 0,
                    "query": str(sql_query),
                    "error": "Data contains invalid values that cannot be displayed",
                },
                "executed_at": datetime.utcnow().isoformat(),
                "warning": "Some data was filtered due to invalid values",
            }

        logger.info(
            f"Query executed successfully, returning {results.get('row_count', 0)} rows"
        )
        return safe_response

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logger.error(f"Unexpected error in execute_query: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail="An unexpected error occurred while processing your request",
        )


def _validate_sql_safety(sql_query: str) -> bool:
    """
    Validate SQL query for basic safety.

    Args:
        sql_query: SQL query to validate

    Returns:
        True if query appears safe, False otherwise
    """
    try:
        query_upper = sql_query.upper().strip()

        # Check for dangerous operations
        dangerous_keywords = [
            "DROP",
            "DELETE",
            "UPDATE",
            "INSERT",
            "CREATE",
            "ALTER",
            "EXEC",
            "EXECUTE",
            "DECLARE",
            "CURSOR",
            "BULK",
            "TRUNCATE",
            "MERGE",
            "GRANT",
            "REVOKE",
        ]

        for keyword in dangerous_keywords:
            if keyword in query_upper:
                logger.warning(f"Dangerous SQL keyword detected: {keyword}")
                return False

        # Ensure query starts with SELECT
        if not query_upper.startswith("SELECT"):
            logger.warning("Query does not start with SELECT")
            return False

        # Check for reasonable length
        if len(sql_query) > 5000:
            logger.warning("Query is too long")
            return False

        return True

    except Exception as e:
        logger.error(f"Error validating SQL safety: {e}")
        return False


def _get_user_friendly_error_message(error_str: str) -> str:
    """
    Convert technical error messages to user-friendly ones.

    Args:
        error_str: Technical error message

    Returns:
        User-friendly error message
    """
    error_lower = error_str.lower()

    if "nan" in error_lower or "json compliant" in error_lower:
        return "Data contains invalid numeric values that cannot be displayed"
    elif "column" in error_lower and "not found" in error_lower:
        return "One or more columns referenced in the query do not exist"
    elif "syntax error" in error_lower or "parse" in error_lower:
        return "The SQL query has invalid syntax"
    elif "permission" in error_lower or "access" in error_lower:
        return "Access denied for this operation"
    elif "timeout" in error_lower:
        return "Query took too long to execute"
    elif "memory" in error_lower:
        return "Query requires too much memory to execute"
    else:
        return "An error occurred while executing the query"


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
            raise HTTPException(status_code=404, detail="Data profile not found")

        return {
            "dataset_id": dataset_id,
            "profile": results.get("profile"),
            "domain_info": results.get("domain_info"),
            "profiled_at": results.get("processed_at"),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data profile: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get data profile")


@router.get("/{dataset_id}/sample")
async def get_data_sample(
    dataset_id: str, limit: int = Query(default=100, ge=1, le=1000)
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
                status_code=404, detail="Dataset not found or no data available"
            )

        return {"dataset_id": dataset_id, "sample_data": sample_data, "limit": limit}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting data sample: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to get data sample")


@router.get("/{dataset_id}/diagnostic")
async def get_diagnostic_info(dataset_id: str):
    """
    Get diagnostic information about the dataset for debugging.

    Args:
        dataset_id: The unique identifier for the dataset

    Returns:
        Diagnostic information
    """
    try:
        # Get file path
        file_path = await file_service.get_file_path(dataset_id)
        if not file_path:
            raise HTTPException(status_code=404, detail="Dataset file not found")

        # Get analytics results to see column classifications
        results = await analytics_service.get_analytics_results(dataset_id)

        # Execute some basic diagnostic queries
        diagnostic_queries = {
            "total_rows": "SELECT COUNT(*) as count FROM dataset",
            "first_5_rows": "SELECT * FROM dataset LIMIT 5",
            "column_info": "DESCRIBE dataset",
        }

        diagnostic_results = {}
        for name, query in diagnostic_queries.items():
            try:
                result = await analytics_service.query_data(
                    dataset_id, query, file_path
                )
                diagnostic_results[name] = result
            except Exception as e:
                diagnostic_results[name] = {"error": str(e)}

        return {
            "dataset_id": dataset_id,
            "file_path": file_path,
            "analytics_available": results is not None,
            "profile_info": (
                {
                    "numeric_columns": (
                        results.get("profile", {}).get("numeric_columns", [])
                        if results
                        else []
                    ),
                    "categorical_columns": (
                        results.get("profile", {}).get("categorical_columns", [])
                        if results
                        else []
                    ),
                    "total_columns": (
                        results.get("profile", {}).get("total_columns", 0)
                        if results
                        else 0
                    ),
                    "total_rows": (
                        results.get("profile", {}).get("total_rows", 0)
                        if results
                        else 0
                    ),
                }
                if results
                else None
            ),
            "dashboard_config": (
                {
                    "kpis": (
                        [
                            {
                                "id": kpi.get("id"),
                                "name": kpi.get("name"),
                                "value_column": kpi.get("value_column"),
                                "calculation": kpi.get("calculation"),
                            }
                            for kpi in results.get("dashboard_config", {}).get(
                                "kpis", []
                            )
                        ]
                        if results
                        else []
                    ),
                    "charts": (
                        [
                            {
                                "id": chart.get("id"),
                                "title": chart.get("title"),
                                "x_axis": chart.get("x_axis"),
                                "y_axis": chart.get("y_axis"),
                                "aggregation": chart.get("aggregation"),
                            }
                            for chart in results.get("dashboard_config", {}).get(
                                "charts", []
                            )
                        ]
                        if results
                        else []
                    ),
                }
                if results
                else None
            ),
            "diagnostic_queries": diagnostic_results,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting diagnostic info: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to get diagnostic info: {str(e)}"
        )
