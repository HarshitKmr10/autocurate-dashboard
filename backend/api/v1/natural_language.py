"""Natural language query API endpoints."""

from fastapi import APIRouter, HTTPException, Body
from typing import Dict, Any, List, Optional
import logging
import json
import uuid
import asyncio
from datetime import datetime

from backend.core.llm.client import LLMClient
from backend.services.analytics_service import analytics_service

router = APIRouter()
logger = logging.getLogger(__name__)

# Global lock for preventing race conditions
_query_locks: Dict[str, asyncio.Lock] = {}


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
        
        # Extract rich data context (same as dashboard generation)
        profile = results.get("profile", {})
        domain_info = results.get("domain_info", {})
        
        # Prepare comprehensive profile summary (matching dashboard curator approach)
        profile_summary = {
            "total_rows": profile.get("total_rows", 0),
            "total_columns": profile.get("total_columns", 0),
            "numeric_columns": profile.get("numeric_columns", []),
            "categorical_columns": profile.get("categorical_columns", []),
            "datetime_columns": profile.get("datetime_columns", []),
            "columns": profile.get("columns", [])
        }
        
        available_columns = [col["name"] for col in profile.get("columns", [])]
        domain = domain_info.get("domain", "generic")
        
        # Generate sample data context for LLM (same as dashboard generation)
        sample_data_context = _generate_sample_data_for_llm(profile_summary)
        
        # Parse query using LLM with enhanced context
        llm_client = LLMClient()
        parsed_query = await llm_client.parse_natural_language_query_enhanced(
            query, available_columns, domain, profile_summary, sample_data_context
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


def _generate_sample_data_for_llm(profile_summary: Dict[str, Any]) -> str:
    """
    Generate sample data context for LLM (matching dashboard curator approach).
    
    Args:
        profile_summary: Data profile summary
        
    Returns:
        Formatted sample data string for LLM context
    """
    try:
        columns = profile_summary.get("columns", [])
        if not columns:
            return "No column information available"
        
        sample_rows = []
        for col in columns[:15]:  # Limit to first 15 columns for context
            col_name = col.get("name", "unknown")
            col_type = col.get("data_type", "unknown")
            
            # Get sample values if available
            sample_values = col.get("sample_values", [])
            top_values = col.get("top_values", [])
            
            # Use sample values or top values for context
            display_values = sample_values[:5] if sample_values else top_values[:5]
            if display_values:
                # Clean and format values
                clean_samples = []
                for val in display_values:
                    if val is not None:
                        clean_val = str(val)[:50] if len(str(val)) > 50 else str(val)
                        clean_samples.append(clean_val)
                
                if clean_samples:
                    sample_rows.append(f"- {col_name} ({col_type}): {', '.join(clean_samples)}")
                else:
                    sample_rows.append(f"- {col_name} ({col_type}): [no samples]")
            else:
                sample_rows.append(f"- {col_name} ({col_type}): [no samples]")
        
        return "\n".join(sample_rows) if sample_rows else "No valid column samples available"
        
    except Exception as e:
        logger.error(f"Error generating sample data for LLM: {e}")
        return "Error extracting sample data"


@router.post("/{dataset_id}/execute")
async def execute_natural_language_query(
    dataset_id: str,
    execution_data: Dict[str, Any] = Body(...)
):
    """
    Execute a parsed natural language query and generate actual chart data.
    
    Args:
        dataset_id: The unique identifier for the dataset
        execution_data: Contains the parsed query configuration
        
    Returns:
        Query execution results with actual data
    """
    # Prevent race conditions with dataset-specific locks
    if dataset_id not in _query_locks:
        _query_locks[dataset_id] = asyncio.Lock()
    
    async with _query_locks[dataset_id]:
        try:
            parsed_query = execution_data.get("parsed_query", {})
            if not parsed_query:
                raise HTTPException(
                    status_code=400,
                    detail="Parsed query is required"
                )
            
            # Generate SQL query from parsed configuration
            sql_query = await _generate_sql_from_parsed_query(dataset_id, parsed_query)
            
            # Get the file path for the dataset
            results = await analytics_service.get_analytics_results(dataset_id)
            if not results:
                raise HTTPException(
                    status_code=404,
                    detail="Dataset not found"
                )
            
            # Try to get file path from results or construct it
            file_path = None
            if 'file_path' in results:
                file_path = results['file_path']
            else:
                # Try to construct file path from dataset ID
                import os
                upload_dir = f"data/uploads/{dataset_id}"
                if os.path.exists(upload_dir):
                    # Find the first CSV file in the directory
                    for file in os.listdir(upload_dir):
                        if file.endswith('.csv'):
                            file_path = os.path.join(upload_dir, file)
                            break
            
            # Execute the SQL query
            query_result = await analytics_service.query_data(dataset_id, sql_query, file_path)
            
            # Generate chart configuration
            chart_config = await _generate_chart_config_from_parsed_query(parsed_query, query_result)
            
            return {
                "dataset_id": dataset_id,
                "execution_status": "success",
                "chart_config": chart_config,
                "data": query_result,
                "sql_query": sql_query,
                "executed_at": datetime.utcnow().isoformat()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error executing natural language query: {e}", exc_info=True)
            
            # Provide more specific error messages
            error_message = str(e)
            if "Table with name dataset does not exist" in error_message:
                error_message = "Dataset not properly loaded. Please ensure the dataset is uploaded and processed."
            elif "column" in error_message.lower() and "not found" in error_message.lower():
                error_message = "One or more specified columns were not found in the dataset."
            elif "syntax error" in error_message.lower():
                error_message = "Generated query has syntax issues. Please try rephrasing your request."
            
            raise HTTPException(
                status_code=500,
                detail=f"Failed to execute query: {error_message}"
            )


@router.post("/{dataset_id}/modify_chart")
async def modify_existing_chart(
    dataset_id: str,
    modification_data: Dict[str, Any] = Body(...)
):
    """
    Modify an existing chart based on natural language instructions.
    
    Args:
        dataset_id: The unique identifier for the dataset
        modification_data: Contains the modification instructions and existing chart config
        
    Returns:
        Modified chart configuration and data
    """
    try:
        query = modification_data.get("query", "").strip()
        existing_chart = modification_data.get("existing_chart", {})
        
        if not query:
            raise HTTPException(
                status_code=400,
                detail="Modification query is required"
            )
        
        if not existing_chart:
            raise HTTPException(
                status_code=400,
                detail="Existing chart configuration is required"
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
        
        # Parse modification request using LLM
        llm_client = LLMClient()
        modification_plan = await llm_client.parse_chart_modification(
            query, existing_chart, available_columns, domain
        )
        
        return {
            "dataset_id": dataset_id,
            "original_query": query,
            "existing_chart": existing_chart,
            "modification_plan": modification_plan,
            "available_columns": available_columns
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error parsing chart modification: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to parse modification: {str(e)}"
        )


@router.post("/{dataset_id}/apply_modification")
async def apply_chart_modification(
    dataset_id: str,
    application_data: Dict[str, Any] = Body(...)
):
    """
    Apply a parsed chart modification and return the updated chart with data.
    
    Args:
        dataset_id: The unique identifier for the dataset
        application_data: Contains the modification plan to apply
        
    Returns:
        Updated chart configuration and data
    """
    # Prevent race conditions with dataset-specific locks
    if dataset_id not in _query_locks:
        _query_locks[dataset_id] = asyncio.Lock()
    
    async with _query_locks[dataset_id]:
        try:
            modification_plan = application_data.get("modification_plan", {})
            if not modification_plan:
                raise HTTPException(
                    status_code=400,
                    detail="Modification plan is required"
                )
            
            # Apply the modifications to create new chart config
            new_chart_config = modification_plan.get("new_chart_config", {})
            original_chart = modification_plan.get("original_chart", {})
            
            # Preserve the original chart ID to prevent React key conflicts
            if original_chart.get("id") and not new_chart_config.get("id"):
                new_chart_config["id"] = original_chart["id"]
            
            # Get the dataset info for validation
            results = await analytics_service.get_analytics_results(dataset_id)
            if not results:
                raise HTTPException(
                    status_code=404,
                    detail="Dataset not found"
                )
            
            # Get available columns to validate the chart config
            profile = results.get("profile", {})
            available_columns = [col["name"] for col in profile.get("columns", [])]
            
            # Validate column references in the new chart config
            x_axis = new_chart_config.get("x_axis")
            y_axis = new_chart_config.get("y_axis")
            color_by = new_chart_config.get("color_by")
            aggregation = new_chart_config.get("aggregation", "").lower()
            
            # Check if referenced columns exist
            missing_columns = []
            if x_axis and x_axis not in available_columns:
                missing_columns.append(f"x_axis: {x_axis}")
            if y_axis and y_axis not in available_columns:
                # Only check if y_axis is not meant for count aggregation
                if aggregation != "count":
                    missing_columns.append(f"y_axis: {y_axis}")
            if color_by and color_by not in available_columns:
                missing_columns.append(f"color_by: {color_by}")
            
            if missing_columns:
                logger.error(f"Missing columns: {missing_columns}. Available: {available_columns}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Column(s) not found in dataset: {', '.join(missing_columns)}. Available columns: {', '.join(available_columns)}"
                )
            
            # Generate SQL query for the modified chart
            sql_query = await _generate_sql_from_chart_config(dataset_id, new_chart_config)
            
            # Try to get file path from results or construct it
            file_path = None
            if 'file_path' in results:
                file_path = results['file_path']
            else:
                # Try to construct file path from dataset ID
                import os
                upload_dir = f"data/uploads/{dataset_id}"
                if os.path.exists(upload_dir):
                    # Find the first CSV file in the directory
                    for file in os.listdir(upload_dir):
                        if file.endswith('.csv'):
                            file_path = os.path.join(upload_dir, file)
                            break
            
            # Execute the SQL query
            query_result = await analytics_service.query_data(dataset_id, sql_query, file_path)
            
            return {
                "dataset_id": dataset_id,
                "modification_status": "success",
                "original_chart": modification_plan.get("original_chart", {}),
                "new_chart_config": new_chart_config,
                "data": query_result,
                "sql_query": sql_query,
                "changes_applied": modification_plan.get("changes_applied", []),
                "applied_at": datetime.utcnow().isoformat()
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error applying chart modification: {e}", exc_info=True)
            
            # Provide more specific error messages
            error_message = str(e)
            if "Table with name dataset does not exist" in error_message:
                error_message = "Dataset not properly loaded. Please ensure the dataset is uploaded and processed."
            elif "column" in error_message.lower() and "not found" in error_message.lower():
                error_message = "One or more specified columns were not found in the dataset."
            elif "syntax error" in error_message.lower():
                error_message = "Generated query has syntax issues. Please try rephrasing your request."
            
            raise HTTPException(
                status_code=500,
                detail=f"Failed to apply modification: {error_message}"
            )


@router.post("/{dataset_id}/add_to_dashboard")
async def add_chart_to_dashboard(
    dataset_id: str,
    chart_data: Dict[str, Any] = Body(...)
):
    """
    Add a new chart created from natural language to the existing dashboard.
    
    Args:
        dataset_id: The unique identifier for the dataset
        chart_data: Contains the chart configuration to add
        
    Returns:
        Updated dashboard configuration
    """
    try:
        chart_config = chart_data.get("chart_config", {})
        if not chart_config:
            raise HTTPException(
                status_code=400,
                detail="Chart configuration is required"
            )
        
        # Get current dashboard configuration
        dashboard_results = await analytics_service.get_analytics_results(dataset_id)
        if not dashboard_results:
            raise HTTPException(
                status_code=404,
                detail="Dashboard not found"
            )
        
        current_dashboard = dashboard_results.get("dashboard_config", {})
        
        # Add unique ID to the new chart
        chart_config["id"] = str(uuid.uuid4())
        
        # Add the new chart to the dashboard
        current_charts = current_dashboard.get("charts", [])
        current_charts.append(chart_config)
        current_dashboard["charts"] = current_charts
        
        # Update dashboard configuration in cache
        dashboard_results["dashboard_config"] = current_dashboard
        from backend.services.cache_service import cache_service
        await cache_service.set(
            f"analytics:{dataset_id}", 
            dashboard_results, 
            ttl=86400  # 24 hours in seconds
        )
        
        return {
            "dataset_id": dataset_id,
            "status": "success",
            "new_chart_id": chart_config["id"],
            "updated_dashboard": current_dashboard,
            "total_charts": len(current_charts),
            "added_at": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error adding chart to dashboard: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add chart: {str(e)}"
        )


# Helper functions

async def _generate_sql_from_parsed_query(dataset_id: str, parsed_query: Dict[str, Any]) -> str:
    """Generate SQL query from parsed natural language query."""
    try:
        chart_config = parsed_query.get("chart_config", {})
        
        x_axis = chart_config.get("x_axis")
        y_axis = chart_config.get("y_axis")
        color_by = chart_config.get("color_by")
        aggregation = chart_config.get("aggregation", "count")
        filters = chart_config.get("filters", {})
        
        # Build SELECT clause
        select_parts = []
        if x_axis:
            select_parts.append(f'"{x_axis}"')
        
        if y_axis and aggregation and aggregation.lower() in ["sum", "avg", "count", "min", "max"]:
            if aggregation.lower() == "count":
                select_parts.append(f"COUNT(*) as {y_axis}_count")
            else:
                select_parts.append(f"{aggregation.upper()}(\"{y_axis}\") as {y_axis}_{aggregation}")
        elif y_axis:
            select_parts.append(f'"{y_axis}"')
        
        if color_by and color_by not in [x_axis, y_axis]:
            select_parts.append(f'"{color_by}"')
        
        if not select_parts:
            select_parts.append("*")
        
        # Build base query
        sql_parts = [f"SELECT {', '.join(select_parts)} FROM dataset"]
        
        # Add WHERE clause for filters and null handling
        where_conditions = []
        
        # Filter out nulls for key columns
        if x_axis:
            where_conditions.append(f'"{x_axis}" IS NOT NULL')
        if y_axis:
            where_conditions.append(f'"{y_axis}" IS NOT NULL')
        
        # Add user-specified filters
        if filters:
            for column, value in filters.items():
                if isinstance(value, list):
                    quoted_values = [f"'{v}'" for v in value]
                    where_conditions.append(f'"{column}" IN ({", ".join(quoted_values)})')
                elif isinstance(value, str):
                    where_conditions.append(f'"{column}" = \'{value}\'')
                else:
                    where_conditions.append(f'"{column}" = {value}')
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        # Add GROUP BY if we have aggregation and x_axis
        if x_axis and y_axis and aggregation and aggregation.lower() in ["sum", "avg", "count", "min", "max"]:
            group_by_columns = [f'"{x_axis}"']
            if color_by and color_by != x_axis:
                group_by_columns.append(f'"{color_by}"')
            sql_parts.append(f"GROUP BY {', '.join(group_by_columns)}")
        
        # Add ORDER BY
        if x_axis:
            sql_parts.append(f'ORDER BY "{x_axis}"')
        
        # Add LIMIT
        sql_parts.append("LIMIT 1000")
        
        sql_query = " ".join(sql_parts)
        logger.info(f"Generated SQL from parsed query: {sql_query}")
        
        return sql_query
        
    except Exception as e:
        logger.error(f"Error generating SQL from parsed query: {e}")
        raise Exception(f"Failed to generate SQL query: {str(e)}")


async def _generate_chart_config_from_parsed_query(parsed_query: Dict[str, Any], query_result: Dict[str, Any]) -> Dict[str, Any]:
    """Generate chart configuration from parsed query and query results."""
    try:
        chart_config = parsed_query.get("chart_config", {})
        
        # Get the actual column names from the query result
        result_columns = query_result.get("columns", [])
        
        # Map the parsed config to actual result column names
        x_axis = chart_config.get("x_axis")
        y_axis = chart_config.get("y_axis")
        color_by = chart_config.get("color_by")
        
        # For aggregated queries, the y_axis column name might be different in results
        # e.g., 'order_id' becomes 'order_id_count' after COUNT aggregation
        if y_axis and result_columns:
            aggregation = chart_config.get("aggregation", "")
            if aggregation in ["count", "sum", "avg", "min", "max"]:
                # Look for the aggregated column name pattern
                expected_aggregated_name = f"{y_axis}_{aggregation}"
                if expected_aggregated_name in result_columns:
                    y_axis = expected_aggregated_name
                elif f"{aggregation}_{y_axis}" in result_columns:
                    y_axis = f"{aggregation}_{y_axis}"
                # If COUNT(*), look for 'count' column
                elif aggregation == "count" and "count" in result_columns:
                    y_axis = "count"
        
        # Create a complete chart configuration
        config = {
            "id": str(uuid.uuid4()),
            "type": parsed_query.get("chart_type", "bar"),
            "title": chart_config.get("title", "Natural Language Chart"),
            "description": parsed_query.get("reasoning", "Chart created from natural language query"),
            "x_axis": x_axis,
            "y_axis": y_axis,
            "color_by": color_by,
            "aggregation": chart_config.get("aggregation"),
            "filters": list(chart_config.get("filters", {}).keys()),
            "sort_order": "asc",
            "width": 6,
            "height": 4,
            "importance": "medium",
            "explanation": parsed_query.get("reasoning", "Generated from natural language query")
        }
        
        logger.info(f"Generated chart config with y_axis: {y_axis} from result columns: {result_columns}")
        
        return config
        
    except Exception as e:
        logger.error(f"Error generating chart config: {e}")
        raise Exception(f"Failed to generate chart configuration: {str(e)}")


async def _generate_sql_from_chart_config(dataset_id: str, chart_config: Dict[str, Any]) -> str:
    """Generate SQL query from chart configuration."""
    try:
        x_axis = chart_config.get("x_axis")
        y_axis = chart_config.get("y_axis")
        color_by = chart_config.get("color_by")
        aggregation = chart_config.get("aggregation", "count")
        
        # Build SELECT clause
        select_parts = []
        
        if x_axis:
            # For date columns, consider monthly aggregation to reduce data points
            if 'date' in x_axis.lower() and aggregation and aggregation.lower() in ["sum", "avg", "count", "min", "max"]:
                # Use monthly aggregation for time series to avoid crowded charts
                select_parts.append(f'DATE_TRUNC(\'month\', "{x_axis}") as {x_axis}_month')
                x_axis_for_grouping = f'DATE_TRUNC(\'month\', "{x_axis}")'
                x_axis_display = f"{x_axis}_month"
            else:
                select_parts.append(f'"{x_axis}"')
                x_axis_for_grouping = f'"{x_axis}"'
                x_axis_display = x_axis
        
        # Handle y_axis - DON'T treat aggregation function names as columns
        if y_axis:
            if aggregation and aggregation.lower() in ["sum", "avg", "count", "min", "max"]:
                if aggregation.lower() == "count":
                    # For count aggregation, just count all rows
                    select_parts.append(f"COUNT(*) as count")
                else:
                    # For other aggregations, use the y_axis as the column to aggregate
                    select_parts.append(f'{aggregation.upper()}("{y_axis}") as {y_axis}_{aggregation}')
            else:
                # No aggregation, just select the column
                select_parts.append(f'"{y_axis}"')
        elif aggregation and aggregation.lower() == "count":
            # If no y_axis specified but count aggregation requested
            select_parts.append(f"COUNT(*) as count")
        
        if color_by and color_by not in [x_axis, y_axis]:
            select_parts.append(f'"{color_by}"')
        
        if not select_parts:
            select_parts.append("*")
        
        # Build base query
        sql_parts = [f"SELECT {', '.join(select_parts)} FROM dataset"]
        
        # Add WHERE clause to filter out nulls ONLY for actual columns that exist
        where_conditions = []
        if x_axis:
            where_conditions.append(f'"{x_axis}" IS NOT NULL')
        
        # Only add null filter for y_axis if it's an actual column (not an aggregation)
        if y_axis and (not aggregation or aggregation.lower() not in ["count", "sum", "avg", "min", "max"]):
            where_conditions.append(f'"{y_axis}" IS NOT NULL')
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        # Add GROUP BY if we have aggregation and x_axis
        if x_axis and aggregation and aggregation.lower() in ["sum", "avg", "count", "min", "max"]:
            group_by_columns = [x_axis_for_grouping]
            if color_by and color_by != x_axis:
                group_by_columns.append(f'"{color_by}"')
            sql_parts.append(f"GROUP BY {', '.join(group_by_columns)}")
        
        # Add ORDER BY
        if x_axis:
            sql_parts.append(f'ORDER BY {x_axis_for_grouping}')
        
        # Add LIMIT
        sql_parts.append("LIMIT 1000")
        
        sql_query = " ".join(sql_parts)
        logger.info(f"Generated SQL from chart config: {sql_query}")
        
        return sql_query
        
    except Exception as e:
        logger.error(f"Error generating SQL from chart config: {e}")
        raise Exception(f"Failed to generate SQL query: {str(e)}")