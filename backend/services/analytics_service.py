"""Analytics and data processing service."""

import pandas as pd
import polars as pl
import duckdb
import json
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from pathlib import Path
import numpy as np

from backend.config import get_settings
from backend.schemas.upload import ProcessingStatus, ProcessingStatusResponse
from backend.core.profiler.data_profiler import DataProfiler
from backend.core.domain.detector import (
    DomainDetector,
    DomainClassification,
    DomainType,
)
from backend.core.curator.dashboard_curator import DashboardCurator
from backend.core.analyzer.csv_analyzer import csv_analyzer
from backend.services.cache_service import cache_service

logger = logging.getLogger(__name__)
settings = get_settings()


class AnalyticsService:
    """Service for data analytics and processing."""

    def __init__(self):
        self.data_profiler = DataProfiler()
        self.domain_detector = DomainDetector()
        self.dashboard_curator = DashboardCurator()
        self.processing_status: Dict[str, ProcessingStatusResponse] = {}

    async def process_csv_file(
        self, dataset_id: str, file_path: str, sample_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Process a CSV file and generate analytics.

        Args:
            dataset_id: Unique identifier for the dataset
            file_path: Path to the CSV file
            sample_size: Number of rows to sample for analysis

        Returns:
            Processing results
        """
        try:
            logger.info(f"Starting CSV processing for {dataset_id}")

            # Update status
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.PROCESSING,
                "Loading and validating data...",
                0.1,
            )

            # Load data
            df = await self._load_csv_data(file_path, sample_size)

            # Update status
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.PROCESSING,
                "Profiling data structure and types...",
                0.3,
            )

            # Use enhanced CSV analyzer
            filename = Path(file_path).name
            enhanced_analysis = await csv_analyzer.analyze_csv(df, filename)

            # Update status
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.PROCESSING,
                "Generating insights and visualizations...",
                0.5,
            )

            # Fallback to original profiler if needed
            try:
                profile = await self.data_profiler.profile_data(file_path, dataset_id)
            except Exception as e:
                logger.warning(
                    f"Data profiler failed, creating basic profile from enhanced analysis: {e}"
                )
                # Create a basic DataProfile object from enhanced analysis
                from backend.core.profiler.data_profiler import (
                    DataProfile,
                    ColumnProfile,
                    ColumnType,
                )

                # Create basic column profiles
                column_profiles = []
                for col in enhanced_analysis.columns:
                    column_profile = ColumnProfile(
                        name=col.name,
                        original_name=col.name,  # Use same as name since we don't have original
                        data_type=(
                            ColumnType.CATEGORICAL
                            if col.data_type.value in ["string", "category"]
                            else (
                                ColumnType.NUMERIC
                                if col.data_type.value in ["int", "float", "numeric"]
                                else (
                                    ColumnType.DATETIME
                                    if col.data_type.value in ["datetime", "date"]
                                    else ColumnType.TEXT
                                )
                            )
                        ),
                        null_count=col.null_count,
                        null_percentage=(
                            (col.null_count / df.shape[0] * 100)
                            if df.shape[0] > 0
                            else 0
                        ),
                        unique_count=col.unique_count,
                        cardinality=col.unique_count,
                        sample_values=[],  # Empty for now
                        top_values=[],  # Empty for now
                        patterns=[],
                        is_id_like=False,
                    )
                    column_profiles.append(column_profile)

                # Categorize columns by type
                numeric_columns = [
                    col.name
                    for col in enhanced_analysis.columns
                    if col.data_type.value in ["int", "float", "numeric"]
                ]
                categorical_columns = [
                    col.name
                    for col in enhanced_analysis.columns
                    if col.data_type.value in ["string", "category"]
                ]
                datetime_columns = [
                    col.name
                    for col in enhanced_analysis.columns
                    if col.data_type.value in ["datetime", "date"]
                ]

                # Create basic DataProfile
                profile = DataProfile(
                    dataset_id=dataset_id,
                    total_rows=df.shape[0],
                    total_columns=df.shape[1],
                    columns=column_profiles,
                    numeric_columns=numeric_columns,
                    categorical_columns=categorical_columns,
                    datetime_columns=datetime_columns,
                    boolean_columns=[],
                    text_columns=[
                        col.name
                        for col in enhanced_analysis.columns
                        if col.data_type.value
                        not in [
                            "int",
                            "float",
                            "numeric",
                            "string",
                            "category",
                            "datetime",
                            "date",
                        ]
                    ],
                    has_datetime=len(datetime_columns) > 0,
                    has_numeric=len(numeric_columns) > 0,
                    has_categorical=len(categorical_columns) > 0,
                    potential_target_columns=[],
                    potential_id_columns=[],
                    overall_null_percentage=(
                        sum(col.null_count for col in enhanced_analysis.columns)
                        / (df.shape[0] * df.shape[1] * 100)
                        if df.shape[0] > 0 and df.shape[1] > 0
                        else 0
                    ),
                    high_cardinality_columns=[],
                    low_cardinality_columns=[],
                    correlations={},
                    profiled_at=datetime.utcnow(),
                    sample_size=df.shape[0],
                )

                logger.info(
                    f"Created basic DataProfile with {len(column_profiles)} columns"
                )

            # Convert enhanced analysis to DomainClassification object
            domain_mapping = {
                "ecommerce": DomainType.ECOMMERCE,
                "finance": DomainType.FINANCE,
                "manufacturing": DomainType.MANUFACTURING,
                "saas": DomainType.SAAS,
                "healthcare": DomainType.GENERIC,  # Map to generic for now
                "marketing": DomainType.GENERIC,  # Map to generic for now
                "hr": DomainType.GENERIC,  # Map to generic for now
                "logistics": DomainType.GENERIC,  # Map to generic for now
                "generic": DomainType.GENERIC,
            }

            domain_type = domain_mapping.get(
                enhanced_analysis.domain.value, DomainType.GENERIC
            )

            domain_info = DomainClassification(
                domain=domain_type,
                confidence=enhanced_analysis.confidence,
                reasoning=enhanced_analysis.description,
                rule_based_score=0.5,  # Default value
                llm_score=enhanced_analysis.confidence,
                detected_patterns=enhanced_analysis.key_insights,
                suggested_kpis=enhanced_analysis.suggested_kpis,
                classified_at=datetime.utcnow(),
            )

            # Update status
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.PROCESSING,
                "Generating dashboard configuration...",
                0.7,
            )

            # Generate dashboard
            dashboard_config = await self.dashboard_curator.generate_dashboard(
                profile, domain_info
            )

            # Update status
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.PROCESSING,
                "Saving results and optimizing queries...",
                0.9,
            )

            # Save results
            results = {
                "dataset_id": dataset_id,
                "profile": profile.dict(),
                "domain_info": domain_info.dict(),
                "dashboard_config": dashboard_config.dict(),
                "processed_at": datetime.utcnow().isoformat(),
                "sample_size": sample_size,
                "total_rows": len(df) if hasattr(df, "__len__") else None,
            }

            # Cache results
            await cache_service.set(
                f"analytics:{dataset_id}", results, ttl=settings.cache_ttl_seconds
            )

            # Final status update
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.COMPLETED,
                "Processing completed successfully",
                1.0,
            )

            logger.info(f"CSV processing completed for {dataset_id}")
            return results

        except Exception as e:
            logger.error(f"Error processing CSV for {dataset_id}: {e}", exc_info=True)
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.FAILED,
                f"Processing failed: {str(e)}",
                error_details=str(e),
            )
            raise

    async def _load_csv_data(self, file_path: str, sample_size: int) -> pd.DataFrame:
        """
        Load CSV data with sampling.

        Args:
            file_path: Path to the CSV file
            sample_size: Maximum number of rows to load

        Returns:
            DataFrame with loaded data
        """
        import chardet

        # First, detect encoding
        encoding = "utf-8"
        try:
            with open(file_path, "rb") as f:
                raw_data = f.read(10000)  # Read first 10KB to detect encoding
                result = chardet.detect(raw_data)
                if result["encoding"]:
                    encoding = result["encoding"]
                    logger.info(f"Detected encoding: {encoding}")
        except Exception as e:
            logger.warning(f"Could not detect encoding, using utf-8: {e}")

        # Try multiple approaches to load the CSV
        approaches = [
            # Approach 1: Polars with detected encoding
            lambda: pl.read_csv(file_path, n_rows=sample_size, encoding=encoding),
            # Approach 2: Polars with utf-8
            lambda: pl.read_csv(file_path, n_rows=sample_size, encoding="utf-8"),
            # Approach 3: Polars with auto-detection
            lambda: pl.read_csv(file_path, n_rows=sample_size),
        ]

        for i, approach in enumerate(approaches):
            try:
                logger.info(f"Trying approach {i+1} to load CSV...")
                df_pl = approach()
                df = df_pl.to_pandas()
                logger.info(
                    f"Successfully loaded {len(df)} rows and {len(df.columns)} columns"
                )
                return df
            except Exception as e:
                logger.warning(f"Approach {i+1} failed: {e}")
                continue

        # Fallback to pandas with various options
        pandas_approaches = [
            # Standard pandas
            lambda: pd.read_csv(file_path, nrows=sample_size, encoding=encoding),
            # Try different separators
            lambda: pd.read_csv(
                file_path, nrows=sample_size, encoding=encoding, sep=";"
            ),
            lambda: pd.read_csv(
                file_path, nrows=sample_size, encoding=encoding, sep="\t"
            ),
            # Try with different quote chars
            lambda: pd.read_csv(
                file_path, nrows=sample_size, encoding=encoding, quotechar='"'
            ),
            lambda: pd.read_csv(
                file_path, nrows=sample_size, encoding=encoding, quotechar="'"
            ),
            # Try with error handling
            lambda: pd.read_csv(
                file_path, nrows=sample_size, encoding=encoding, on_bad_lines="skip"
            ),
            # Last resort - latin-1 encoding (handles most files)
            lambda: pd.read_csv(file_path, nrows=sample_size, encoding="latin-1"),
        ]

        for i, approach in enumerate(pandas_approaches):
            try:
                logger.info(f"Trying pandas approach {i+1}...")
                df = approach()
                logger.info(
                    f"Fallback: Loaded {len(df)} rows and {len(df.columns)} columns"
                )
                return df
            except Exception as e:
                logger.warning(f"Pandas approach {i+1} failed: {e}")
                continue

        # If all approaches fail, raise an error
        raise Exception("Unable to load CSV file with any approach")

    async def get_processing_status(
        self, dataset_id: str
    ) -> Optional[ProcessingStatusResponse]:
        """
        Get the processing status for a dataset.

        Args:
            dataset_id: Unique identifier for the dataset

        Returns:
            Processing status or None if not found
        """
        # First check in-memory cache
        status = self.processing_status.get(dataset_id)
        if status:
            return status
            
        # If not in memory, check persistent cache
        try:
            cached_status = await cache_service.get(f"status:{dataset_id}")
            if cached_status:
                # Convert cache data back to ProcessingStatusResponse
                from datetime import datetime
                return ProcessingStatusResponse(
                    dataset_id=dataset_id,
                    status=ProcessingStatus(cached_status["status"]),
                    message=cached_status.get("message", ""),
                    progress=cached_status.get("progress", 0.0),
                    created_at=datetime.fromisoformat(cached_status["created_at"].replace("Z", "+00:00")),
                    updated_at=datetime.fromisoformat(cached_status["updated_at"].replace("Z", "+00:00")),
                    completion_time=datetime.fromisoformat(cached_status["completion_time"].replace("Z", "+00:00")) if cached_status.get("completion_time") else None,
                    error_details=cached_status.get("error_details")
                )
        except Exception as e:
            logger.error(f"Error loading status from cache for {dataset_id}: {e}")
            
        return None

    async def update_processing_status(
        self,
        dataset_id: str,
        status: ProcessingStatus,
        message: str,
        progress: Optional[float] = None,
        error_details: Optional[str] = None,
    ):
        """
        Update the processing status for a dataset.

        Args:
            dataset_id: Unique identifier for the dataset
            status: Current processing status
            message: Status message
            progress: Processing progress (0.0 to 1.0)
            error_details: Error details if status is FAILED
        """
        now = datetime.utcnow()

        if dataset_id in self.processing_status:
            # Update existing status
            current_status = self.processing_status[dataset_id]
            current_status.status = status
            current_status.message = message
            current_status.progress = progress
            current_status.updated_at = now
            current_status.error_details = error_details

            if status == ProcessingStatus.COMPLETED:
                current_status.completion_time = now
        else:
            # Create new status
            self.processing_status[dataset_id] = ProcessingStatusResponse(
                dataset_id=dataset_id,
                status=status,
                message=message,
                progress=progress,
                created_at=now,
                updated_at=now,
                completion_time=now if status == ProcessingStatus.COMPLETED else None,
                error_details=error_details,
            )

        logger.info(f"Status updated for {dataset_id}: {status} - {message}")

    async def get_analytics_results(self, dataset_id: str) -> Optional[Dict[str, Any]]:
        """
        Get cached analytics results for a dataset.

        Args:
            dataset_id: Unique identifier for the dataset

        Returns:
            Analytics results or None if not found
        """
        try:
            # First try the standard cache lookup
            results = await cache_service.get(f"analytics:{dataset_id}")
            if results:
                return results
            
            # Fallback: try the legacy cache key format
            results = await cache_service.get(f"{dataset_id}_analytics")
            if results:
                return results
                
            # Fallback: try to read direct JSON file (for datasets that were cached directly)
            import json
            import aiofiles
            from pathlib import Path
            
            json_file_path = Path(f"./data/cache/{dataset_id}_analytics.json")
            if json_file_path.exists():
                try:
                    async with aiofiles.open(json_file_path, "r", encoding="utf-8") as f:
                        content = await f.read()
                        results = json.loads(content)
                        logger.info(f"Loaded analytics results from direct JSON file for {dataset_id}")
                        
                        # Optionally re-cache it in the proper format for future use
                        await cache_service.set(f"analytics:{dataset_id}", results, ttl=settings.cache_ttl_seconds)
                        
                        return results
                except Exception as e:
                    logger.error(f"Failed to read JSON cache file for {dataset_id}: {e}")
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get analytics results for {dataset_id}: {e}")
            return None

    async def query_data(
        self, dataset_id: str, query: str, file_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query on the dataset using DuckDB with proper error handling.

        Args:
            dataset_id: Unique identifier for the dataset
            query: SQL query to execute
            file_path: Optional path to the CSV file

        Returns:
            Query results with sanitized data
        """
        conn = None
        try:
            logger.info(f"Executing query for dataset {dataset_id}: {query}")

            # Initialize DuckDB connection
            conn = duckdb.connect()

            if file_path:
                # Register CSV file as a table
                conn.execute(
                    f"""
                    CREATE TABLE dataset AS 
                    SELECT * FROM read_csv_auto('{file_path}')
                """
                )
                logger.info(f"Registered CSV file as table: {file_path}")

            # Execute query with error handling
            try:
                result = conn.execute(query).fetchdf()
                logger.info(f"Query executed successfully, returned {len(result)} rows")
            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                logger.error(f"Failed query: {query}")
                raise

            # Sanitize the result to handle NaN, infinity, and other problematic values
            sanitized_result = self._sanitize_dataframe(result)

            # Convert to JSON-serializable format
            query_results = {
                "columns": sanitized_result.columns.tolist(),
                "data": sanitized_result.to_dict("records"),
                "row_count": len(sanitized_result),
                "query": query,
            }

            # Validate the result can be JSON serialized
            try:
                import json

                json.dumps(query_results, default=str)
                logger.info(f"Query results validated for JSON serialization")
            except (ValueError, TypeError) as e:
                logger.error(f"Query results contain non-JSON serializable data: {e}")
                # Apply more aggressive sanitization
                query_results = self._aggressive_sanitize_results(query_results)

            return query_results

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Dataset ID: {dataset_id}")
            logger.error(f"Query: {query}")
            logger.error(f"File path: {file_path}")
            raise
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass

    def _sanitize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitize DataFrame to handle NaN, infinity, and other problematic values.

        Args:
            df: Input DataFrame

        Returns:
            Sanitized DataFrame safe for JSON serialization
        """
        try:
            logger.info(f"Sanitizing DataFrame with shape {df.shape}")

            # Create a copy to avoid modifying the original
            sanitized_df = df.copy()

            # Handle numeric columns
            numeric_columns = sanitized_df.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                # Replace NaN with None (which becomes null in JSON)
                sanitized_df[col] = sanitized_df[col].replace([np.nan], None)

                # Replace infinity with None
                sanitized_df[col] = sanitized_df[col].replace([np.inf, -np.inf], None)

                # Convert numpy types to Python types for JSON serialization
                if sanitized_df[col].dtype == np.int64:
                    sanitized_df[col] = sanitized_df[col].astype(
                        "Int64"
                    )  # Pandas nullable integer
                elif sanitized_df[col].dtype in [np.float64, np.float32]:
                    sanitized_df[col] = sanitized_df[col].astype(float)

            # Handle text columns
            text_columns = sanitized_df.select_dtypes(include=[object]).columns
            for col in text_columns:
                # Replace NaN with None
                sanitized_df[col] = sanitized_df[col].replace([np.nan], None)

                # Ensure all values are strings or None
                sanitized_df[col] = sanitized_df[col].apply(
                    lambda x: str(x) if x is not None and pd.notna(x) else None
                )

            # Handle datetime columns
            datetime_columns = sanitized_df.select_dtypes(
                include=[np.datetime64]
            ).columns
            for col in datetime_columns:
                # Convert to ISO format strings
                sanitized_df[col] = sanitized_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
                sanitized_df[col] = sanitized_df[col].replace(["NaT"], None)

            logger.info(f"DataFrame sanitization completed successfully")
            return sanitized_df

        except Exception as e:
            logger.error(f"Error sanitizing DataFrame: {e}")
            # Return empty DataFrame with same structure as fallback
            return pd.DataFrame(columns=df.columns)

    def _aggressive_sanitize_results(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply aggressive sanitization when regular sanitization fails.

        Args:
            results: Query results dictionary

        Returns:
            Aggressively sanitized results
        """
        try:
            logger.warning("Applying aggressive sanitization to query results")

            sanitized_data = []
            for row in results.get("data", []):
                sanitized_row = {}
                for key, value in row.items():
                    try:
                        # Convert problematic values to strings
                        if pd.isna(value) or value is None:
                            sanitized_row[key] = None
                        elif isinstance(value, (np.floating, float)) and (
                            np.isnan(value) or np.isinf(value)
                        ):
                            sanitized_row[key] = None
                        elif isinstance(value, (np.integer, int)):
                            sanitized_row[key] = int(value)
                        elif isinstance(value, (np.floating, float)):
                            sanitized_row[key] = float(value)
                        else:
                            sanitized_row[key] = str(value)
                    except:
                        # Ultimate fallback - convert to string
                        sanitized_row[key] = str(value) if value is not None else None

                sanitized_data.append(sanitized_row)

            return {
                "columns": results.get("columns", []),
                "data": sanitized_data,
                "row_count": len(sanitized_data),
                "query": results.get("query", ""),
            }

        except Exception as e:
            logger.error(f"Aggressive sanitization failed: {e}")
            # Return minimal safe result
            return {
                "columns": [],
                "data": [],
                "row_count": 0,
                "query": results.get("query", ""),
                "error": "Data sanitization failed",
            }

    async def get_data_sample(
        self, dataset_id: str, limit: int = 100
    ) -> Optional[Dict[str, Any]]:
        """
        Get a sample of the data for preview.

        Args:
            dataset_id: Unique identifier for the dataset
            limit: Number of rows to return

        Returns:
            Data sample or None if not found
        """
        try:
            results = await self.get_analytics_results(dataset_id)
            if not results:
                return None

            # Get the file path and query the data
            from backend.services.file_service import file_service

            file_path = await file_service.get_file_path(dataset_id)

            if not file_path:
                return None

            query = f"SELECT * FROM dataset LIMIT {limit}"
            sample_data = await self.query_data(dataset_id, query, file_path)

            return sample_data

        except Exception as e:
            logger.error(f"Failed to get data sample for {dataset_id}: {e}")
            return None


# Global analytics service instance
analytics_service = AnalyticsService()
