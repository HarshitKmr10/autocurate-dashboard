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

from backend.config import get_settings
from backend.schemas.upload import ProcessingStatus, ProcessingStatusResponse
from backend.core.profiler.data_profiler import DataProfiler
from backend.core.domain.detector import DomainDetector, DomainClassification, DomainType
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
        self,
        dataset_id: str,
        file_path: str,
        sample_size: int = 1000
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
                0.1
            )
            
            # Load data
            df = await self._load_csv_data(file_path, sample_size)
            
            # Update status
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.PROCESSING,
                "Profiling data structure and types...",
                0.3
            )
            
            # Use enhanced CSV analyzer
            filename = Path(file_path).name
            enhanced_analysis = await csv_analyzer.analyze_csv(df, filename)
            
            # Update status
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.PROCESSING,
                "Generating insights and visualizations...",
                0.5
            )
            
            # Fallback to original profiler if needed
            try:
                profile = await self.data_profiler.profile_dataset(df, dataset_id)
            except Exception as e:
                logger.warning(f"Original profiler failed, using enhanced analysis: {e}")
                # Create a simplified profile from enhanced analysis
                profile = {
                    "columns": {col.name: {
                        "type": col.data_type.value,
                        "description": col.description,
                        "null_count": col.null_count,
                        "unique_count": col.unique_count
                    } for col in enhanced_analysis.columns},
                    "shape": df.shape,
                    "summary": enhanced_analysis.description
                }
            
            # Convert enhanced analysis to DomainClassification object
            domain_mapping = {
                "ecommerce": DomainType.ECOMMERCE,
                "finance": DomainType.FINANCE,
                "manufacturing": DomainType.MANUFACTURING,
                "saas": DomainType.SAAS,
                "healthcare": DomainType.GENERIC,  # Map to generic for now
                "marketing": DomainType.GENERIC,   # Map to generic for now
                "hr": DomainType.GENERIC,          # Map to generic for now
                "logistics": DomainType.GENERIC,   # Map to generic for now
                "generic": DomainType.GENERIC
            }
            
            domain_type = domain_mapping.get(enhanced_analysis.domain.value, DomainType.GENERIC)
            
            domain_info = DomainClassification(
                domain=domain_type,
                confidence=enhanced_analysis.confidence,
                reasoning=enhanced_analysis.description,
                rule_based_score=0.5,  # Default value
                llm_score=enhanced_analysis.confidence,
                detected_patterns=enhanced_analysis.key_insights,
                suggested_kpis=enhanced_analysis.suggested_kpis,
                classified_at=datetime.now()
            )
            
            # Update status
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.PROCESSING,
                "Generating dashboard configuration...",
                0.7
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
                0.9
            )
            
            # Save results
            results = {
                "dataset_id": dataset_id,
                "profile": profile.dict(),
                "domain_info": domain_info.dict(),
                "dashboard_config": dashboard_config.dict(),
                "processed_at": datetime.utcnow().isoformat(),
                "sample_size": sample_size,
                "total_rows": len(df) if hasattr(df, '__len__') else None
            }
            
            # Cache results
            await cache_service.set(
                f"analytics:{dataset_id}",
                results,
                ttl=settings.cache_ttl_seconds
            )
            
            # Final status update
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.COMPLETED,
                "Processing completed successfully",
                1.0
            )
            
            logger.info(f"CSV processing completed for {dataset_id}")
            return results
            
        except Exception as e:
            logger.error(f"Error processing CSV for {dataset_id}: {e}", exc_info=True)
            await self.update_processing_status(
                dataset_id,
                ProcessingStatus.FAILED,
                f"Processing failed: {str(e)}",
                error_details=str(e)
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
        encoding = 'utf-8'
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(10000)  # Read first 10KB to detect encoding
                result = chardet.detect(raw_data)
                if result['encoding']:
                    encoding = result['encoding']
                    logger.info(f"Detected encoding: {encoding}")
        except Exception as e:
            logger.warning(f"Could not detect encoding, using utf-8: {e}")
        
        # Try multiple approaches to load the CSV
        approaches = [
            # Approach 1: Polars with detected encoding
            lambda: pl.read_csv(file_path, n_rows=sample_size, encoding=encoding),
            # Approach 2: Polars with utf-8
            lambda: pl.read_csv(file_path, n_rows=sample_size, encoding='utf-8'),
            # Approach 3: Polars with auto-detection
            lambda: pl.read_csv(file_path, n_rows=sample_size),
        ]
        
        for i, approach in enumerate(approaches):
            try:
                logger.info(f"Trying approach {i+1} to load CSV...")
                df_pl = approach()
                df = df_pl.to_pandas()
                logger.info(f"Successfully loaded {len(df)} rows and {len(df.columns)} columns")
                return df
            except Exception as e:
                logger.warning(f"Approach {i+1} failed: {e}")
                continue
        
        # Fallback to pandas with various options
        pandas_approaches = [
            # Standard pandas
            lambda: pd.read_csv(file_path, nrows=sample_size, encoding=encoding),
            # Try different separators
            lambda: pd.read_csv(file_path, nrows=sample_size, encoding=encoding, sep=';'),
            lambda: pd.read_csv(file_path, nrows=sample_size, encoding=encoding, sep='\t'),
            # Try with different quote chars
            lambda: pd.read_csv(file_path, nrows=sample_size, encoding=encoding, quotechar='"'),
            lambda: pd.read_csv(file_path, nrows=sample_size, encoding=encoding, quotechar="'"),
            # Try with error handling
            lambda: pd.read_csv(file_path, nrows=sample_size, encoding=encoding, on_bad_lines='skip'),
            # Last resort - latin-1 encoding (handles most files)
            lambda: pd.read_csv(file_path, nrows=sample_size, encoding='latin-1'),
        ]
        
        for i, approach in enumerate(pandas_approaches):
            try:
                logger.info(f"Trying pandas approach {i+1}...")
                df = approach()
                logger.info(f"Fallback: Loaded {len(df)} rows and {len(df.columns)} columns")
                return df
            except Exception as e:
                logger.warning(f"Pandas approach {i+1} failed: {e}")
                continue
        
        # If all approaches fail, raise an error
        raise Exception("Unable to load CSV file with any approach")
    
    async def get_processing_status(self, dataset_id: str) -> Optional[ProcessingStatusResponse]:
        """
        Get the processing status for a dataset.
        
        Args:
            dataset_id: Unique identifier for the dataset
            
        Returns:
            Processing status or None if not found
        """
        return self.processing_status.get(dataset_id)
    
    async def update_processing_status(
        self,
        dataset_id: str,
        status: ProcessingStatus,
        message: str,
        progress: Optional[float] = None,
        error_details: Optional[str] = None
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
                error_details=error_details
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
            results = await cache_service.get(f"analytics:{dataset_id}")
            return results
        except Exception as e:
            logger.error(f"Failed to get analytics results for {dataset_id}: {e}")
            return None
    
    async def query_data(
        self,
        dataset_id: str,
        query: str,
        file_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Execute a SQL query on the dataset using DuckDB.
        
        Args:
            dataset_id: Unique identifier for the dataset
            query: SQL query to execute
            file_path: Optional path to the CSV file
            
        Returns:
            Query results
        """
        try:
            # Initialize DuckDB connection
            conn = duckdb.connect()
            
            if file_path:
                # Register CSV file as a table
                conn.execute(f"""
                    CREATE TABLE dataset AS 
                    SELECT * FROM read_csv_auto('{file_path}')
                """)
            
            # Execute query
            result = conn.execute(query).fetchdf()
            
            # Convert to JSON-serializable format
            return {
                "columns": result.columns.tolist(),
                "data": result.to_dict('records'),
                "row_count": len(result),
                "query": query
            }
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()
    
    async def get_data_sample(
        self,
        dataset_id: str,
        limit: int = 100
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