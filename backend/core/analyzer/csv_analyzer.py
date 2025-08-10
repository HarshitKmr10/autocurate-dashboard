"""Enhanced CSV analyzer with OpenAI structured output."""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
import logging
from pydantic import BaseModel, Field
from enum import Enum
import asyncio

from backend.core.llm.client import llm_client

logger = logging.getLogger(__name__)


class DataType(str, Enum):
    """Data type enumeration."""
    NUMERIC = "numeric"
    TEXT = "text"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    CATEGORICAL = "categorical"
    EMAIL = "email"
    URL = "url"
    CURRENCY = "currency"
    PERCENTAGE = "percentage"
    ID = "id"


class BusinessDomain(str, Enum):
    """Business domain enumeration."""
    ECOMMERCE = "ecommerce"
    FINANCE = "finance"
    SAAS = "saas"
    MANUFACTURING = "manufacturing"
    HEALTHCARE = "healthcare"
    MARKETING = "marketing"
    HR = "hr"
    LOGISTICS = "logistics"
    GENERIC = "generic"


class ColumnAnalysis(BaseModel):
    """Analysis result for a single column."""
    name: str
    data_type: DataType
    description: str
    business_meaning: Optional[str] = None
    sample_values: List[str] = Field(max_length=5)
    null_count: int
    unique_count: int
    is_key_field: bool = False
    
    class Config:
        extra = "forbid"


class DatasetAnalysis(BaseModel):
    """Complete dataset analysis."""
    domain: BusinessDomain
    confidence: float = Field(ge=0.0, le=1.0)
    description: str
    columns: List[ColumnAnalysis]
    suggested_kpis: List[str] = Field(max_length=10)
    key_insights: List[str] = Field(max_length=5)
    potential_visualizations: List[str] = Field(max_length=8)
    
    class Config:
        extra = "forbid"


class EnhancedCSVAnalyzer:
    """Enhanced CSV analyzer using OpenAI for intelligent analysis."""
    
    def __init__(self):
        self.llm_client = llm_client
    
    async def analyze_csv(self, df: pd.DataFrame, filename: str) -> DatasetAnalysis:
        """Analyze CSV data using OpenAI structured output."""
        try:
            logger.info(f"Starting enhanced CSV analysis for {filename}")
            
            # Basic data profiling
            basic_info = self._get_basic_info(df)
            
            # Sample data for AI analysis
            sample_data = self._get_sample_data(df)
            
            # Get AI analysis
            analysis = await self._get_ai_analysis(
                basic_info=basic_info,
                sample_data=sample_data,
                filename=filename
            )
            
            logger.info(f"Analysis completed. Domain: {analysis.domain}, Confidence: {analysis.confidence}")
            return analysis
            
        except Exception as e:
            logger.error(f"Error in CSV analysis: {e}")
            # Fallback to basic analysis
            return self._fallback_analysis(df, filename)
    
    def _get_basic_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get basic information about the dataset."""
        return {
            "shape": df.shape,
            "columns": list(df.columns),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "null_counts": df.isnull().sum().to_dict(),
            "unique_counts": df.nunique().to_dict()
        }
    
    def _get_sample_data(self, df: pd.DataFrame, n_rows: int = 5) -> Dict[str, Any]:
        """Get sample data for AI analysis."""
        # Get first few rows
        sample_df = df.head(n_rows)
        
        # Convert to a clean format for AI
        sample_dict = {}
        for col in df.columns:
            sample_values = sample_df[col].dropna().astype(str).tolist()
            sample_dict[col] = sample_values[:3]  # Max 3 samples per column
        
        return sample_dict
    
    async def _get_ai_analysis(
        self, 
        basic_info: Dict[str, Any], 
        sample_data: Dict[str, Any],
        filename: str
    ) -> DatasetAnalysis:
        """Get AI-powered analysis using structured output."""
        
        # Prepare prompt
        prompt = f"""
        Analyze this CSV dataset and provide structured insights:
        
        Filename: {filename}
        Shape: {basic_info['shape'][0]} rows, {basic_info['shape'][1]} columns
        
        Columns and sample data:
        """
        
        for col, samples in sample_data.items():
            null_count = basic_info['null_counts'].get(col, 0)
            unique_count = basic_info['unique_counts'].get(col, 0)
            prompt += f"\n- {col}: {samples} (nulls: {null_count}, unique: {unique_count})"
        
        prompt += """
        
        Please analyze this data and determine:
        1. The business domain this data belongs to
        2. The data type and business meaning of each column
        3. Suggested KPIs that would be relevant for this domain
        4. Key insights about the data structure
        5. Appropriate visualization types for this data
        
        Be specific and practical in your analysis.
        """
        
        try:
            # Use OpenAI with structured output
            response = await self.llm_client.generate_structured_response(
                prompt=prompt,
                response_model=DatasetAnalysis,
                temperature=0.3
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Error getting AI analysis: {e}")
            # Fallback if AI fails
            return self._fallback_analysis_from_basic_info(basic_info, sample_data, filename)
    
    def _fallback_analysis(self, df: pd.DataFrame, filename: str) -> DatasetAnalysis:
        """Fallback analysis without AI."""
        logger.warning("Using fallback analysis without AI")
        
        basic_info = self._get_basic_info(df)
        sample_data = self._get_sample_data(df)
        
        return self._fallback_analysis_from_basic_info(basic_info, sample_data, filename)
    
    def _fallback_analysis_from_basic_info(
        self, 
        basic_info: Dict[str, Any], 
        sample_data: Dict[str, Any],
        filename: str
    ) -> DatasetAnalysis:
        """Create basic analysis from basic info."""
        
        columns = []
        for col in basic_info['columns']:
            # Simple type inference
            dtype = basic_info['dtypes'].get(col, 'object')
            sample_values = sample_data.get(col, [])
            
            if 'int' in dtype or 'float' in dtype:
                data_type = DataType.NUMERIC
            elif 'datetime' in dtype:
                data_type = DataType.DATETIME
            elif any(keyword in col.lower() for keyword in ['email', 'mail']):
                data_type = DataType.EMAIL
            elif any(keyword in col.lower() for keyword in ['id', 'key', 'uuid']):
                data_type = DataType.ID
            else:
                data_type = DataType.TEXT
            
            columns.append(ColumnAnalysis(
                name=col,
                data_type=data_type,
                description=f"Column containing {data_type.value} data",
                sample_values=[str(v) for v in sample_values[:3]],
                null_count=basic_info['null_counts'].get(col, 0),
                unique_count=basic_info['unique_counts'].get(col, 0)
            ))
        
        # Guess domain based on column names
        domain = self._guess_domain_from_columns(basic_info['columns'])
        
        return DatasetAnalysis(
            domain=domain,
            confidence=0.5,  # Low confidence for fallback
            description=f"Dataset with {len(columns)} columns analyzed without AI assistance",
            columns=columns,
            suggested_kpis=["Row Count", "Column Count", "Data Quality"],
            key_insights=["Basic data structure analysis performed"],
            potential_visualizations=["Table", "Summary Statistics"]
        )
    
    def _guess_domain_from_columns(self, columns: List[str]) -> BusinessDomain:
        """Guess business domain from column names."""
        col_text = " ".join(columns).lower()
        
        if any(keyword in col_text for keyword in ['order', 'product', 'customer', 'price', 'quantity']):
            return BusinessDomain.ECOMMERCE
        elif any(keyword in col_text for keyword in ['transaction', 'account', 'balance', 'amount']):
            return BusinessDomain.FINANCE
        elif any(keyword in col_text for keyword in ['user', 'subscription', 'feature', 'plan']):
            return BusinessDomain.SAAS
        elif any(keyword in col_text for keyword in ['production', 'quality', 'defect', 'efficiency']):
            return BusinessDomain.MANUFACTURING
        else:
            return BusinessDomain.GENERIC


# Global analyzer instance
csv_analyzer = EnhancedCSVAnalyzer()