"""Data profiling engine for analyzing CSV datasets."""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import logging
from pydantic import BaseModel
from enum import Enum

logger = logging.getLogger(__name__)


class ColumnType(str, Enum):
    """Enumeration of column data types."""
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    TEXT = "text"
    MIXED = "mixed"


class ColumnProfile(BaseModel):
    """Profile information for a single column."""
    name: str
    original_name: str
    data_type: ColumnType
    null_count: int
    null_percentage: float
    unique_count: int
    cardinality: int
    
    # Type-specific statistics
    min_value: Optional[Union[str, float, int]] = None
    max_value: Optional[Union[str, float, int]] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_value: Optional[float] = None
    
    # String-specific statistics
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    
    # Sample values
    sample_values: List[Any] = []
    top_values: List[Dict[str, Any]] = []
    
    # Pattern information
    patterns: List[str] = []
    is_id_like: bool = False
    is_email_like: bool = False
    is_phone_like: bool = False
    is_url_like: bool = False


class DataProfile(BaseModel):
    """Complete profile of a dataset."""
    dataset_id: str
    total_rows: int
    total_columns: int
    
    # Column profiles
    columns: List[ColumnProfile]
    
    # Data type counts
    numeric_columns: List[str] = []
    categorical_columns: List[str] = []
    datetime_columns: List[str] = []
    boolean_columns: List[str] = []
    text_columns: List[str] = []
    
    # Dataset characteristics
    has_datetime: bool = False
    has_numeric: bool = False
    has_categorical: bool = False
    potential_target_columns: List[str] = []
    potential_id_columns: List[str] = []
    
    # Quality metrics
    overall_null_percentage: float
    high_cardinality_columns: List[str] = []
    low_cardinality_columns: List[str] = []
    
    # Correlation information (for numeric columns)
    correlations: Dict[str, Dict[str, float]] = {}
    
    # Profile metadata
    profiled_at: datetime
    sample_size: int


class DataProfiler:
    """Main data profiling engine."""
    
    def __init__(self):
        self.max_sample_values = 10
        self.max_top_values = 10
        self.high_cardinality_threshold = 0.8
        self.low_cardinality_threshold = 0.1
    
    async def profile_dataset(self, df: pd.DataFrame, dataset_id: str) -> DataProfile:
        """
        Generate a comprehensive profile of the dataset.
        
        Args:
            df: DataFrame to profile
            dataset_id: Unique identifier for the dataset
            
        Returns:
            DataProfile with comprehensive analysis
        """
        logger.info(f"Starting data profiling for dataset {dataset_id}")
        
        # Basic dataset information
        total_rows, total_columns = df.shape
        
        # Profile each column
        column_profiles = []
        for col in df.columns:
            profile = await self._profile_column(df[col], col)
            column_profiles.append(profile)
        
        # Categorize columns by type
        type_categorization = self._categorize_columns_by_type(column_profiles)
        
        # Calculate correlations for numeric columns
        correlations = self._calculate_correlations(df, type_categorization['numeric'])
        
        # Identify special columns
        potential_targets = self._identify_potential_targets(column_profiles)
        potential_ids = self._identify_potential_ids(column_profiles)
        
        # Calculate quality metrics
        overall_null_percentage = df.isnull().sum().sum() / (total_rows * total_columns) * 100
        
        # Identify high/low cardinality columns
        high_cardinality = []
        low_cardinality = []
        
        for profile in column_profiles:
            cardinality_ratio = profile.unique_count / total_rows if total_rows > 0 else 0
            
            if cardinality_ratio > self.high_cardinality_threshold:
                high_cardinality.append(profile.name)
            elif cardinality_ratio < self.low_cardinality_threshold:
                low_cardinality.append(profile.name)
        
        # Create comprehensive profile
        data_profile = DataProfile(
            dataset_id=dataset_id,
            total_rows=total_rows,
            total_columns=total_columns,
            columns=column_profiles,
            numeric_columns=type_categorization['numeric'],
            categorical_columns=type_categorization['categorical'],
            datetime_columns=type_categorization['datetime'],
            boolean_columns=type_categorization['boolean'],
            text_columns=type_categorization['text'],
            has_datetime=len(type_categorization['datetime']) > 0,
            has_numeric=len(type_categorization['numeric']) > 0,
            has_categorical=len(type_categorization['categorical']) > 0,
            potential_target_columns=potential_targets,
            potential_id_columns=potential_ids,
            overall_null_percentage=overall_null_percentage,
            high_cardinality_columns=high_cardinality,
            low_cardinality_columns=low_cardinality,
            correlations=correlations,
            profiled_at=datetime.utcnow(),
            sample_size=len(df)
        )
        
        logger.info(f"Data profiling completed for dataset {dataset_id}")
        return data_profile
    
    async def _profile_column(self, series: pd.Series, column_name: str) -> ColumnProfile:
        """
        Profile a single column.
        
        Args:
            series: Column data as pandas Series
            column_name: Name of the column
            
        Returns:
            ColumnProfile with detailed analysis
        """
        # Basic statistics
        total_count = len(series)
        null_count = series.isnull().sum()
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
        unique_count = series.nunique()
        cardinality = unique_count
        
        # Infer data type
        data_type = self._infer_column_type(series)
        
        # Get sample values (non-null)
        non_null_series = series.dropna()
        sample_values = non_null_series.head(self.max_sample_values).tolist()
        
        # Get top values with counts
        top_values = []
        if not non_null_series.empty:
            value_counts = non_null_series.value_counts().head(self.max_top_values)
            top_values = [
                {"value": str(val), "count": int(count), "percentage": count / len(non_null_series) * 100}
                for val, count in value_counts.items()
            ]
        
        # Initialize profile
        profile = ColumnProfile(
            name=self._sanitize_column_name(column_name),
            original_name=column_name,
            data_type=data_type,
            null_count=null_count,
            null_percentage=null_percentage,
            unique_count=unique_count,
            cardinality=cardinality,
            sample_values=sample_values,
            top_values=top_values
        )
        
        # Type-specific analysis
        if data_type == ColumnType.NUMERIC:
            await self._analyze_numeric_column(series, profile)
        elif data_type == ColumnType.TEXT:
            await self._analyze_text_column(series, profile)
        elif data_type == ColumnType.DATETIME:
            await self._analyze_datetime_column(series, profile)
        
        # Pattern analysis
        await self._analyze_patterns(series, profile)
        
        return profile
    
    def _infer_column_type(self, series: pd.Series) -> ColumnType:
        """
        Infer the data type of a column.
        
        Args:
            series: Column data
            
        Returns:
            Inferred ColumnType
        """
        # Remove nulls for analysis
        non_null_series = series.dropna()
        
        if non_null_series.empty:
            return ColumnType.TEXT
        
        # Check for boolean
        if self._is_boolean_column(non_null_series):
            return ColumnType.BOOLEAN
        
        # Check for datetime
        if self._is_datetime_column(non_null_series):
            return ColumnType.DATETIME
        
        # Check for numeric
        if self._is_numeric_column(non_null_series):
            return ColumnType.NUMERIC
        
        # Check for categorical vs text
        if self._is_categorical_column(non_null_series):
            return ColumnType.CATEGORICAL
        
        return ColumnType.TEXT
    
    def _is_boolean_column(self, series: pd.Series) -> bool:
        """Check if column contains boolean-like data."""
        unique_values = set(str(val).lower() for val in series.unique())
        boolean_patterns = [
            {'true', 'false'},
            {'yes', 'no'},
            {'y', 'n'},
            {'1', '0'},
            {'t', 'f'},
            {'on', 'off'}
        ]
        
        return any(unique_values.issubset(pattern) for pattern in boolean_patterns)
    
    def _is_datetime_column(self, series: pd.Series) -> bool:
        """Check if column contains datetime data."""
        try:
            # Try to parse a sample as datetime
            sample = series.head(min(100, len(series)))
            pd.to_datetime(sample, errors='raise')
            return True
        except:
            return False
    
    def _is_numeric_column(self, series: pd.Series) -> bool:
        """Check if column contains numeric data."""
        try:
            pd.to_numeric(series, errors='raise')
            return True
        except:
            return False
    
    def _is_categorical_column(self, series: pd.Series) -> bool:
        """Check if column should be treated as categorical."""
        total_count = len(series)
        unique_count = series.nunique()
        
        # If unique count is very low relative to total, likely categorical
        if total_count > 0 and unique_count / total_count < 0.1:
            return True
        
        # If unique count is low in absolute terms, likely categorical
        if unique_count < 20:
            return True
        
        return False
    
    async def _analyze_numeric_column(self, series: pd.Series, profile: ColumnProfile):
        """Analyze numeric column specifics."""
        numeric_series = pd.to_numeric(series, errors='coerce').dropna()
        
        if not numeric_series.empty:
            profile.min_value = float(numeric_series.min())
            profile.max_value = float(numeric_series.max())
            profile.mean_value = float(numeric_series.mean())
            profile.median_value = float(numeric_series.median())
            profile.std_value = float(numeric_series.std())
    
    async def _analyze_text_column(self, series: pd.Series, profile: ColumnProfile):
        """Analyze text column specifics."""
        text_series = series.astype(str).dropna()
        
        if not text_series.empty:
            lengths = text_series.str.len()
            profile.min_length = int(lengths.min())
            profile.max_length = int(lengths.max())
            profile.avg_length = float(lengths.mean())
    
    async def _analyze_datetime_column(self, series: pd.Series, profile: ColumnProfile):
        """Analyze datetime column specifics."""
        try:
            datetime_series = pd.to_datetime(series, errors='coerce').dropna()
            
            if not datetime_series.empty:
                profile.min_value = datetime_series.min().isoformat()
                profile.max_value = datetime_series.max().isoformat()
        except:
            pass
    
    async def _analyze_patterns(self, series: pd.Series, profile: ColumnProfile):
        """Analyze patterns in the data."""
        text_series = series.astype(str).dropna()
        
        if text_series.empty:
            return
        
        # Check for ID-like patterns
        profile.is_id_like = self._check_id_pattern(text_series)
        
        # Check for email patterns
        profile.is_email_like = self._check_email_pattern(text_series)
        
        # Check for phone patterns
        profile.is_phone_like = self._check_phone_pattern(text_series)
        
        # Check for URL patterns
        profile.is_url_like = self._check_url_pattern(text_series)
    
    def _check_id_pattern(self, series: pd.Series) -> bool:
        """Check if column looks like an ID field."""
        sample = series.head(100)
        
        # Check for sequential numbers
        try:
            numeric_vals = pd.to_numeric(sample, errors='coerce').dropna()
            if len(numeric_vals) > 10:
                # Check if values are roughly sequential
                diff = numeric_vals.diff().dropna()
                if diff.median() == 1.0:
                    return True
        except:
            pass
        
        # Check for UUID-like patterns
        uuid_like = sample.str.match(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
        if uuid_like.sum() > len(sample) * 0.5:
            return True
        
        return False
    
    def _check_email_pattern(self, series: pd.Series) -> bool:
        """Check if column contains email addresses."""
        sample = series.head(100)
        email_like = sample.str.contains(r'@.*\.', na=False, regex=True)
        return email_like.sum() > len(sample) * 0.5
    
    def _check_phone_pattern(self, series: pd.Series) -> bool:
        """Check if column contains phone numbers."""
        sample = series.head(100)
        phone_like = sample.str.match(r'^[\+]?[1-9]?[\d\s\-\(\)\.]{7,15}$', na=False)
        return phone_like.sum() > len(sample) * 0.5
    
    def _check_url_pattern(self, series: pd.Series) -> bool:
        """Check if column contains URLs."""
        sample = series.head(100)
        url_like = sample.str.contains(r'https?://', na=False, regex=True)
        return url_like.sum() > len(sample) * 0.5
    
    def _categorize_columns_by_type(self, column_profiles: List[ColumnProfile]) -> Dict[str, List[str]]:
        """Categorize columns by their data types."""
        categorization = {
            'numeric': [],
            'categorical': [],
            'datetime': [],
            'boolean': [],
            'text': []
        }
        
        for profile in column_profiles:
            if profile.data_type == ColumnType.NUMERIC:
                categorization['numeric'].append(profile.name)
            elif profile.data_type == ColumnType.CATEGORICAL:
                categorization['categorical'].append(profile.name)
            elif profile.data_type == ColumnType.DATETIME:
                categorization['datetime'].append(profile.name)
            elif profile.data_type == ColumnType.BOOLEAN:
                categorization['boolean'].append(profile.name)
            else:
                categorization['text'].append(profile.name)
        
        return categorization
    
    def _calculate_correlations(self, df: pd.DataFrame, numeric_columns: List[str]) -> Dict[str, Dict[str, float]]:
        """Calculate correlations between numeric columns."""
        correlations = {}
        
        if len(numeric_columns) < 2:
            return correlations
        
        try:
            numeric_df = df[numeric_columns].select_dtypes(include=[np.number])
            corr_matrix = numeric_df.corr()
            
            for col1 in numeric_columns:
                if col1 in corr_matrix.columns:
                    correlations[col1] = {}
                    for col2 in numeric_columns:
                        if col2 in corr_matrix.columns and not pd.isna(corr_matrix.loc[col1, col2]):
                            correlations[col1][col2] = float(corr_matrix.loc[col1, col2])
        except Exception as e:
            logger.warning(f"Failed to calculate correlations: {e}")
        
        return correlations
    
    def _identify_potential_targets(self, column_profiles: List[ColumnProfile]) -> List[str]:
        """Identify columns that might be target variables."""
        potential_targets = []
        
        for profile in column_profiles:
            # Look for columns with names suggesting they are targets
            target_keywords = ['target', 'label', 'class', 'outcome', 'result', 'prediction']
            
            if any(keyword in profile.original_name.lower() for keyword in target_keywords):
                potential_targets.append(profile.name)
            
            # Look for binary categorical variables
            elif (profile.data_type == ColumnType.CATEGORICAL and 
                  profile.unique_count == 2 and 
                  profile.null_percentage < 10):
                potential_targets.append(profile.name)
        
        return potential_targets
    
    def _identify_potential_ids(self, column_profiles: List[ColumnProfile]) -> List[str]:
        """Identify columns that might be ID fields."""
        potential_ids = []
        
        for profile in column_profiles:
            # Check for ID-like patterns
            if profile.is_id_like:
                potential_ids.append(profile.name)
            
            # Check for high cardinality with ID-like names
            id_keywords = ['id', 'key', 'identifier', 'uuid', 'guid']
            if (any(keyword in profile.original_name.lower() for keyword in id_keywords) and
                profile.unique_count > profile.cardinality * 0.9):
                potential_ids.append(profile.name)
        
        return potential_ids
    
    def _sanitize_column_name(self, name: str) -> str:
        """Sanitize column name for safe processing."""
        # Convert to string and strip whitespace
        clean_name = str(name).strip()
        
        # Replace problematic characters
        clean_name = clean_name.replace(' ', '_').replace('-', '_')
        clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
        
        # Ensure it doesn't start with a number
        if clean_name and clean_name[0].isdigit():
            clean_name = f"col_{clean_name}"
        
        # Provide default if empty
        if not clean_name:
            clean_name = "unnamed_column"
        
        return clean_name