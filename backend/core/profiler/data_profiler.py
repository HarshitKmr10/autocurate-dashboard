"""Data profiling engine for analyzing CSV datasets."""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import logging
from pydantic import BaseModel
from enum import Enum
import re

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

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Comprehensive data cleaning and validation.

        Args:
            df: Raw DataFrame from CSV

        Returns:
            Cleaned DataFrame
        """
        try:
            logger.info(f"Starting data cleaning for DataFrame with shape {df.shape}")
            cleaned_df = df.copy()
            original_rows = len(cleaned_df)

            # 1. Remove completely empty rows and columns
            cleaned_df = cleaned_df.dropna(
                how="all"
            )  # Remove rows that are entirely NaN
            cleaned_df = cleaned_df.loc[
                :, ~cleaned_df.isnull().all()
            ]  # Remove columns that are entirely NaN

            logger.info(
                f"Removed empty rows/cols: {original_rows} -> {len(cleaned_df)} rows"
            )

            # 2. Clean column names
            cleaned_df.columns = self._clean_column_names(cleaned_df.columns)

            # 3. Handle numeric columns
            numeric_columns = cleaned_df.select_dtypes(include=[np.number]).columns
            for col in numeric_columns:
                cleaned_df[col] = self._clean_numeric_column(cleaned_df[col], col)

            # 4. Handle text columns
            text_columns = cleaned_df.select_dtypes(include=[object]).columns
            for col in text_columns:
                cleaned_df[col] = self._clean_text_column(cleaned_df[col], col)

            # 5. Detect and clean date columns
            for col in text_columns:
                if self._is_date_column(cleaned_df[col]):
                    try:
                        cleaned_df[col] = pd.to_datetime(
                            cleaned_df[col], errors="coerce"
                        )
                        logger.info(f"Converted column '{col}' to datetime")
                    except Exception as e:
                        logger.warning(f"Failed to convert '{col}' to datetime: {e}")

            # 6. Remove rows with too many missing values (>80% missing)
            missing_threshold = 0.8
            rows_before = len(cleaned_df)
            cleaned_df = cleaned_df.dropna(
                thresh=int(len(cleaned_df.columns) * (1 - missing_threshold))
            )
            rows_after = len(cleaned_df)

            if rows_before != rows_after:
                logger.info(
                    f"Removed {rows_before - rows_after} rows with >80% missing data"
                )

            # 7. Ensure minimum data quality
            if len(cleaned_df) < 10:
                logger.warning(
                    f"Very few rows remaining after cleaning: {len(cleaned_df)}"
                )

            # 8. Final validation
            self._validate_cleaned_data(cleaned_df)

            logger.info(
                f"Data cleaning completed: {original_rows} -> {len(cleaned_df)} rows, {len(cleaned_df.columns)} columns"
            )
            return cleaned_df

        except Exception as e:
            logger.error(f"Error during data cleaning: {e}")
            # Return original data if cleaning fails
            return df

    def _clean_column_names(self, columns) -> List[str]:
        """Clean and standardize column names."""
        cleaned_names = []
        seen_names = set()

        for col in columns:
            # Convert to string and clean
            clean_name = str(col).strip()

            # Remove special characters and spaces
            clean_name = re.sub(r"[^\w\s]", "", clean_name)
            clean_name = re.sub(r"\s+", "_", clean_name)
            clean_name = clean_name.lower()

            # Ensure it starts with a letter
            if clean_name and not clean_name[0].isalpha():
                clean_name = f"col_{clean_name}"

            # Handle empty names
            if not clean_name:
                clean_name = f"unnamed_column"

            # Handle duplicates
            original_name = clean_name
            counter = 1
            while clean_name in seen_names:
                clean_name = f"{original_name}_{counter}"
                counter += 1

            seen_names.add(clean_name)
            cleaned_names.append(clean_name)

        return cleaned_names

    def _clean_numeric_column(self, series: pd.Series, col_name: str) -> pd.Series:
        """Clean numeric column data."""
        try:
            cleaned_series = series.copy()

            # Replace infinite values with NaN
            cleaned_series = cleaned_series.replace([np.inf, -np.inf], np.nan)

            # Remove extreme outliers (beyond 3 standard deviations)
            if cleaned_series.notna().sum() > 10:  # Only if we have enough data
                mean_val = cleaned_series.mean()
                std_val = cleaned_series.std()

                if pd.notna(mean_val) and pd.notna(std_val) and std_val > 0:
                    outlier_mask = np.abs((cleaned_series - mean_val) / std_val) > 3
                    outliers_count = outlier_mask.sum()

                    if (
                        outliers_count > 0
                        and outliers_count < len(cleaned_series) * 0.1
                    ):  # Max 10% outliers
                        cleaned_series.loc[outlier_mask] = np.nan
                        logger.debug(
                            f"Removed {outliers_count} outliers from '{col_name}'"
                        )

            return cleaned_series

        except Exception as e:
            logger.warning(f"Error cleaning numeric column '{col_name}': {e}")
            return series

    def _clean_text_column(self, series: pd.Series, col_name: str) -> pd.Series:
        """Clean text column data."""
        try:
            cleaned_series = series.copy()

            # Convert to string and strip whitespace
            cleaned_series = cleaned_series.astype(str).str.strip()

            # Replace common null-like strings with actual NaN
            null_like_values = [
                "null",
                "none",
                "n/a",
                "na",
                "nil",
                "undefined",
                "empty",
                "",
                "missing",
                "unknown",
                "nan",
            ]

            for null_val in null_like_values:
                cleaned_series = cleaned_series.replace(null_val.lower(), np.nan)
                cleaned_series = cleaned_series.replace(null_val.upper(), np.nan)
                cleaned_series = cleaned_series.replace(null_val.title(), np.nan)

            # Remove extremely long text (likely data corruption)
            max_length = 1000
            long_text_mask = cleaned_series.str.len() > max_length
            if long_text_mask.any():
                logger.debug(
                    f"Truncated {long_text_mask.sum()} long values in '{col_name}'"
                )
                cleaned_series.loc[long_text_mask] = cleaned_series.loc[
                    long_text_mask
                ].str[:max_length]

            # Replace 'nan' string with actual NaN
            cleaned_series = cleaned_series.replace("nan", np.nan)

            return cleaned_series

        except Exception as e:
            logger.warning(f"Error cleaning text column '{col_name}': {e}")
            return series

    def _is_date_column(self, series: pd.Series) -> bool:
        """Detect if a text column contains dates."""
        try:
            # Sample first 100 non-null values
            sample = series.dropna().head(100)
            if len(sample) < 5:
                return False

            # Common date patterns
            date_patterns = [
                r"\d{4}-\d{2}-\d{2}",  # YYYY-MM-DD
                r"\d{2}/\d{2}/\d{4}",  # MM/DD/YYYY
                r"\d{2}-\d{2}-\d{4}",  # MM-DD-YYYY
                r"\d{4}/\d{2}/\d{2}",  # YYYY/MM/DD
            ]

            # Check if majority match date patterns
            pattern_matches = 0
            for pattern in date_patterns:
                matches = sample.astype(str).str.match(pattern).sum()
                pattern_matches = max(pattern_matches, matches)

            # If >50% match date patterns, consider it a date column
            if pattern_matches / len(sample) > 0.5:
                return True

            # Try parsing a sample
            try:
                parsed = pd.to_datetime(sample.head(10), errors="coerce")
                valid_dates = parsed.notna().sum()
                return valid_dates >= 7  # At least 70% valid dates
            except:
                return False

        except Exception:
            return False

    def _validate_cleaned_data(self, df: pd.DataFrame) -> None:
        """Validate the cleaned data quality."""
        try:
            # Check for minimum data requirements
            if len(df) == 0:
                raise ValueError("No data remaining after cleaning")

            if len(df.columns) == 0:
                raise ValueError("No columns remaining after cleaning")

            # Check for excessive missing data
            missing_percentage = (
                df.isnull().sum().sum() / (len(df) * len(df.columns))
            ) * 100
            if missing_percentage > 90:
                logger.warning(
                    f"High percentage of missing data: {missing_percentage:.1f}%"
                )

            # Check for data types
            numeric_cols = len(df.select_dtypes(include=[np.number]).columns)
            text_cols = len(df.select_dtypes(include=[object]).columns)
            datetime_cols = len(df.select_dtypes(include=[np.datetime64]).columns)

            logger.info(
                f"Data validation: {numeric_cols} numeric, {text_cols} text, {datetime_cols} datetime columns"
            )

            # Ensure we have at least some analyzable data
            if numeric_cols == 0 and text_cols == 0:
                logger.warning("No analyzable columns found after cleaning")

        except Exception as e:
            logger.error(f"Data validation error: {e}")

    async def profile_data(self, file_path: str, dataset_id: str) -> DataProfile:
        """
        Profile the uploaded CSV data with comprehensive cleaning and analysis.

        Args:
            file_path: Path to the CSV file
            dataset_id: Unique identifier for the dataset

        Returns:
            DataProfile with comprehensive analysis
        """
        try:
            logger.info(f"Starting data profiling for {file_path}")

            # Read CSV with robust error handling
            try:
                df = pd.read_csv(file_path, encoding="utf-8")
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(file_path, encoding="latin-1")
                    logger.info("Used latin-1 encoding for CSV reading")
                except Exception as e:
                    df = pd.read_csv(file_path, encoding="utf-8", errors="ignore")
                    logger.warning(f"Used UTF-8 with error ignore: {e}")

            logger.info(f"Loaded CSV with shape: {df.shape}")

            # Comprehensive data cleaning
            df_cleaned = self._clean_data(df)

            # Basic dataset information
            total_rows, total_columns = df_cleaned.shape

            # Profile each column
            column_profiles = []
            for col in df_cleaned.columns:
                profile = await self._profile_column(df_cleaned[col], col)
                column_profiles.append(profile)

            # Categorize columns by type
            type_categorization = self._categorize_columns_by_type(column_profiles)

            # Calculate correlations for numeric columns
            correlations = self._calculate_correlations(
                df_cleaned, type_categorization["numeric"]
            )

            # Identify special columns
            potential_targets = self._identify_potential_targets(column_profiles)
            potential_ids = self._identify_potential_ids(column_profiles)

            # Calculate quality metrics
            overall_null_percentage = (
                df_cleaned.isnull().sum().sum() / (total_rows * total_columns) * 100
            )

            # Identify high/low cardinality columns
            high_cardinality = []
            low_cardinality = []

            for profile in column_profiles:
                cardinality_ratio = (
                    profile.unique_count / total_rows if total_rows > 0 else 0
                )

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
                numeric_columns=type_categorization["numeric"],
                categorical_columns=type_categorization["categorical"],
                datetime_columns=type_categorization["datetime"],
                boolean_columns=type_categorization["boolean"],
                text_columns=type_categorization["text"],
                has_datetime=len(type_categorization["datetime"]) > 0,
                has_numeric=len(type_categorization["numeric"]) > 0,
                has_categorical=len(type_categorization["categorical"]) > 0,
                potential_target_columns=potential_targets,
                potential_id_columns=potential_ids,
                overall_null_percentage=overall_null_percentage,
                high_cardinality_columns=high_cardinality,
                low_cardinality_columns=low_cardinality,
                correlations=correlations,
                profiled_at=datetime.utcnow(),
                sample_size=len(df_cleaned),
            )

            logger.info(f"Data profiling completed for dataset {dataset_id}")
            return data_profile

        except Exception as e:
            logger.error(f"Error during data profiling: {e}")
            raise

    async def _profile_column(
        self, series: pd.Series, column_name: str
    ) -> ColumnProfile:
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
                {
                    "value": str(val),
                    "count": int(count),
                    "percentage": count / len(non_null_series) * 100,
                }
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
            top_values=top_values,
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
            {"true", "false"},
            {"yes", "no"},
            {"y", "n"},
            {"1", "0"},
            {"t", "f"},
            {"on", "off"},
        ]

        return any(unique_values.issubset(pattern) for pattern in boolean_patterns)

    def _is_datetime_column(self, series: pd.Series) -> bool:
        """Check if column contains datetime data."""
        try:
            # Try to parse a sample as datetime
            sample = series.head(min(100, len(series)))
            pd.to_datetime(sample, errors="raise")
            return True
        except:
            return False

    def _is_numeric_column(self, series: pd.Series) -> bool:
        """Check if column contains numeric data."""
        try:
            pd.to_numeric(series, errors="raise")
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
        numeric_series = pd.to_numeric(series, errors="coerce").dropna()

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
            datetime_series = pd.to_datetime(series, errors="coerce").dropna()

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
            numeric_vals = pd.to_numeric(sample, errors="coerce").dropna()
            if len(numeric_vals) > 10:
                # Check if values are roughly sequential
                diff = numeric_vals.diff().dropna()
                if diff.median() == 1.0:
                    return True
        except:
            pass

        # Check for UUID-like patterns
        uuid_like = sample.str.match(
            r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        )
        if uuid_like.sum() > len(sample) * 0.5:
            return True

        return False

    def _check_email_pattern(self, series: pd.Series) -> bool:
        """Check if column contains email addresses."""
        sample = series.head(100)
        email_like = sample.str.contains(r"@.*\.", na=False, regex=True)
        return email_like.sum() > len(sample) * 0.5

    def _check_phone_pattern(self, series: pd.Series) -> bool:
        """Check if column contains phone numbers."""
        sample = series.head(100)
        phone_like = sample.str.match(r"^[\+]?[1-9]?[\d\s\-\(\)\.]{7,15}$", na=False)
        return phone_like.sum() > len(sample) * 0.5

    def _check_url_pattern(self, series: pd.Series) -> bool:
        """Check if column contains URLs."""
        sample = series.head(100)
        url_like = sample.str.contains(r"https?://", na=False, regex=True)
        return url_like.sum() > len(sample) * 0.5

    def _categorize_columns_by_type(
        self, column_profiles: List[ColumnProfile]
    ) -> Dict[str, List[str]]:
        """Categorize columns by their data types."""
        categorization = {
            "numeric": [],
            "categorical": [],
            "datetime": [],
            "boolean": [],
            "text": [],
        }

        for profile in column_profiles:
            if profile.data_type == ColumnType.NUMERIC:
                categorization["numeric"].append(profile.name)
            elif profile.data_type == ColumnType.CATEGORICAL:
                categorization["categorical"].append(profile.name)
            elif profile.data_type == ColumnType.DATETIME:
                categorization["datetime"].append(profile.name)
            elif profile.data_type == ColumnType.BOOLEAN:
                categorization["boolean"].append(profile.name)
            else:
                categorization["text"].append(profile.name)

        return categorization

    def _calculate_correlations(
        self, df: pd.DataFrame, numeric_columns: List[str]
    ) -> Dict[str, Dict[str, float]]:
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
                        if col2 in corr_matrix.columns and not pd.isna(
                            corr_matrix.loc[col1, col2]
                        ):
                            correlations[col1][col2] = float(
                                corr_matrix.loc[col1, col2]
                            )
        except Exception as e:
            logger.warning(f"Failed to calculate correlations: {e}")

        return correlations

    def _identify_potential_targets(
        self, column_profiles: List[ColumnProfile]
    ) -> List[str]:
        """Identify columns that might be target variables."""
        potential_targets = []

        for profile in column_profiles:
            # Look for columns with names suggesting they are targets
            target_keywords = [
                "target",
                "label",
                "class",
                "outcome",
                "result",
                "prediction",
            ]

            if any(
                keyword in profile.original_name.lower() for keyword in target_keywords
            ):
                potential_targets.append(profile.name)

            # Look for binary categorical variables
            elif (
                profile.data_type == ColumnType.CATEGORICAL
                and profile.unique_count == 2
                and profile.null_percentage < 10
            ):
                potential_targets.append(profile.name)

        return potential_targets

    def _identify_potential_ids(
        self, column_profiles: List[ColumnProfile]
    ) -> List[str]:
        """Identify columns that might be ID fields."""
        potential_ids = []

        for profile in column_profiles:
            # Check for ID-like patterns
            if profile.is_id_like:
                potential_ids.append(profile.name)

            # Check for high cardinality with ID-like names
            id_keywords = ["id", "key", "identifier", "uuid", "guid"]
            if (
                any(keyword in profile.original_name.lower() for keyword in id_keywords)
                and profile.unique_count > profile.cardinality * 0.9
            ):
                potential_ids.append(profile.name)

        return potential_ids

    def _sanitize_column_name(self, name: str) -> str:
        """Sanitize column name for safe processing."""
        # Convert to string and strip whitespace
        clean_name = str(name).strip()

        # Replace problematic characters
        clean_name = clean_name.replace(" ", "_").replace("-", "_")
        clean_name = "".join(c for c in clean_name if c.isalnum() or c == "_")

        # Ensure it doesn't start with a number
        if clean_name and clean_name[0].isdigit():
            clean_name = f"col_{clean_name}"

        # Provide default if empty
        if not clean_name:
            clean_name = "unnamed_column"

        return clean_name
