"""File validation utilities."""

import pandas as pd
import io
import csv
from typing import List, Optional
from fastapi import UploadFile
import chardet
import logging

from backend.config import get_settings
from backend.schemas.upload import FileValidationResponse
from backend.utils.exceptions import FileValidationException

logger = logging.getLogger(__name__)
settings = get_settings()


async def validate_csv_file(file: UploadFile) -> FileValidationResponse:
    """
    Validate an uploaded CSV file.
    
    Args:
        file: Uploaded file to validate
        
    Returns:
        FileValidationResponse with validation results
    """
    warnings = []
    
    try:
        # Reset file pointer
        await file.seek(0)
        content = await file.read()
        await file.seek(0)  # Reset for future reads
        
        # Check file size
        file_size = len(content)
        if file_size == 0:
            return FileValidationResponse(
                is_valid=False,
                file_size=file_size,
                error_message="File is empty"
            )
        
        if file_size > settings.max_file_size:
            return FileValidationResponse(
                is_valid=False,
                file_size=file_size,
                error_message=f"File size ({file_size} bytes) exceeds maximum allowed size ({settings.max_file_size} bytes)"
            )
        
        # Detect encoding
        encoding_result = chardet.detect(content[:10000])  # Check first 10KB
        detected_encoding = encoding_result.get('encoding', 'utf-8')
        confidence = encoding_result.get('confidence', 0)
        
        if confidence < 0.7:
            warnings.append(f"Low confidence in encoding detection ({confidence:.2f})")
        
        # Decode content
        try:
            text_content = content.decode(detected_encoding)
        except UnicodeDecodeError:
            # Fallback to utf-8 with error handling
            try:
                text_content = content.decode('utf-8', errors='replace')
                detected_encoding = 'utf-8'
                warnings.append("Used UTF-8 encoding with error replacement")
            except Exception:
                return FileValidationResponse(
                    is_valid=False,
                    file_size=file_size,
                    error_message="Unable to decode file content"
                )
        
        # Detect CSV delimiter
        try:
            # Sample first few lines for delimiter detection
            sample_lines = text_content.split('\n')[:10]
            sample_text = '\n'.join(sample_lines)
            
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample_text).delimiter
        except Exception:
            # Default to comma
            delimiter = ','
            warnings.append("Could not detect delimiter, using comma as default")
        
        # Read CSV to validate structure
        try:
            # Use StringIO to create a file-like object
            csv_buffer = io.StringIO(text_content)
            
            # Try to read with pandas
            df = pd.read_csv(
                csv_buffer,
                delimiter=delimiter,
                nrows=100,  # Only read first 100 rows for validation
                encoding=None  # Let pandas handle encoding
            )
            
            estimated_rows = _estimate_total_rows(text_content)
            estimated_columns = len(df.columns)
            
            # Validate minimum requirements
            if estimated_columns < 1:
                return FileValidationResponse(
                    is_valid=False,
                    file_size=file_size,
                    error_message="CSV file must have at least one column"
                )
            
            if estimated_rows < 2:  # Header + at least one data row
                return FileValidationResponse(
                    is_valid=False,
                    file_size=file_size,
                    error_message="CSV file must have at least one data row"
                )
            
            # Check for common issues
            if df.empty:
                warnings.append("No data rows found after header")
            
            # Check for columns with no name
            unnamed_cols = [col for col in df.columns if col.startswith('Unnamed:')]
            if unnamed_cols:
                warnings.append(f"Found {len(unnamed_cols)} unnamed columns")
            
            # Check for mostly empty columns
            mostly_empty_cols = []
            for col in df.columns:
                if df[col].isna().mean() > 0.9:  # More than 90% missing
                    mostly_empty_cols.append(col)
            
            if mostly_empty_cols:
                warnings.append(f"Found {len(mostly_empty_cols)} columns with >90% missing values")
            
            # Check for very wide datasets
            if estimated_columns > 100:
                warnings.append(f"Dataset has many columns ({estimated_columns}), this may affect performance")
            
            # Check for very long datasets
            if estimated_rows > 100000:
                warnings.append(f"Dataset has many rows ({estimated_rows}), sampling will be used for analysis")
            
            return FileValidationResponse(
                is_valid=True,
                file_size=file_size,
                estimated_rows=estimated_rows,
                estimated_columns=estimated_columns,
                detected_delimiter=delimiter,
                detected_encoding=detected_encoding,
                warnings=warnings
            )
            
        except pd.errors.EmptyDataError:
            return FileValidationResponse(
                is_valid=False,
                file_size=file_size,
                error_message="CSV file appears to be empty"
            )
        except pd.errors.ParserError as e:
            return FileValidationResponse(
                is_valid=False,
                file_size=file_size,
                error_message=f"CSV parsing error: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Unexpected error during CSV validation: {e}")
            return FileValidationResponse(
                is_valid=False,
                file_size=file_size,
                error_message=f"Unable to parse CSV file: {str(e)}"
            )
    
    except Exception as e:
        logger.error(f"Error validating file: {e}", exc_info=True)
        return FileValidationResponse(
            is_valid=False,
            file_size=0,
            error_message=f"File validation failed: {str(e)}"
        )


def _estimate_total_rows(content: str) -> int:
    """
    Estimate total number of rows in CSV content.
    
    Args:
        content: CSV content as string
        
    Returns:
        Estimated number of rows
    """
    try:
        # Count newlines, accounting for different line endings
        lines = content.replace('\r\n', '\n').replace('\r', '\n').split('\n')
        
        # Filter out empty lines
        non_empty_lines = [line for line in lines if line.strip()]
        
        # Subtract 1 for header row
        return max(0, len(non_empty_lines) - 1)
    
    except Exception:
        return 0


def validate_column_names(columns: List[str]) -> List[str]:
    """
    Validate and clean column names.
    
    Args:
        columns: List of column names
        
    Returns:
        List of validation warnings
    """
    warnings = []
    
    # Check for duplicate column names
    if len(columns) != len(set(columns)):
        warnings.append("Found duplicate column names")
    
    # Check for empty or whitespace-only column names
    empty_cols = [i for i, col in enumerate(columns) if not str(col).strip()]
    if empty_cols:
        warnings.append(f"Found {len(empty_cols)} empty column names")
    
    # Check for very long column names
    long_cols = [col for col in columns if len(str(col)) > 100]
    if long_cols:
        warnings.append(f"Found {len(long_cols)} very long column names (>100 characters)")
    
    return warnings


def sanitize_column_name(name: str) -> str:
    """
    Sanitize a column name for safe processing.
    
    Args:
        name: Original column name
        
    Returns:
        Sanitized column name
    """
    # Convert to string and strip whitespace
    clean_name = str(name).strip()
    
    # Replace problematic characters
    clean_name = clean_name.replace(' ', '_')
    clean_name = ''.join(c for c in clean_name if c.isalnum() or c in '_-')
    
    # Ensure it doesn't start with a number
    if clean_name and clean_name[0].isdigit():
        clean_name = f"col_{clean_name}"
    
    # Provide default if empty
    if not clean_name:
        clean_name = "unnamed_column"
    
    return clean_name