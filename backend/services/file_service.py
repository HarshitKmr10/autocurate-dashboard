"""File handling service."""

import os
import aiofiles
import shutil
from pathlib import Path
from typing import Optional
from fastapi import UploadFile
import logging

from backend.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class FileService:
    """Service for handling file operations."""
    
    def __init__(self):
        self.upload_dir = Path(settings.upload_dir)
        self.processed_dir = Path("./data/processed")
        self.cache_dir = Path("./data/cache")
        
        # Ensure directories exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure all required directories exist."""
        for directory in [self.upload_dir, self.processed_dir, self.cache_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured directory exists: {directory}")
    
    async def save_uploaded_file(self, file: UploadFile, dataset_id: str) -> str:
        """
        Save an uploaded file to the uploads directory.
        
        Args:
            file: The uploaded file
            dataset_id: Unique identifier for the dataset
            
        Returns:
            Path to the saved file
        """
        # Create dataset-specific directory
        dataset_dir = self.upload_dir / dataset_id
        dataset_dir.mkdir(exist_ok=True)
        
        # Generate safe filename
        filename = self._sanitize_filename(file.filename or "data.csv")
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        file_path = dataset_dir / filename
        
        try:
            # Save file asynchronously
            async with aiofiles.open(file_path, 'wb') as f:
                # Reset file pointer to beginning
                await file.seek(0)
                content = await file.read()
                await f.write(content)
            
            logger.info(f"File saved successfully: {file_path}")
            return str(file_path)
            
        except Exception as e:
            logger.error(f"Failed to save file: {e}")
            # Clean up on failure
            if file_path.exists():
                file_path.unlink()
            raise
    
    def _sanitize_filename(self, filename: str) -> str:
        """
        Sanitize filename to remove potentially dangerous characters.
        
        Args:
            filename: Original filename
            
        Returns:
            Sanitized filename
        """
        # Remove path components and dangerous characters
        filename = os.path.basename(filename)
        filename = "".join(c for c in filename if c.isalnum() or c in '._-')
        
        # Ensure it's not empty
        if not filename:
            filename = "data.csv"
        
        return filename
    
    async def get_file_path(self, dataset_id: str) -> Optional[str]:
        """
        Get the file path for a dataset.
        
        Args:
            dataset_id: Unique identifier for the dataset
            
        Returns:
            Path to the file or None if not found
        """
        dataset_dir = self.upload_dir / dataset_id
        
        if not dataset_dir.exists():
            return None
        
        # Find CSV file in the directory
        csv_files = list(dataset_dir.glob("*.csv"))
        if csv_files:
            return str(csv_files[0])
        
        return None
    
    async def delete_dataset(self, dataset_id: str) -> bool:
        """
        Delete all files associated with a dataset.
        
        Args:
            dataset_id: Unique identifier for the dataset
            
        Returns:
            True if deleted successfully, False if not found
        """
        dataset_dir = self.upload_dir / dataset_id
        
        if not dataset_dir.exists():
            return False
        
        try:
            # Remove entire dataset directory
            shutil.rmtree(dataset_dir)
            
            # Also clean up processed and cache directories
            processed_dir = self.processed_dir / dataset_id
            if processed_dir.exists():
                shutil.rmtree(processed_dir)
            
            cache_dir = self.cache_dir / dataset_id
            if cache_dir.exists():
                shutil.rmtree(cache_dir)
            
            logger.info(f"Dataset {dataset_id} deleted successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete dataset {dataset_id}: {e}")
            return False
    
    async def get_file_info(self, dataset_id: str) -> Optional[dict]:
        """
        Get information about a dataset file.
        
        Args:
            dataset_id: Unique identifier for the dataset
            
        Returns:
            File information dictionary or None if not found
        """
        file_path = await self.get_file_path(dataset_id)
        
        if not file_path:
            return None
        
        file_path_obj = Path(file_path)
        
        try:
            stat = file_path_obj.stat()
            return {
                "dataset_id": dataset_id,
                "filename": file_path_obj.name,
                "file_size": stat.st_size,
                "created_at": stat.st_ctime,
                "modified_at": stat.st_mtime,
                "file_path": file_path
            }
        except Exception as e:
            logger.error(f"Failed to get file info for {dataset_id}: {e}")
            return None
    
    def get_processed_file_path(self, dataset_id: str, filename: str) -> str:
        """
        Get path for a processed file.
        
        Args:
            dataset_id: Unique identifier for the dataset
            filename: Name of the processed file
            
        Returns:
            Path to the processed file
        """
        dataset_dir = self.processed_dir / dataset_id
        dataset_dir.mkdir(parents=True, exist_ok=True)
        return str(dataset_dir / filename)
    
    def get_cache_file_path(self, dataset_id: str, filename: str) -> str:
        """
        Get path for a cache file.
        
        Args:
            dataset_id: Unique identifier for the dataset
            filename: Name of the cache file
            
        Returns:
            Path to the cache file
        """
        dataset_dir = self.cache_dir / dataset_id
        dataset_dir.mkdir(parents=True, exist_ok=True)
        return str(dataset_dir / filename)


# Global file service instance
file_service = FileService()