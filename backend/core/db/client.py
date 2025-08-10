"""Prisma database client for autocurate application."""

import asyncio
from typing import Optional, Dict, Any
from datetime import datetime
import logging
from contextlib import asynccontextmanager

from prisma import Prisma
from prisma.models import Dataset, Analytics
from prisma.models import enums

logger = logging.getLogger(__name__)


class DatabaseClient:
    """Database client for Prisma operations."""
    
    def __init__(self):
        self.prisma: Optional[Prisma] = None
        self._is_connected = False
    
    async def connect(self):
        """Connect to the database."""
        try:
            self.prisma = Prisma()
            self.prisma.connect()  # Synchronous call
            self._is_connected = True
            logger.info("Database connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            self._is_connected = False
            # Don't raise the exception, just log it
    
    async def disconnect(self):
        """Disconnect from the database."""
        if self.prisma and self._is_connected:
            self.prisma.disconnect()  # Synchronous call
            self._is_connected = False
            logger.info("Database disconnected")
    
    def get_client(self):
        """Get database client."""
        if not self._is_connected or not self.prisma:
            # Note: This is called from async context, but connect is sync
            import asyncio
            loop = asyncio.get_event_loop()
            loop.run_in_executor(None, self._sync_connect)
        return self.prisma if self._is_connected else None
    
    def _sync_connect(self):
        """Synchronous connect method."""
        try:
            self.prisma = Prisma()
            self.prisma.connect()
            self._is_connected = True
            logger.info("Database connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            self._is_connected = False
    
    # Dataset operations
    async def create_dataset(
        self,
        filename: str,
        file_size: int,
        row_count: Optional[int] = None,
        column_count: Optional[int] = None
    ) -> Optional[Dataset]:
        """Create a new dataset record."""
        client = self.get_client()
        if not client:
            logger.warning("Database client not available")
            return None
        
        import asyncio
        loop = asyncio.get_event_loop()
        
        def _create():
            return client.dataset.create({
                'data': {
                    'filename': filename,
                    'fileSize': file_size,
                    'rowCount': row_count,
                    'columnCount': column_count,
                    'status': enums.ProcessingStatus.PENDING
                }
            })
        
        return await loop.run_in_executor(None, _create)
    
    async def get_dataset(self, dataset_id: str) -> Optional[Dataset]:
        """Get a dataset by ID."""
        client = self.get_client()
        if not client:
            logger.warning("Database client not available")
            return None
        
        import asyncio
        loop = asyncio.get_event_loop()
        
        def _get():
            return client.dataset.find_unique({
                'where': {'id': dataset_id}
            })
        
        return await loop.run_in_executor(None, _get)
    
    async def update_dataset_status(
        self,
        dataset_id: str,
        status: enums.ProcessingStatus,
        row_count: Optional[int] = None,
        column_count: Optional[int] = None
    ) -> Optional[Dataset]:
        """Update dataset status and metadata."""
        client = self.get_client()
        if not client:
            logger.warning("Database client not available")
            return None
        
        import asyncio
        loop = asyncio.get_event_loop()
        
        def _update():
            update_data = {'status': status}
            if row_count is not None:
                update_data['rowCount'] = row_count
            if column_count is not None:
                update_data['columnCount'] = column_count
            
            return client.dataset.update({
                'where': {'id': dataset_id},
                'data': update_data
            })
        
        return await loop.run_in_executor(None, _update)
    
    # Analytics operations
    async def create_analytics(
        self,
        dataset_id: str,
        profile: Dict[str, Any],
        domain_info: Dict[str, Any],
        dashboard_config: Dict[str, Any]
    ) -> Optional[Analytics]:
        """Create analytics results for a dataset."""
        client = self.get_client()
        if not client:
            logger.warning("Database client not available")
            return None
        
        import asyncio
        loop = asyncio.get_event_loop()
        
        def _create():
            return client.analytics.create({
                'data': {
                    'datasetId': dataset_id,
                    'profile': profile,
                    'domainInfo': domain_info,
                    'dashboardConfig': dashboard_config
                }
            })
        
        return await loop.run_in_executor(None, _create)
    
    async def get_analytics(self, dataset_id: str) -> Optional[Analytics]:
        """Get analytics results for a dataset."""
        client = self.get_client()
        if not client:
            logger.warning("Database client not available")
            return None
        
        import asyncio
        loop = asyncio.get_event_loop()
        
        def _get():
            return client.analytics.find_unique({
                'where': {'datasetId': dataset_id}
            })
        
        return await loop.run_in_executor(None, _get)
    
    async def update_analytics(
        self,
        dataset_id: str,
        profile: Optional[Dict[str, Any]] = None,
        domain_info: Optional[Dict[str, Any]] = None,
        dashboard_config: Optional[Dict[str, Any]] = None
    ) -> Optional[Analytics]:
        """Update analytics results."""
        client = self.get_client()
        if not client:
            logger.warning("Database client not available")
            return None
        
        import asyncio
        loop = asyncio.get_event_loop()
        
        def _update():
            update_data = {}
            if profile is not None:
                update_data['profile'] = profile
            if domain_info is not None:
                update_data['domainInfo'] = domain_info
            if dashboard_config is not None:
                update_data['dashboardConfig'] = dashboard_config
            
            return client.analytics.update({
                'where': {'datasetId': dataset_id},
                'data': update_data
            })
        
        return await loop.run_in_executor(None, _update)


# Global database client instance
db_client = DatabaseClient() 