"""Redis caching service."""

import redis.asyncio as redis
import json
import pickle
from typing import Any, Optional, Union
import logging
from datetime import datetime, timedelta

from backend.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class CacheService:
    """Redis-based caching service."""
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self._is_connected = False
    
    async def initialize(self):
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=False  # We'll handle encoding manually
            )
            
            # Test connection
            await self.redis_client.ping()
            self._is_connected = True
            logger.info("Redis cache service initialized successfully")
            
        except Exception as e:
            logger.warning(f"Failed to connect to Redis: {e}. Falling back to memory cache.")
            self.redis_client = None
            self._is_connected = False
    
    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            self._is_connected = False
            logger.info("Redis connection closed")
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set a value in the cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self._is_connected or not self.redis_client:
                logger.warning("Redis not available, skipping cache set")
                return False
            
            # Serialize value
            serialized_value = self._serialize_value(value)
            
            # Set value with optional TTL
            if ttl:
                await self.redis_client.setex(key, ttl, serialized_value)
            else:
                await self.redis_client.set(key, serialized_value)
            
            logger.debug(f"Cached value for key: {key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to set cache value for key {key}: {e}")
            return False
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        try:
            if not self._is_connected or not self.redis_client:
                logger.debug("Redis not available, cache miss")
                return None
            
            # Get value from Redis
            serialized_value = await self.redis_client.get(key)
            
            if serialized_value is None:
                logger.debug(f"Cache miss for key: {key}")
                return None
            
            # Deserialize value
            value = self._deserialize_value(serialized_value)
            logger.debug(f"Cache hit for key: {key}")
            return value
            
        except Exception as e:
            logger.error(f"Failed to get cache value for key {key}: {e}")
            return None
    
    async def delete(self, key: str) -> bool:
        """
        Delete a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self._is_connected or not self.redis_client:
                return False
            
            result = await self.redis_client.delete(key)
            logger.debug(f"Deleted cache key: {key}")
            return result > 0
            
        except Exception as e:
            logger.error(f"Failed to delete cache key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in the cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if key exists, False otherwise
        """
        try:
            if not self._is_connected or not self.redis_client:
                return False
            
            result = await self.redis_client.exists(key)
            return result > 0
            
        except Exception as e:
            logger.error(f"Failed to check cache key existence {key}: {e}")
            return False
    
    async def set_hash(
        self,
        key: str,
        field: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Set a field in a hash.
        
        Args:
            key: Hash key
            field: Field name
            value: Value to set
            ttl: Time to live in seconds for the entire hash
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self._is_connected or not self.redis_client:
                return False
            
            # Serialize value
            serialized_value = self._serialize_value(value)
            
            # Set hash field
            await self.redis_client.hset(key, field, serialized_value)
            
            # Set TTL if specified
            if ttl:
                await self.redis_client.expire(key, ttl)
            
            logger.debug(f"Set hash field {field} for key: {key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to set hash field {field} for key {key}: {e}")
            return False
    
    async def get_hash(self, key: str, field: str) -> Optional[Any]:
        """
        Get a field from a hash.
        
        Args:
            key: Hash key
            field: Field name
            
        Returns:
            Field value or None if not found
        """
        try:
            if not self._is_connected or not self.redis_client:
                return None
            
            # Get hash field
            serialized_value = await self.redis_client.hget(key, field)
            
            if serialized_value is None:
                return None
            
            # Deserialize value
            value = self._deserialize_value(serialized_value)
            logger.debug(f"Got hash field {field} for key: {key}")
            return value
            
        except Exception as e:
            logger.error(f"Failed to get hash field {field} for key {key}: {e}")
            return None
    
    async def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """
        Increment a numeric value.
        
        Args:
            key: Cache key
            amount: Amount to increment
            
        Returns:
            New value or None if failed
        """
        try:
            if not self._is_connected or not self.redis_client:
                return None
            
            result = await self.redis_client.incrby(key, amount)
            logger.debug(f"Incremented key {key} by {amount}, new value: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to increment key {key}: {e}")
            return None
    
    def _serialize_value(self, value: Any) -> bytes:
        """
        Serialize a value for storage.
        
        Args:
            value: Value to serialize
            
        Returns:
            Serialized bytes
        """
        try:
            # Try JSON first for simple types
            if isinstance(value, (dict, list, str, int, float, bool, type(None))):
                return json.dumps(value, default=str).encode('utf-8')
            else:
                # Use pickle for complex objects
                return pickle.dumps(value)
        except Exception:
            # Fallback to pickle
            return pickle.dumps(value)
    
    def _deserialize_value(self, data: bytes) -> Any:
        """
        Deserialize a value from storage.
        
        Args:
            data: Serialized data
            
        Returns:
            Deserialized value
        """
        try:
            # Try JSON first
            try:
                return json.loads(data.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                # Fallback to pickle
                return pickle.loads(data)
        except Exception as e:
            logger.error(f"Failed to deserialize value: {e}")
            raise
    
    async def clear_pattern(self, pattern: str) -> int:
        """
        Delete all keys matching a pattern.
        
        Args:
            pattern: Key pattern (e.g., "analytics:*")
            
        Returns:
            Number of keys deleted
        """
        try:
            if not self._is_connected or not self.redis_client:
                return 0
            
            # Get keys matching pattern
            keys = await self.redis_client.keys(pattern)
            
            if not keys:
                return 0
            
            # Delete keys
            result = await self.redis_client.delete(*keys)
            logger.info(f"Deleted {result} keys matching pattern: {pattern}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to clear pattern {pattern}: {e}")
            return 0


# Global cache service instance
cache_service = CacheService()