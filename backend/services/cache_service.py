"""Redis caching service."""

import redis.asyncio as redis
import json
import pickle
from typing import Any, Optional, Union
import logging
from datetime import datetime, timedelta
from pathlib import Path
import aiofiles
import hashlib

from backend.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class CacheService:
    """Redis-based caching service with file fallback."""

    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self._is_connected = False
        # File-based fallback storage
        self.fallback_dir = Path("./data/cache")
        self.fallback_dir.mkdir(parents=True, exist_ok=True)

    async def initialize(self):
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=False,  # We'll handle encoding manually
            )

            # Test connection
            await self.redis_client.ping()
            self._is_connected = True
            logger.info("Redis cache service initialized successfully")

        except Exception as e:
            logger.warning(
                f"Failed to connect to Redis: {e}. Using file-based fallback."
            )
            self.redis_client = None
            self._is_connected = False

    async def close(self):
        """Close Redis connection."""
        if self.redis_client:
            await self.redis_client.close()
            self._is_connected = False
            logger.info("Redis connection closed")

    def _get_fallback_path(self, key: str) -> Path:
        """Get the fallback file path for a cache key."""
        # Create a safe filename from the key
        safe_key = hashlib.md5(key.encode()).hexdigest()
        return self.fallback_dir / f"{safe_key}.cache"

    async def _set_fallback(
        self, key: str, value: Any, ttl: Optional[int] = None
    ) -> bool:
        """Store value in file-based fallback."""
        try:
            file_path = self._get_fallback_path(key)

            # Prepare data with metadata
            cache_data = {
                "key": key,
                "value": value,
                "created_at": datetime.utcnow().isoformat(),
                "ttl": ttl,
                "expires_at": (
                    (datetime.utcnow() + timedelta(seconds=ttl)).isoformat()
                    if ttl
                    else None
                ),
            }

            # Write to file
            async with aiofiles.open(file_path, "w", encoding="utf-8") as f:
                await f.write(json.dumps(cache_data, default=str))

            logger.debug(f"Stored fallback cache for key: {key}")
            return True

        except Exception as e:
            logger.error(f"Failed to store fallback cache for key {key}: {e}")
            return False

    async def _get_fallback(self, key: str) -> Optional[Any]:
        """Retrieve value from file-based fallback."""
        try:
            file_path = self._get_fallback_path(key)

            if not file_path.exists():
                return None

            # Read from file
            async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
                content = await f.read()
                cache_data = json.loads(content)

            # Check if expired
            if cache_data.get("expires_at"):
                expires_at = datetime.fromisoformat(cache_data["expires_at"])
                if datetime.utcnow() > expires_at:
                    # Clean up expired file
                    try:
                        file_path.unlink()
                    except:
                        pass
                    return None

            logger.debug(f"Retrieved fallback cache for key: {key}")
            return cache_data["value"]

        except Exception as e:
            logger.error(f"Failed to retrieve fallback cache for key {key}: {e}")
            return None

    async def _delete_fallback(self, key: str) -> bool:
        """Delete value from file-based fallback."""
        try:
            file_path = self._get_fallback_path(key)
            if file_path.exists():
                file_path.unlink()
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to delete fallback cache for key {key}: {e}")
            return False

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a value in the cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds

        Returns:
            True if successful, False otherwise
        """
        redis_success = False
        fallback_success = False

        # Try Redis first
        try:
            if self._is_connected and self.redis_client:
                # Serialize value
                serialized_value = self._serialize_value(value)

                # Set value with optional TTL
                if ttl:
                    await self.redis_client.setex(key, ttl, serialized_value)
                else:
                    await self.redis_client.set(key, serialized_value)

                logger.debug(f"Cached value in Redis for key: {key}")
                redis_success = True
        except Exception as e:
            logger.error(f"Failed to set Redis cache value for key {key}: {e}")

        # Always try fallback storage
        try:
            fallback_success = await self._set_fallback(key, value, ttl)
        except Exception as e:
            logger.error(f"Failed to set fallback cache value for key {key}: {e}")

        if not redis_success and not fallback_success:
            logger.warning(
                f"Failed to cache value for key {key} in both Redis and fallback"
            )
            return False

        return True

    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found
        """
        # Try Redis first
        if self._is_connected and self.redis_client:
            try:
                # Get value from Redis
                serialized_value = await self.redis_client.get(key)

                if serialized_value is not None:
                    # Deserialize value
                    value = self._deserialize_value(serialized_value)
                    logger.debug(f"Cache hit in Redis for key: {key}")
                    return value
            except Exception as e:
                logger.error(f"Failed to get Redis cache value for key {key}: {e}")

        # Try fallback storage
        try:
            value = await self._get_fallback(key)
            if value is not None:
                logger.debug(f"Cache hit in fallback for key: {key}")
                return value
        except Exception as e:
            logger.error(f"Failed to get fallback cache value for key {key}: {e}")

        logger.debug(f"Cache miss for key: {key}")
        return None

    async def delete(self, key: str) -> bool:
        """
        Delete a value from the cache.

        Args:
            key: Cache key

        Returns:
            True if successful, False otherwise
        """
        redis_success = False
        fallback_success = False

        # Try Redis
        if self._is_connected and self.redis_client:
            try:
                result = await self.redis_client.delete(key)
                redis_success = result > 0
                logger.debug(f"Deleted Redis cache key: {key}")
            except Exception as e:
                logger.error(f"Failed to delete Redis cache key {key}: {e}")

        # Try fallback
        try:
            fallback_success = await self._delete_fallback(key)
        except Exception as e:
            logger.error(f"Failed to delete fallback cache key {key}: {e}")

        return redis_success or fallback_success

    async def exists(self, key: str) -> bool:
        """
        Check if a key exists in the cache.

        Args:
            key: Cache key

        Returns:
            True if key exists, False otherwise
        """
        # Try Redis first
        if self._is_connected and self.redis_client:
            try:
                result = await self.redis_client.exists(key)
                if result > 0:
                    return True
            except Exception as e:
                logger.error(f"Failed to check Redis cache key existence {key}: {e}")

        # Try fallback
        try:
            file_path = self._get_fallback_path(key)
            if file_path.exists():
                # Check if not expired
                value = await self._get_fallback(key)
                return value is not None
        except Exception as e:
            logger.error(f"Failed to check fallback cache key existence {key}: {e}")

        return False

    async def set_hash(
        self, key: str, field: str, value: Any, ttl: Optional[int] = None
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
                return json.dumps(value, default=str).encode("utf-8")
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
                return json.loads(data.decode("utf-8"))
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
