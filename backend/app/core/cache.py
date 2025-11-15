"""
Redis cache manager for caching responses and rate limiting
"""

import redis.asyncio as redis
from typing import Optional, Any
import json
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)


class CacheManager:
    """Redis cache manager"""

    def __init__(self):
        self.redis: Optional[redis.Redis] = None

    async def connect(self):
        """Connect to Redis"""
        try:
            self.redis = await redis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
            await self.redis.ping()
            logger.info("Connected to Redis")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            # Continue without cache if Redis is unavailable
            self.redis = None

    async def disconnect(self):
        """Disconnect from Redis"""
        if self.redis:
            await self.redis.close()
            logger.info("Disconnected from Redis")

    async def get(self, key: str) -> Optional[str]:
        """Get value from cache"""
        if not self.redis:
            return None
        try:
            value = await self.redis.get(key)
            return value
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None

    async def set(self, key: str, value: Any, ttl: int = None) -> bool:
        """Set value in cache with optional TTL"""
        if not self.redis:
            return False
        try:
            if ttl:
                await self.redis.setex(key, ttl, value)
            else:
                await self.redis.set(key, value)
            return True
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete key from cache"""
        if not self.redis:
            return False
        try:
            await self.redis.delete(key)
            return True
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            return False

    async def increment(self, key: str, amount: int = 1) -> int:
        """Increment counter"""
        if not self.redis:
            return 0
        try:
            return await self.redis.incrby(key, amount)
        except Exception as e:
            logger.error(f"Cache increment error: {e}")
            return 0

    async def expire(self, key: str, ttl: int) -> bool:
        """Set expiration on key"""
        if not self.redis:
            return False
        try:
            await self.redis.expire(key, ttl)
            return True
        except Exception as e:
            logger.error(f"Cache expire error: {e}")
            return False

    async def get_json(self, key: str) -> Optional[dict]:
        """Get JSON value from cache"""
        value = await self.get(key)
        if value:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return None
        return None

    async def set_json(self, key: str, value: dict, ttl: int = None) -> bool:
        """Set JSON value in cache"""
        try:
            json_value = json.dumps(value)
            return await self.set(key, json_value, ttl)
        except Exception as e:
            logger.error(f"Cache set_json error: {e}")
            return False


# Global instance
cache_manager = CacheManager()
