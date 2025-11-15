"""
Rate limiting middleware using Redis
"""

from fastapi import Request, HTTPException, status
from typing import Optional
import time
import logging

from app.core.cache import cache_manager
from app.core.config import settings

logger = logging.getLogger(__name__)


class RateLimiter:
    """Rate limiter using Redis"""

    def __init__(
        self,
        requests_per_minute: int = None,
        requests_per_hour: int = None
    ):
        self.requests_per_minute = requests_per_minute or settings.RATE_LIMIT_PER_MINUTE
        self.requests_per_hour = requests_per_hour or settings.RATE_LIMIT_PER_HOUR

    async def check_rate_limit(self, identifier: str) -> bool:
        """Check if request is within rate limits"""

        # Minute-based rate limit
        minute_key = f"ratelimit:minute:{identifier}:{int(time.time() // 60)}"
        minute_count = await cache_manager.increment(minute_key)
        if minute_count == 1:
            await cache_manager.expire(minute_key, 60)

        if minute_count > self.requests_per_minute:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded: {self.requests_per_minute} requests per minute"
            )

        # Hour-based rate limit
        hour_key = f"ratelimit:hour:{identifier}:{int(time.time() // 3600)}"
        hour_count = await cache_manager.increment(hour_key)
        if hour_count == 1:
            await cache_manager.expire(hour_key, 3600)

        if hour_count > self.requests_per_hour:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Rate limit exceeded: {self.requests_per_hour} requests per hour"
            )

        return True

    async def get_identifier(self, request: Request) -> str:
        """Get unique identifier for rate limiting"""
        # Try to get user ID from auth token
        auth_header = request.headers.get("Authorization")
        if auth_header:
            try:
                from app.core.security import verify_token
                token = auth_header.replace("Bearer ", "")
                token_data = verify_token(token)
                return f"user:{token_data.user_id}"
            except Exception:
                pass

        # Fallback to IP address
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return f"ip:{forwarded.split(',')[0]}"
        return f"ip:{request.client.host}"


# Global rate limiter instance
rate_limiter = RateLimiter()


async def check_rate_limit(request: Request):
    """Dependency for rate limiting"""
    identifier = await rate_limiter.get_identifier(request)
    await rate_limiter.check_rate_limit(identifier)
