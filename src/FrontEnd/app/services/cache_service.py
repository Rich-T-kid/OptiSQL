import os
import json
import hashlib
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

try:
    import redis
    _REDIS_AVAILABLE = True
except ImportError:
    logger.warning("Redis not available. Install with 'pip install redis'")
    _REDIS_AVAILABLE = False


class CacheService:
    """Redis cache for Substrait plans."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        ttl: int = 3600
    ):
        self.host = host or os.getenv("REDIS_HOST", "localhost")
        self.port = port or int(os.getenv("REDIS_PORT", "6379"))
        self.ttl = ttl
        self._client: Optional["redis.Redis"] = None

    @property
    def is_available(self) -> bool:
        if not _REDIS_AVAILABLE:
            return False
        try:
            client = self._get_client()
            client.ping()
            return True
        except Exception:
            return False

    def _get_client(self) -> "redis.Redis":
        if not _REDIS_AVAILABLE:
            raise RuntimeError("Redis not available. Install with 'pip install redis'")

        if self._client is None:
            self._client = redis.Redis(
                host=self.host,
                port=self.port,
                decode_responses=True,
                socket_connect_timeout=5
            )
            logger.info(f"Connected to Redis at {self.host}:{self.port}")

        return self._client

    @staticmethod
    def _make_cache_key(sql_query: str, s3_source: Optional[str] = None) -> str:
        normalized = sql_query.strip().lower()
        key_data = f"{normalized}|{s3_source or ''}"
        return f"substrait:{hashlib.sha256(key_data.encode()).hexdigest()}"

    def get_plan(self, sql_query: str, s3_source: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not _REDIS_AVAILABLE:
            return None

        try:
            client = self._get_client()
            cache_key = self._make_cache_key(sql_query, s3_source)
            cached = client.get(cache_key)

            if cached:
                logger.info(f"Cache hit for query: {sql_query[:50]}...")
                return json.loads(cached)

            logger.debug(f"Cache miss for query: {sql_query[:50]}...")
            return None

        except Exception as e:
            logger.warning(f"Cache get failed: {e}")
            return None

    def set_plan(
        self,
        sql_query: str,
        plan: Dict[str, Any],
        s3_source: Optional[str] = None
    ) -> bool:
        if not _REDIS_AVAILABLE:
            return False

        try:
            client = self._get_client()
            cache_key = self._make_cache_key(sql_query, s3_source)
            client.setex(cache_key, self.ttl, json.dumps(plan))
            logger.info(f"Cached plan for query: {sql_query[:50]}...")
            return True

        except Exception as e:
            logger.warning(f"Cache set failed: {e}")
            return False

    def invalidate(self, sql_query: str, s3_source: Optional[str] = None) -> bool:
        if not _REDIS_AVAILABLE:
            return False

        try:
            client = self._get_client()
            cache_key = self._make_cache_key(sql_query, s3_source)
            client.delete(cache_key)
            return True

        except Exception as e:
            logger.warning(f"Cache invalidate failed: {e}")
            return False

    def close(self):
        if self._client:
            self._client.close()
            self._client = None


_cache_service: Optional[CacheService] = None


def get_cache_service() -> CacheService:
    global _cache_service
    if _cache_service is None:
        _cache_service = CacheService()
    return _cache_service
