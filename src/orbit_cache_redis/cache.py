# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Redis-backed implementation of the provider-neutral Orbit cache contract."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol, cast

import redis.asyncio as redis_async
from orbit_cache import AsyncCache, CacheConfigurationError, CacheOperationError

from orbit_cache_redis.config import RedisCacheConfig


class _RedisClient(Protocol):
    """Narrow async redis-py surface used by the adapter and its contract tests."""

    async def get(self, name: str) -> bytes | None:
        """Get one key's raw bytes."""

    async def set(self, name: str, value: bytes, *, ex: int | None = None) -> bool | None:
        """Set raw bytes with optional expiration in seconds."""

    async def delete(self, *names: str) -> int:
        """Delete keys and return the number removed."""

    async def aclose(self) -> None:
        """Close the client and its owned connection pool."""


class RedisCache:
    """Standalone Redis adapter for :class:`orbit_cache.AsyncCache`.

    A client passed to the constructor is owned by this adapter and closed by ``aclose``. The
    preferred ``from_url`` factory creates a redis-py client with decoded text responses disabled,
    so values preserve their byte representation.
    """

    def __init__(self, client: _RedisClient) -> None:
        """Wrap an async Redis client; the adapter takes responsibility for closing it."""
        required_methods = ("get", "set", "delete", "aclose")
        if not all(callable(getattr(client, name, None)) for name in required_methods):
            raise TypeError("client must provide async get, set, delete, and aclose methods.")
        self._client = client
        self._closed = False
        self._closing = False
        self._active = 0
        self._state_lock = asyncio.Lock()
        self._idle = asyncio.Event()
        self._idle.set()
        self._close_complete = asyncio.Event()

    @classmethod
    def from_url(cls, url: str, *, max_connections: int = 100) -> RedisCache:
        """Create a lazily connected cache client from a Redis URL.

        Use ``rediss://`` for TLS. The URL is not retained in this object or exposed in errors.
        Connection attempts occur when an operation is first awaited.
        """
        return cls.from_config(RedisCacheConfig(url=url, max_connections=max_connections))

    @classmethod
    def from_config(cls, config: RedisCacheConfig) -> RedisCache:
        """Create a Redis cache using the supplied validated settings object."""
        if not isinstance(config, RedisCacheConfig):
            raise TypeError("config must be a RedisCacheConfig instance.")
        try:
            client = redis_async.Redis.from_url(
                config.url,
                decode_responses=False,
                max_connections=config.max_connections,
                socket_timeout=config.socket_timeout,
                socket_connect_timeout=config.socket_connect_timeout,
            )
        except Exception:
            raise CacheConfigurationError("Redis URL or client configuration is invalid.") from None
        return cls(cast(_RedisClient, client))

    async def get(self, key: str) -> bytes | None:
        """Read one key, returning ``None`` for a cache miss."""
        self._validate_key(key)
        try:
            async with self._operation():
                result = await self._client.get(key)
        except asyncio.CancelledError:
            raise
        except CacheOperationError:
            raise
        except Exception:
            raise CacheOperationError("Redis cache read failed.") from None
        if result is not None and not isinstance(result, bytes):
            raise CacheOperationError("Redis returned a value that is not bytes.")
        return result

    async def set(self, key: str, value: bytes, *, ttl: int | None = None) -> None:
        """Store bytes with an optional expiration in whole seconds."""
        self._validate_key(key)
        if not isinstance(value, bytes):
            raise CacheConfigurationError("cache value must be bytes.")
        if ttl is not None and (
            not isinstance(ttl, int) or isinstance(ttl, bool) or ttl < 1 or ttl > 2_147_483_647
        ):
            raise CacheConfigurationError("ttl must be a positive integer no greater than 2^31-1.")
        try:
            async with self._operation():
                await self._client.set(key, value, ex=ttl)
        except asyncio.CancelledError:
            raise
        except CacheOperationError:
            raise
        except Exception:
            raise CacheOperationError("Redis cache write failed.") from None

    async def delete(self, key: str) -> bool:
        """Remove a key and report whether Redis removed it."""
        self._validate_key(key)
        try:
            async with self._operation():
                removed = await self._client.delete(key)
        except asyncio.CancelledError:
            raise
        except CacheOperationError:
            raise
        except Exception:
            raise CacheOperationError("Redis cache delete failed.") from None
        return removed > 0

    async def aclose(self) -> None:
        """Reject new work, drain active commands, then close the owned client once."""
        async with self._state_lock:
            if self._closed:
                return
            if self._closing:
                owner = False
            else:
                owner = True
                self._closing = True
                self._close_complete.clear()
                if self._active == 0:
                    self._idle.set()
        if not owner:
            await self._close_complete.wait()
            if not self._closed:
                await self.aclose()
            return
        try:
            await self._idle.wait()
            await self._client.aclose()
        except asyncio.CancelledError:
            async with self._state_lock:
                self._closing = False
                self._close_complete.set()
            raise
        except Exception:
            async with self._state_lock:
                self._closing = False
                self._close_complete.set()
            raise CacheOperationError("Redis cache shutdown failed.") from None
        async with self._state_lock:
            self._closed = True
            self._closing = False
            self._close_complete.set()

    @staticmethod
    def _validate_key(key: str) -> None:
        """Reject empty or non-text keys before sending commands to Redis."""
        if not isinstance(key, str) or not key:
            raise CacheConfigurationError("cache key must be a non-empty string.")

    @asynccontextmanager
    async def _operation(self) -> AsyncIterator[None]:
        """Keep shutdown from closing the pool while a Redis command is in flight."""
        async with self._state_lock:
            if self._closing or self._closed:
                raise CacheOperationError("Redis cache is closed.")
            self._active += 1
            self._idle.clear()
        try:
            yield
        finally:
            async with self._state_lock:
                self._active -= 1
                if self._active == 0:
                    self._idle.set()


def _implements_cache_contract(cache: RedisCache) -> AsyncCache:
    """Keep the implementation statically checked against the separate capability contract."""
    return cache


__all__ = ["RedisCache"]
