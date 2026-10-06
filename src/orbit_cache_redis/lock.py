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
"""Optional Redis adapter for Orbit's distributed lease capability."""

from __future__ import annotations

import asyncio
import hashlib
import re
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol, cast

import redis.asyncio as redis_async
from orbit_lock import (
    LockClosedError,
    LockLease,
    LockOperationError,
    validate_resource,
    validate_ttl,
)

from orbit_cache_redis.config import RedisCacheConfig

_PREFIX = re.compile(r"[A-Za-z0-9][A-Za-z0-9:_-]{0,63}\Z")
_MAX_SAFE_FENCE = 9_007_199_254_740_991
_ACQUIRE_SCRIPT = f"""
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local current = tonumber(redis.call('GET', KEYS[2]) or '0')
if current >= {_MAX_SAFE_FENCE - 1} then
  return redis.error_reply('fencing token exhausted')
end
local token = redis.call('INCR', KEYS[2])
local ok = redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX')
if not ok then return 0 end
return token
"""
_RENEW_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])
"""
_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"""


class _RedisLockClient(Protocol):
    """Narrow async Redis client surface used by the adapter."""

    async def eval(self, script: str, numkeys: int, *keys_and_args: str | bytes) -> object:
        """Run one atomic script against the configured Redis server."""

    async def aclose(self) -> None:
        """Close the owned Redis client and connection pool."""


class RedisLockProvider:
    """Redis-backed leases with compare-and-renew/release and bounded provider lifecycle.

    Fencing counters are global within the configured key prefix so token ordering spans
    resources. Redis script execution is atomic on one Redis history; failover or restore that
    rolls back the counter can repeat or decrease tokens. Protected storage must reject stale
    tokens, and deployments needing failover-safe fencing should use a consensus-backed allocator.
    """

    def __init__(self, client: _RedisLockClient, *, prefix: str = "orbit:lock") -> None:
        """Take ownership of a binary-mode async Redis client and its connection pool."""
        if not all(callable(getattr(client, name, None)) for name in ("eval", "aclose")):
            raise TypeError("client must provide async eval and aclose methods.")
        _validate_prefix(prefix)
        self._client = client
        self._prefix = prefix
        self._state_lock = asyncio.Lock()
        self._idle = asyncio.Event()
        self._idle.set()
        self._close_complete = asyncio.Event()
        self._active = 0
        self._closing = False
        self._closed = False

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        max_connections: int = 100,
        prefix: str = "orbit:lock",
    ) -> RedisLockProvider:
        """Create a lazy binary Redis client; configure TLS using ``rediss://``."""
        return cls.from_config(
            RedisCacheConfig(url=url, max_connections=max_connections), prefix=prefix
        )

    @classmethod
    def from_config(
        cls, config: RedisCacheConfig, *, prefix: str = "orbit:lock"
    ) -> RedisLockProvider:
        """Create a provider using Orbit Redis's validated, repr-redacted settings."""
        if not isinstance(config, RedisCacheConfig):
            raise TypeError("config must be a RedisCacheConfig instance.")
        _validate_prefix(prefix)
        try:
            client = redis_async.Redis.from_url(
                config.url,
                decode_responses=False,
                max_connections=config.max_connections,
                socket_timeout=config.socket_timeout,
                socket_connect_timeout=config.socket_connect_timeout,
            )
        except Exception:
            raise LockOperationError("Redis lock configuration is invalid.") from None
        return cls(cast(_RedisLockClient, client), prefix=prefix)

    async def try_acquire(self, resource: str, *, ttl_ms: int) -> LockLease | None:
        """Atomically acquire the resource or return ``None`` while another lease is live."""
        resource = validate_resource(resource)
        validate_ttl(ttl_ms)
        owner = secrets.token_bytes(32)
        try:
            async with self._operation():
                result = await self._client.eval(
                    _ACQUIRE_SCRIPT,
                    2,
                    self._resource_key(resource),
                    self._fence_key(),
                    owner,
                    str(ttl_ms),
                )
        except asyncio.CancelledError:
            raise
        except LockOperationError:
            raise
        except Exception:
            raise LockOperationError("Redis lock acquisition failed.") from None
        if result is None or result is False or result == 0:
            return None
        if isinstance(result, bool) or not isinstance(result, (int, bytes, str)):
            raise LockOperationError("Redis returned an invalid fencing token.")
        try:
            token = int(result)
        except (TypeError, ValueError):
            raise LockOperationError("Redis returned an invalid fencing token.") from None
        if not 1 <= token < _MAX_SAFE_FENCE:
            raise LockOperationError("Redis returned an invalid fencing token.")
        return LockLease(self, resource, owner, token)

    async def renew_lease(self, resource: str, owner: bytes, ttl_ms: int) -> bool:
        """Renew only if the Redis value still contains this lease's random owner secret."""
        validate_resource(resource)
        validate_ttl(ttl_ms)
        return await self._run_script(
            _RENEW_SCRIPT,
            self._resource_key(resource),
            owner,
            str(ttl_ms),
            failure="Redis lock renewal failed.",
        )

    async def release_lease(self, resource: str, owner: bytes) -> bool:
        """Compare owner token before deleting so an expired owner's handle cannot delete anew."""
        validate_resource(resource)
        return await self._run_script(
            _RELEASE_SCRIPT,
            self._resource_key(resource),
            owner,
            failure="Redis lock release failed.",
        )

    async def _run_script(
        self,
        script: str,
        key: str,
        owner: bytes,
        *arguments: str,
        failure: str,
    ) -> bool:
        """Execute a bounded owner-checked script and normalize backend errors."""
        try:
            async with self._operation():
                result = await self._client.eval(script, 1, key, owner, *arguments)
        except asyncio.CancelledError:
            raise
        except LockOperationError:
            raise
        except Exception:
            raise LockOperationError(failure) from None
        return result is True or result == 1 or result == b"1"

    async def aclose(self) -> None:
        """Reject new commands, wait for active commands, then close the owned pool."""
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
            raise LockOperationError("Redis lock provider shutdown failed.") from None
        async with self._state_lock:
            self._closed = True
            self._closing = False
            self._close_complete.set()

    async def __aenter__(self) -> RedisLockProvider:
        """Enter an async context without opening a Redis connection eagerly."""
        async with self._state_lock:
            if self._closing or self._closed:
                raise LockClosedError("Redis lock provider is closed.")
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Close the owned connection pool on context exit."""
        await self.aclose()

    def _resource_key(self, resource: str) -> str:
        """Hash the resource so sensitive or high-entropy names are not stored in Redis keys."""
        digest = hashlib.sha256(resource.encode("utf-8")).hexdigest()
        return f"{self._prefix}:lease:{digest}"

    def _fence_key(self) -> str:
        """Return the single prefix-scoped counter key shared across resources."""
        return f"{self._prefix}:fence-sequence"

    @asynccontextmanager
    async def _operation(self) -> AsyncIterator[None]:
        """Keep shutdown from closing the pool while Redis commands are in flight."""
        async with self._state_lock:
            if self._closing or self._closed:
                raise LockClosedError("Redis lock provider is closed.")
            self._active += 1
            self._idle.clear()
        try:
            yield
        finally:
            async with self._state_lock:
                self._active -= 1
                if self._active == 0:
                    self._idle.set()


def _validate_prefix(prefix: str) -> None:
    """Validate a Redis namespace without echoing it in errors."""
    if not isinstance(prefix, str) or not _PREFIX.fullmatch(prefix):
        raise ValueError("prefix must be 1-64 ASCII letters, digits, colon, underscore, or hyphen.")
