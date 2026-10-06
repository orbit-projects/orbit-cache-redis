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
"""Redis adapter contract tests use a fake client and need no Redis server."""

import asyncio

import pytest
from orbit import Application, ApplicationConfig
from orbit_cache import (
    CACHE_DEPENDENCY_KEY,
    AsyncCache,
    CacheConfigurationError,
    CacheOperationError,
)

from orbit_cache_redis import RedisCache, RedisCacheConfig, RedisCachePlugin


class FakeRedis:
    """Record Redis commands and provide deterministic replies for adapter tests."""

    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.expirations: dict[str, int | None] = {}
        self.closed = 0
        self.failure: Exception | None = None
        self.close_started = asyncio.Event()
        self.close_release: asyncio.Event | None = None

    async def get(self, name: str) -> bytes | None:
        self._raise_if_failed()
        return self.values.get(name)

    async def set(self, name: str, value: bytes, *, ex: int | None = None) -> bool:
        self._raise_if_failed()
        self.values[name] = value
        self.expirations[name] = ex
        return True

    async def delete(self, *names: str) -> int:
        self._raise_if_failed()
        return int(self.values.pop(names[0], None) is not None)

    async def aclose(self) -> None:
        self.close_started.set()
        if self.close_release is not None:
            await self.close_release.wait()
        self.closed += 1

    def _raise_if_failed(self) -> None:
        if self.failure is not None:
            raise self.failure


async def test_cache_implements_provider_neutral_contract() -> None:
    """The adapter is structurally usable anywhere the capability contract is expected."""
    cache: AsyncCache = RedisCache(FakeRedis())
    assert isinstance(cache, AsyncCache)


async def test_get_returns_none_on_miss_and_bytes_on_hit() -> None:
    """Cache misses and binary values retain their documented representation."""
    backend = FakeRedis()
    cache = RedisCache(backend)
    assert await cache.get("missing") is None
    await cache.set("payload", b"\x00orbit")
    assert await cache.get("payload") == b"\x00orbit"


async def test_set_forwards_ttl_as_seconds() -> None:
    """The provider adapter preserves the capability's whole-seconds TTL contract."""
    backend = FakeRedis()
    await RedisCache(backend).set("session", b"value", ttl=60)
    assert backend.expirations["session"] == 60


async def test_delete_reports_if_key_was_present() -> None:
    """Delete returns true once and false after the key is absent."""
    cache = RedisCache(FakeRedis())
    await cache.set("key", b"value")
    assert await cache.delete("key") is True
    assert await cache.delete("key") is False


@pytest.mark.parametrize("key", ["", 1, None])
async def test_rejects_invalid_keys(key: object) -> None:
    """Invalid names are rejected before reaching the backend."""
    with pytest.raises(CacheConfigurationError):
        await RedisCache(FakeRedis()).get(key)  # type: ignore[arg-type]


@pytest.mark.parametrize("ttl", [0, -1, True, 1.5, 2_147_483_648])
async def test_rejects_invalid_ttls(ttl: object) -> None:
    """TTL validation excludes zero, booleans, fractions, and oversized Redis expiries."""
    with pytest.raises(CacheConfigurationError):
        await RedisCache(FakeRedis()).set("key", b"value", ttl=ttl)  # type: ignore[arg-type]


async def test_rejects_text_values() -> None:
    """The public API does not silently choose an encoding for application strings."""
    with pytest.raises(CacheConfigurationError):
        await RedisCache(FakeRedis()).set("key", "value")  # type: ignore[arg-type]


async def test_backend_errors_are_normalized_without_details() -> None:
    """Underlying exception strings and connection details are never re-exposed."""
    backend = FakeRedis()
    backend.failure = RuntimeError("redis://user:secret@example.invalid refused")
    with pytest.raises(CacheOperationError, match="Redis cache read failed") as caught:
        await RedisCache(backend).get("key")
    assert "secret" not in str(caught.value)


async def test_cancellation_propagates() -> None:
    """Cancellation remains control flow and is not converted into a backend error."""

    class CancelledRedis(FakeRedis):
        async def get(self, name: str) -> bytes | None:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await RedisCache(CancelledRedis()).get("key")


async def test_close_is_idempotent_and_blocks_later_operations() -> None:
    """The adapter releases its owned client once and rejects use after closure."""
    backend = FakeRedis()
    cache = RedisCache(backend)
    await cache.aclose()
    await cache.aclose()
    assert backend.closed == 1
    with pytest.raises(CacheOperationError, match="closed"):
        await cache.get("key")


async def test_close_waits_for_inflight_commands_and_rejects_new_work() -> None:
    class SlowRedis(FakeRedis):
        def __init__(self) -> None:
            super().__init__()
            self.read_started = asyncio.Event()
            self.read_release = asyncio.Event()

        async def get(self, name: str) -> bytes | None:
            self.read_started.set()
            await self.read_release.wait()
            return await super().get(name)

    backend = SlowRedis()
    cache = RedisCache(backend)
    read = asyncio.create_task(cache.get("active"))
    await backend.read_started.wait()
    close = asyncio.create_task(cache.aclose())
    await asyncio.sleep(0)

    with pytest.raises(CacheOperationError, match="closed"):
        await cache.get("new")
    assert backend.closed == 0

    backend.read_release.set()
    assert await read is None
    await close
    assert backend.closed == 1


async def test_cancelled_close_waiter_does_not_abandon_shared_shutdown() -> None:
    backend = FakeRedis()
    backend.close_release = asyncio.Event()
    cache = RedisCache(backend)
    first = asyncio.create_task(cache.aclose())
    await backend.close_started.wait()
    second = asyncio.create_task(cache.aclose())
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    retried_close = asyncio.create_task(cache.aclose())
    await asyncio.sleep(0)
    backend.close_release.set()
    await asyncio.gather(second, retried_close)
    assert backend.closed == 1
    with pytest.raises(CacheOperationError, match="closed"):
        await cache.get("key")


def test_redis_config_hides_url_from_repr_and_validates_bounds() -> None:
    """Connection credentials stay out of repr and resource limits reject unsafe values."""
    config = RedisCacheConfig("rediss://user:secret@cache.example", max_connections=10)
    assert "secret" not in repr(config)
    with pytest.raises(CacheConfigurationError):
        RedisCacheConfig("redis://localhost", max_connections=0)


async def test_core_plugin_registers_contract_and_closes_adapter() -> None:
    """The optional plugin wires Core's container/lifecycle to the separate cache capability."""
    backend = FakeRedis()
    cache = RedisCache(backend)
    app = Application(ApplicationConfig(name="cache-test"))
    plugin = RedisCachePlugin(cache)
    app.register_plugin(plugin)
    app.plugins.setup(app)
    resolved: AsyncCache = app.container.resolve(CACHE_DEPENDENCY_KEY)
    assert resolved is cache
    await app.plugins.activate()
    await app.plugins.deactivate()
    assert backend.closed == 1


def test_from_url_configures_lazy_binary_client_without_exposing_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The convenience factory applies bounded settings and defers network access."""
    backend = FakeRedis()
    calls: dict[str, object] = {}

    def create_client(url: str, **options: object) -> FakeRedis:
        calls["url"] = url
        calls.update(options)
        return backend

    monkeypatch.setattr("orbit_cache_redis.cache.redis_async.Redis.from_url", create_client)
    cache = RedisCache.from_url("rediss://user:secret@cache.example", max_connections=12)
    assert cache is not None
    assert calls["max_connections"] == 12
    assert calls["decode_responses"] is False
    assert calls["socket_timeout"] == 5.0


def test_from_url_redacts_client_configuration_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Malformed provider configuration is normalized without echoing URL details."""

    def reject_url(url: str, **options: object) -> FakeRedis:
        raise ValueError(f"rejected credential-bearing URL: {url}")

    monkeypatch.setattr("orbit_cache_redis.cache.redis_async.Redis.from_url", reject_url)
    with pytest.raises(CacheConfigurationError) as caught:
        RedisCache.from_url("redis://user:secret@cache.example")
    assert "secret" not in str(caught.value)
