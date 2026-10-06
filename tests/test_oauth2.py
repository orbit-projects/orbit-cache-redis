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
"""Redis OAuth adapter tests use an atomic fake and need no Redis server."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from urllib.parse import urlencode

import pytest
from orbit_auth import OAuthAuthorizationRequest, OAuthTokenResponse
from orbit_auth_oauth2 import (
    OAuth2Flow,
    OAuth2Provider,
    OAuthTransaction,
    OAuthTransactionStore,
)

from orbit_cache_redis import RedisCacheConfig
from orbit_cache_redis.oauth2 import RedisOAuthStoreError, RedisOAuthTransactionStore

STATE = "oauth-state-" + "x" * 32
SESSION = "browser-session-" + "s" * 32
SESSION_HASH = hashlib.sha256(SESSION.encode("ascii")).digest()


class FakeRedis:
    """Model SET NX, TTL, and the atomic binding-check/delete Lua operation."""

    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}
        self.expirations: dict[str, int] = {}
        self.closed = 0
        self.failure: Exception | None = None
        self.block_eval = False
        self.eval_started = asyncio.Event()
        self.eval_release = asyncio.Event()

    async def set(self, name: str, value: bytes, *, ex: int, nx: bool) -> bool | None:
        self._raise_if_failed()
        if nx and name in self.values:
            return None
        self.values[name] = value
        self.expirations[name] = ex
        return True

    async def eval(self, script: str, numkeys: int, *keys_and_args: str | bytes) -> object:
        self._raise_if_failed()
        if self.block_eval:
            self.eval_started.set()
            await self.eval_release.wait()
        if numkeys != 1 or len(keys_and_args) != 2:
            raise AssertionError("unexpected Redis Lua arguments")
        key, binding = keys_and_args
        assert isinstance(key, str) and isinstance(binding, str)
        value = self.values.get(key)
        if value is None:
            return None
        record = json.loads(value)
        if record["session_binding_hash"] != binding:
            return 0
        del self.values[key]
        return value

    async def aclose(self) -> None:
        self.closed += 1

    def _raise_if_failed(self) -> None:
        if self.failure is not None:
            raise self.failure


class Provider:
    client_id = "client-id"
    authorization_endpoint = "https://id.example/authorize"
    issuer = "https://id.example"

    def __init__(self) -> None:
        self.exchange: tuple[str, str, str | None] | None = None

    def authorize_url(self, request: OAuthAuthorizationRequest) -> str:
        query = {
            "client_id": request.client_id,
            "redirect_uri": str(request.redirect_uri),
            "response_type": "code",
            "state": request.state,
            "code_challenge": request.code_challenge or "",
            "code_challenge_method": request.code_challenge_method or "",
        }
        return self.authorization_endpoint + "?" + urlencode(query)

    async def exchange_code(
        self, code: str, *, redirect_uri: str, code_verifier: str | None = None
    ) -> OAuthTokenResponse:
        self.exchange = (code, redirect_uri, code_verifier)
        return OAuthTokenResponse(access_token="access-token", expires_in=60)


def make_transaction(*, state: str = STATE, expires_at: float | None = None) -> OAuthTransaction:
    return OAuthTransaction(
        client_id="client-id",
        redirect_uri="https://app.example/callback",
        authorization_endpoint="https://id.example/authorize",
        state=state,
        session_binding_hash=SESSION_HASH,
        nonce=None,
        code_verifier="v" * 43,
        provider_issuer="https://id.example",
        expires_at=expires_at if expires_at is not None else time.time() + 300,
        issuer=None,
    )


def state_key(state: str = STATE) -> bytes:
    return hashlib.sha256(state.encode("ascii")).digest()


async def test_redis_store_implements_capability_and_roundtrips_with_ttl() -> None:
    backend = FakeRedis()
    store = RedisOAuthTransactionStore(backend)
    assert isinstance(store, OAuthTransactionStore)

    await store.put(state_key(), make_transaction(), ttl=90)
    redis_key = f"orbit:oauth2:txn:{state_key().hex()}"
    assert backend.expirations[redis_key] == 90
    assert STATE not in redis_key

    stored = await store.take(state_key(), session_binding_hash=SESSION_HASH)

    assert stored is not None
    assert stored.state == STATE
    assert stored.code_verifier == "v" * 43
    assert await store.take(state_key(), session_binding_hash=SESSION_HASH) is None


async def test_wrong_browser_binding_does_not_consume_transaction() -> None:
    backend = FakeRedis()
    store = RedisOAuthTransactionStore(backend)
    await store.put(state_key(), make_transaction(), ttl=90)
    other_binding = hashlib.sha256(b"another-browser-session-xxxxxxxx").digest()

    assert await store.take(state_key(), session_binding_hash=other_binding) is None
    assert len(backend.values) == 1
    assert await store.take(state_key(), session_binding_hash=SESSION_HASH) is not None


async def test_state_is_consumed_only_once_under_concurrent_callbacks() -> None:
    backend = FakeRedis()
    store = RedisOAuthTransactionStore(backend)
    await store.put(state_key(), make_transaction(), ttl=90)
    first, second = await asyncio.gather(
        store.take(state_key(), session_binding_hash=SESSION_HASH),
        store.take(state_key(), session_binding_hash=SESSION_HASH),
    )
    assert sum(result is not None for result in (first, second)) == 1


async def test_expired_transaction_is_consumed_and_never_returned() -> None:
    backend = FakeRedis()
    store = RedisOAuthTransactionStore(backend)
    await store.put(state_key(), make_transaction(expires_at=time.time() - 1), ttl=90)
    assert await store.take(state_key(), session_binding_hash=SESSION_HASH) is None
    assert backend.values == {}


@pytest.mark.parametrize("key", [b"short", "x" * 32, b"x" * 33])
async def test_store_rejects_non_sha256_keys(key: object) -> None:
    with pytest.raises(ValueError, match="32-byte SHA-256"):
        await RedisOAuthTransactionStore(FakeRedis()).put(  # type: ignore[arg-type]
            key, make_transaction(), ttl=90
        )


@pytest.mark.parametrize("ttl", [0, 29, 1_801, True, 1.5])
async def test_store_rejects_unsafe_ttls(ttl: object) -> None:
    with pytest.raises(ValueError, match="TTL"):
        await RedisOAuthTransactionStore(FakeRedis()).put(
            state_key(),
            make_transaction(),
            ttl=ttl,  # type: ignore[arg-type]
        )


def test_prefix_is_validated_before_redis_client_creation() -> None:
    for prefix in ("", "bad prefix", "x" * 65):
        with pytest.raises(ValueError, match="prefix"):
            RedisOAuthTransactionStore(FakeRedis(), prefix=prefix)


async def test_redis_errors_are_sanitized_and_cancellation_propagates() -> None:
    backend = FakeRedis()
    backend.failure = RuntimeError("redis://user:password@private.example refused")
    store = RedisOAuthTransactionStore(backend)
    with pytest.raises(RedisOAuthStoreError, match="write failed") as caught:
        await store.put(state_key(), make_transaction(), ttl=90)
    assert "password" not in str(caught.value)
    backend.failure = None

    class CancelledRedis(FakeRedis):
        async def eval(self, script: str, numkeys: int, *keys_and_args: str | bytes) -> object:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await RedisOAuthTransactionStore(CancelledRedis()).take(
            state_key(), session_binding_hash=SESSION_HASH
        )


async def test_shutdown_waits_for_active_commands_and_is_idempotent() -> None:
    backend = FakeRedis()
    backend.block_eval = True
    store = RedisOAuthTransactionStore(backend)
    await store.put(state_key(), make_transaction(), ttl=90)
    operation = asyncio.create_task(store.take(state_key(), session_binding_hash=SESSION_HASH))
    await backend.eval_started.wait()
    shutdown = asyncio.create_task(store.aclose())
    await asyncio.sleep(0)
    assert backend.closed == 0
    backend.eval_release.set()
    await operation
    await shutdown
    await store.aclose()
    assert backend.closed == 1
    with pytest.raises(RedisOAuthStoreError, match="closed"):
        await store.take(state_key(), session_binding_hash=SESSION_HASH)


async def test_oauth_flow_uses_redis_as_its_browser_bound_transaction_store() -> None:
    store = RedisOAuthTransactionStore(FakeRedis())
    flow = OAuth2Flow(store)
    provider = Provider()
    start = await flow.begin(
        provider,
        client_id="client-id",
        redirect_uri="https://app.example/callback",
        session_binding=SESSION,
    )

    result = await flow.complete(
        provider,
        state=start.state,
        session_binding=SESSION,
        code="authorization-code",
    )

    assert result.tokens.access_token == "access-token"
    assert provider.exchange is not None
    assert provider.exchange[0] == "authorization-code"
    assert provider.exchange[2] is not None


def test_from_config_uses_binary_redis_client_and_redacts_configuration_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = FakeRedis()
    calls: dict[str, object] = {}

    def create_client(url: str, **options: object) -> FakeRedis:
        calls["url"] = url
        calls.update(options)
        return backend

    monkeypatch.setattr("orbit_cache_redis.oauth2.redis_async.Redis.from_url", create_client)
    store = RedisOAuthTransactionStore.from_config(
        RedisCacheConfig("rediss://user:secret@redis.example", max_connections=17)
    )
    assert calls["decode_responses"] is False
    assert calls["max_connections"] == 17
    assert "secret" not in repr(store)

    def reject_url(url: str, **options: object) -> FakeRedis:
        raise ValueError(f"bad credential-bearing URL: {url}")

    monkeypatch.setattr("orbit_cache_redis.oauth2.redis_async.Redis.from_url", reject_url)
    with pytest.raises(RedisOAuthStoreError) as caught:
        RedisOAuthTransactionStore.from_url("redis://user:secret@redis.example")
    assert "secret" not in str(caught.value)


def test_provider_contract_type_is_exported() -> None:
    """The OAuth adapter remains separate from orbit-cache-redis's base cache contract."""
    assert isinstance(Provider(), OAuth2Provider)
