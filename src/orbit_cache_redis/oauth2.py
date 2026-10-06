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
"""Optional Redis adapter for Orbit OAuth2's browser-bound transaction-store contract."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import math
import re
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Protocol, cast

import redis.asyncio as redis_async
from orbit_auth_oauth2 import OAuthTransaction, OAuthTransactionStore

from orbit_cache_redis.config import RedisCacheConfig

_MIN_TTL = 30
_MAX_TTL = 1_800
_MAX_TRANSACTION_BYTES = 8_192
_PREFIX = re.compile(r"[A-Za-z0-9][A-Za-z0-9:_-]{0,63}\Z")
_TAKE_SCRIPT = """
local value = redis.call('GET', KEYS[1])
if not value then return nil end
local transaction = cjson.decode(value)
if transaction['session_binding_hash'] ~= ARGV[1] then return false end
redis.call('DEL', KEYS[1])
return value
"""


class RedisOAuthStoreError(Exception):
    """Safe Redis transaction-store failure without connection or transaction details."""


class _RedisTransactionClient(Protocol):
    """Narrow redis-py async methods used by this adapter and its tests."""

    async def set(self, name: str, value: bytes, *, ex: int, nx: bool) -> bool | None:
        """Store a value only when no unexpired key already exists."""

    async def eval(self, script: str, numkeys: int, *keys_and_args: str | bytes) -> object:
        """Evaluate one bounded atomic Redis Lua operation."""

    async def aclose(self) -> None:
        """Close the client and its owned connection pool."""


class RedisOAuthTransactionStore:
    """Shared, TTL-bound implementation of ``OAuthTransactionStore`` using Redis.

    The Redis Lua operation verifies the browser-session digest and deletes the transaction in one
    atomic operation. A wrong browser session cannot consume another session's state. The store
    owns its Redis client, waits for active operations before closing it, and rejects new work once
    shutdown starts. Configure Redis TLS, ACLs, backups, and encryption-at-rest policy separately.
    """

    def __init__(
        self, client: _RedisTransactionClient, *, prefix: str = "orbit:oauth2:txn"
    ) -> None:
        """Wrap a binary-mode async Redis client and take responsibility for its lifecycle."""
        if not all(callable(getattr(client, name, None)) for name in ("set", "eval", "aclose")):
            raise TypeError("client must provide async set, eval, and aclose methods.")
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
        prefix: str = "orbit:oauth2:txn",
    ) -> RedisOAuthTransactionStore:
        """Create a lazy binary Redis client; use ``rediss://`` to enable TLS transport."""
        return cls.from_config(
            RedisCacheConfig(url=url, max_connections=max_connections), prefix=prefix
        )

    @classmethod
    def from_config(
        cls, config: RedisCacheConfig, *, prefix: str = "orbit:oauth2:txn"
    ) -> RedisOAuthTransactionStore:
        """Create a store from Orbit Redis's validated, repr-redacted connection settings."""
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
            raise RedisOAuthStoreError("Redis OAuth store configuration is invalid.") from None
        return cls(cast(_RedisTransactionClient, client), prefix=prefix)

    async def put(self, key: bytes, transaction: OAuthTransaction, *, ttl: int) -> None:
        """Persist one validated transaction with a bounded TTL and collision-safe SET NX."""
        _validate_key(key)
        _validate_ttl(ttl)
        payload = _serialize_transaction(key, transaction)
        redis_key = self._redis_key(key)
        try:
            async with self._operation():
                stored = await self._client.set(redis_key, payload, ex=ttl, nx=True)
        except asyncio.CancelledError:
            raise
        except RedisOAuthStoreError:
            raise
        except Exception:
            raise RedisOAuthStoreError("Redis OAuth transaction write failed.") from None
        if stored is not True:
            raise RedisOAuthStoreError("Redis OAuth transaction write failed.")

    async def take(self, key: bytes, *, session_binding_hash: bytes) -> OAuthTransaction | None:
        """Atomically match browser-session binding and consume a pending transaction once."""
        _validate_key(key)
        if not isinstance(session_binding_hash, bytes) or len(session_binding_hash) != 32:
            raise ValueError("session_binding_hash must be a 32-byte SHA-256 digest.")
        binding_text = _encode(session_binding_hash)
        try:
            async with self._operation():
                result = await self._client.eval(
                    _TAKE_SCRIPT, 1, self._redis_key(key), binding_text
                )
        except asyncio.CancelledError:
            raise
        except RedisOAuthStoreError:
            raise
        except Exception:
            raise RedisOAuthStoreError("Redis OAuth transaction read failed.") from None
        if result is None or result is False or result == 0:
            return None
        if not isinstance(result, bytes) or not 1 <= len(result) <= _MAX_TRANSACTION_BYTES:
            raise RedisOAuthStoreError("Redis returned an invalid OAuth transaction.")
        transaction = _deserialize_transaction(key, result)
        if not hmac.compare_digest(transaction.session_binding_hash, session_binding_hash):
            return None
        if transaction.expires_at <= time.time():
            return None
        return transaction

    async def aclose(self) -> None:
        """Reject new operations, wait for in-flight Redis commands, then close the owned client."""
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
            raise RedisOAuthStoreError("Redis OAuth store shutdown failed.") from None
        async with self._state_lock:
            self._closed = True
            self._closing = False
            self._close_complete.set()

    async def __aenter__(self) -> RedisOAuthTransactionStore:
        """Enter an async context without opening a network connection eagerly."""
        async with self._state_lock:
            if self._closing or self._closed:
                raise RedisOAuthStoreError("Redis OAuth store is closed.")
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Close the owned client when leaving an async context."""
        await self.aclose()

    def _redis_key(self, digest: bytes) -> str:
        """Namespace one already-hashed OAuth state without exposing the raw state value."""
        return f"{self._prefix}:{digest.hex()}"

    @asynccontextmanager
    async def _operation(self) -> AsyncIterator[None]:
        """Track active Redis commands so shutdown does not close their connection pool early."""
        async with self._state_lock:
            if self._closing or self._closed:
                raise RedisOAuthStoreError("Redis OAuth store is closed.")
            self._active += 1
            self._idle.clear()
        try:
            yield
        finally:
            async with self._state_lock:
                self._active -= 1
                if self._active == 0:
                    self._idle.set()


def _serialize_transaction(key: bytes, transaction: OAuthTransaction) -> bytes:
    """Validate fixed transaction shape and encode a bounded JSON record for Redis."""
    if not isinstance(transaction, OAuthTransaction):
        raise TypeError("transaction must be an OAuthTransaction instance.")
    _validate_transaction_text(transaction)
    if not hmac.compare_digest(key, hashlib.sha256(transaction.state.encode("ascii")).digest()):
        raise ValueError("transaction state does not match its opaque storage key.")
    if (
        not isinstance(transaction.session_binding_hash, bytes)
        or len(transaction.session_binding_hash) != 32
    ):
        raise ValueError("transaction session binding must be a SHA-256 digest.")
    if (
        not isinstance(transaction.expires_at, (int, float))
        or isinstance(transaction.expires_at, bool)
        or not math.isfinite(transaction.expires_at)
    ):
        raise ValueError("transaction expiry must be finite.")
    payload = {
        "client_id": transaction.client_id,
        "redirect_uri": transaction.redirect_uri,
        "authorization_endpoint": transaction.authorization_endpoint,
        "state": transaction.state,
        "session_binding_hash": _encode(transaction.session_binding_hash),
        "nonce": transaction.nonce,
        "code_verifier": transaction.code_verifier,
        "provider_issuer": transaction.provider_issuer,
        "expires_at": float(transaction.expires_at),
        "issuer": transaction.issuer,
    }
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    if len(encoded) > _MAX_TRANSACTION_BYTES:
        raise ValueError("transaction exceeds the Redis store size limit.")
    return encoded


def _deserialize_transaction(key: bytes, value: bytes) -> OAuthTransaction:
    """Parse an untrusted stored value into the exact Orbit transaction shape."""
    try:
        data = json.loads(value, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise RedisOAuthStoreError("Redis contains an invalid OAuth transaction.") from None
    fields = {
        "client_id",
        "redirect_uri",
        "authorization_endpoint",
        "state",
        "session_binding_hash",
        "nonce",
        "code_verifier",
        "provider_issuer",
        "expires_at",
        "issuer",
    }
    if not isinstance(data, dict) or set(data) != fields:
        raise RedisOAuthStoreError("Redis contains an invalid OAuth transaction.")
    if any(
        not isinstance(data[name], str) or not data[name]
        for name in fields
        - {"session_binding_hash", "nonce", "provider_issuer", "expires_at", "issuer"}
    ):
        raise RedisOAuthStoreError("Redis contains an invalid OAuth transaction.")
    try:
        binding = _decode(data["session_binding_hash"])
        expires_at = data["expires_at"]
        state = data["state"]
        if (
            len(binding) != 32
            or not isinstance(expires_at, (int, float))
            or isinstance(expires_at, bool)
            or not math.isfinite(expires_at)
            or not isinstance(state, str)
            or not hmac.compare_digest(key, hashlib.sha256(state.encode("ascii")).digest())
        ):
            raise ValueError
        if data["nonce"] is not None and (
            not isinstance(data["nonce"], str)
            or not 16 <= len(data["nonce"]) <= 512
            or _has_control(data["nonce"])
        ):
            raise ValueError
        if data["provider_issuer"] is not None and not _valid_optional_text(
            data["provider_issuer"]
        ):
            raise ValueError
        if data["issuer"] is not None and not _valid_optional_text(data["issuer"]):
            raise ValueError
        transaction = OAuthTransaction(
            client_id=data["client_id"],
            redirect_uri=data["redirect_uri"],
            authorization_endpoint=data["authorization_endpoint"],
            state=state,
            session_binding_hash=binding,
            nonce=data["nonce"],
            code_verifier=data["code_verifier"],
            provider_issuer=data["provider_issuer"],
            expires_at=float(expires_at),
            issuer=data["issuer"],
        )
        _validate_transaction_text(transaction)
        return transaction
    except (UnicodeError, TypeError, ValueError, OverflowError):
        raise RedisOAuthStoreError("Redis contains an invalid OAuth transaction.") from None


def _validate_key(key: bytes) -> None:
    """Require the SHA-256 sized opaque key defined by the OAuth capability contract."""
    if not isinstance(key, bytes) or len(key) != 32:
        raise ValueError("OAuth transaction key must be a 32-byte SHA-256 digest.")


def _validate_prefix(prefix: str) -> None:
    """Reject unsafe or overly long key namespaces before creating a Redis client."""
    if not isinstance(prefix, str) or _PREFIX.fullmatch(prefix) is None:
        raise ValueError("prefix must be 1 to 64 safe ASCII Redis key characters.")


def _validate_transaction_text(transaction: OAuthTransaction) -> None:
    """Bound persisted fields and validate PKCE/state text before it reaches Redis or Lua."""
    for name, value, maximum in (
        ("client_id", transaction.client_id, 255),
        ("redirect_uri", transaction.redirect_uri, 2_048),
        ("authorization_endpoint", transaction.authorization_endpoint, 2_048),
        ("state", transaction.state, 512),
        ("code_verifier", transaction.code_verifier, 128),
    ):
        if not isinstance(value, str) or not 1 <= len(value) <= maximum or _has_control(value):
            raise ValueError(f"transaction {name} is invalid.")
    if not transaction.state.isascii() or not transaction.code_verifier.isascii():
        raise ValueError("transaction state and code verifier must be ASCII.")
    if not 16 <= len(transaction.state) <= 512:
        raise ValueError("transaction state length is invalid.")
    if not 43 <= len(transaction.code_verifier) <= 128:
        raise ValueError("transaction code verifier length is invalid.")
    if (
        any(
            character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
            for character in transaction.code_verifier
        )
        or not _valid_optional_text(transaction.nonce)
        or (transaction.nonce is not None and len(transaction.nonce) < 16)
        or not _valid_optional_text(transaction.issuer)
    ):
        raise ValueError("transaction OIDC metadata is invalid.")
    if not _valid_optional_text(transaction.provider_issuer):
        raise ValueError("transaction provider issuer is invalid.")


def _valid_optional_text(value: object) -> bool:
    """Validate an optional bounded issuer or nonce without leaking its value."""
    if value is None:
        return True
    return isinstance(value, str) and 1 <= len(value) <= 2_048 and not _has_control(value)


def _has_control(value: str) -> bool:
    """Return whether text contains ASCII controls or DEL."""
    return any(ord(character) < 32 or ord(character) == 127 for character in value)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate JSON object keys instead of accepting last-value-wins metadata."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate transaction field")
        result[key] = value
    return result


def _validate_ttl(ttl: int) -> None:
    """Bound expiry input independently of caller-side flow validation."""
    if isinstance(ttl, bool) or not isinstance(ttl, int) or not _MIN_TTL <= ttl <= _MAX_TTL:
        raise ValueError(
            f"OAuth transaction TTL must be between {_MIN_TTL} and {_MAX_TTL} seconds."
        )


def _encode(value: bytes) -> str:
    """Encode a fixed-length digest for JSON and Redis Lua string comparison."""
    return base64.urlsafe_b64encode(value).decode("ascii")


def _decode(value: str) -> bytes:
    """Decode strict URL-safe base64 without accepting ignored invalid characters."""
    if not isinstance(value, str) or not value.isascii():
        raise ValueError("encoded digest is invalid")
    return base64.b64decode(value.encode("ascii"), altchars=b"-_", validate=True)


def _implements_oauth_transaction_contract(
    store: RedisOAuthTransactionStore,
) -> OAuthTransactionStore:
    """Keep the Redis implementation statically checked against the capability contract."""
    return store


__all__ = ["RedisOAuthStoreError", "RedisOAuthTransactionStore"]
