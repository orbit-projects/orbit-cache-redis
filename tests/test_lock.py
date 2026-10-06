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
from __future__ import annotations

import asyncio

import pytest
from orbit_lock import LeaseLostError, LockClosedError, LockOperationError

from orbit_cache_redis.lock import (
    _ACQUIRE_SCRIPT,
    _RELEASE_SCRIPT,
    _RENEW_SCRIPT,
    RedisLockProvider,
)


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, tuple[bytes, int]] = {}
        self.sequence = 0
        self.closed = False
        self.entered = asyncio.Event()
        self.resume = asyncio.Event()
        self.block = False
        self.fail = False

    async def eval(self, script: str, numkeys: int, *args: str | bytes) -> object:
        if self.block:
            self.entered.set()
            await self.resume.wait()
        if self.fail:
            raise RuntimeError("redis password should not escape")
        if script == _ACQUIRE_SCRIPT:
            resource_key, sequence_key, owner, ttl = args
            assert isinstance(resource_key, str) and isinstance(sequence_key, str)
            assert isinstance(owner, bytes)
            assert int(ttl) > 0
            if resource_key in self.values:
                return 0
            self.sequence += 1
            self.values[resource_key] = (owner, int(ttl))
            return self.sequence
        key, owner, *rest = args
        assert isinstance(key, str) and isinstance(owner, bytes)
        current = self.values.get(key)
        if current is None or current[0] != owner:
            return 0
        if script == _RENEW_SCRIPT:
            self.values[key] = (owner, int(rest[0]))
            return 1
        if script == _RELEASE_SCRIPT:
            del self.values[key]
            return 1
        raise AssertionError(f"unexpected script: {script}")

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_acquire_is_nonblocking_and_tokens_increase_globally() -> None:
    client = FakeRedis()
    provider = RedisLockProvider(client)
    first = await provider.try_acquire("secret:resource", ttl_ms=2_000)
    assert first is not None
    assert await provider.try_acquire("secret:resource", ttl_ms=2_000) is None
    second = await provider.try_acquire("other", ttl_ms=2_000)
    assert second is not None
    assert second.fencing_token > first.fencing_token
    assert "secret:resource" not in repr(first)
    assert all("secret:resource" not in key for key in client.values)
    await first.release()
    await second.release()
    await provider.aclose()


@pytest.mark.asyncio
async def test_stale_lease_cannot_renew_or_release_reacquired_resource() -> None:
    client = FakeRedis()
    provider = RedisLockProvider(client)
    stale = await provider.try_acquire("job", ttl_ms=500)
    assert stale is not None
    stale_key = next(iter(client.values))
    del client.values[stale_key]
    current = await provider.try_acquire("job", ttl_ms=500)
    assert current is not None
    with pytest.raises(LeaseLostError):
        await stale.renew(ttl_ms=500)
    await stale.release()
    assert stale_key in client.values
    await current.renew(ttl_ms=750)
    await current.release()
    await provider.aclose()


@pytest.mark.asyncio
async def test_errors_are_normalized_and_cancellation_is_propagated() -> None:
    client = FakeRedis()
    provider = RedisLockProvider(client)
    client.fail = True
    with pytest.raises(LockOperationError, match="acquisition failed") as caught:
        await provider.try_acquire("resource", ttl_ms=500)
    assert "password" not in str(caught.value)
    client.fail = False
    client.block = True
    operation = asyncio.create_task(provider.try_acquire("resource", ttl_ms=500))
    await client.entered.wait()
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    client.resume.set()
    await provider.aclose()


@pytest.mark.asyncio
async def test_close_waits_for_inflight_commands_and_rejects_new_work() -> None:
    client = FakeRedis()
    client.block = True
    provider = RedisLockProvider(client)
    operation = asyncio.create_task(provider.try_acquire("resource", ttl_ms=500))
    await client.entered.wait()
    closing = asyncio.create_task(provider.aclose())
    await asyncio.sleep(0)
    assert not client.closed
    with pytest.raises(LockClosedError):
        await provider.try_acquire("other", ttl_ms=500)
    client.resume.set()
    await operation
    await closing
    assert client.closed
