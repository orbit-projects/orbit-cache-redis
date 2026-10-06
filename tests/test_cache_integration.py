"""Opt-in live Redis integration check; set ORBIT_REDIS_URL to enable it."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest

from orbit_cache_redis import RedisCache


@pytest.mark.asyncio
@pytest.mark.skipif(not os.environ.get("ORBIT_REDIS_URL"), reason="ORBIT_REDIS_URL is not set")
async def test_live_redis_binary_ttl_and_key_lifecycle() -> None:
    """Exercise actual server I/O without requiring a service in ordinary unit-test runs."""
    url = os.environ["ORBIT_REDIS_URL"]
    key = f"orbit:integration:{uuid4().hex}"
    expiration_key = f"{key}:expiration"
    cache = RedisCache.from_url(url)
    try:
        await cache.set(key, b"\x00orbit\xff", ttl=30)
        assert await cache.get(key) == b"\x00orbit\xff"
        assert await cache.delete(key) is True
        assert await cache.get(key) is None
        await cache.set(expiration_key, b"expires", ttl=1)
        assert await cache.get(expiration_key) == b"expires"
        await asyncio.sleep(1.1)
        assert await cache.get(expiration_key) is None
    finally:
        await cache.aclose()
