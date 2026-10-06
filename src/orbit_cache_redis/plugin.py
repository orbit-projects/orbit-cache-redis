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
"""Optional Orbit Core plugin that registers and owns a Redis cache adapter."""

from orbit import Application
from orbit.plugins import Plugin, PluginMetadata
from orbit_cache import CACHE_DEPENDENCY_KEY

from orbit_cache_redis.cache import RedisCache


class RedisCachePlugin(Plugin):
    """Expose one cache instance as ``AsyncCache`` and close it during plugin shutdown.

    The application must explicitly install and register this package/plugin. The plugin does not
    connect eagerly; redis-py establishes connections when the first cache operation is awaited.
    """

    metadata = PluginMetadata(
        name="orbit-cache-redis",
        version="0.1.0a1",
        capabilities=frozenset({"cache.redis"}),
    )

    def __init__(self, cache: RedisCache) -> None:
        """Take lifecycle ownership of a configured ``RedisCache`` instance."""
        if not isinstance(cache, RedisCache):
            raise TypeError("RedisCachePlugin requires a RedisCache instance.")
        self.cache = cache

    def setup(self, application: Application) -> None:
        """Register the adapter against the provider-neutral cache contract."""
        if not isinstance(application, Application):
            raise TypeError("RedisCachePlugin requires an Orbit Application.")
        # Core's generic dependency registry accepts types or stable string keys. The capability
        # publishes this key so integrations agree on the same container boundary.
        application.container.register_instance(CACHE_DEPENDENCY_KEY, self.cache)

    async def deactivate(self) -> None:
        """Close the cache during normal shutdown or partial-startup rollback."""
        await self.cache.aclose()


__all__ = ["RedisCachePlugin"]
