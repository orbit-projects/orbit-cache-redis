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
"""Validated, secret-aware connection settings for the Redis cache adapter."""

import math
from dataclasses import dataclass, field

from orbit_cache import CacheConfigurationError


@dataclass(frozen=True, slots=True)
class RedisCacheConfig:
    """Redis URL and bounded socket/pool settings.

    The URL is excluded from repr because Redis URLs commonly contain credentials. redis-py parses
    the URL and supports TCP, TLS, and Unix-domain socket schemes.
    """

    url: str = field(repr=False)
    max_connections: int = 100
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0

    def __post_init__(self) -> None:
        """Reject malformed basic settings without echoing secrets in an error message."""
        if not isinstance(self.url, str) or not self.url.strip():
            raise CacheConfigurationError("Redis URL must be a non-empty string.")
        if not isinstance(self.max_connections, int) or isinstance(self.max_connections, bool):
            raise CacheConfigurationError("max_connections must be a positive integer.")
        if self.max_connections < 1 or self.max_connections > 10_000:
            raise CacheConfigurationError("max_connections must be between 1 and 10000.")
        for name, value in (
            ("socket_timeout", self.socket_timeout),
            ("socket_connect_timeout", self.socket_connect_timeout),
        ):
            valid_number = isinstance(value, (int, float)) and not isinstance(value, bool)
            try:
                valid_finite = valid_number and math.isfinite(value)
            except OverflowError:
                valid_finite = False
            if not valid_finite or not 0 < value <= 300:
                raise CacheConfigurationError(f"{name} must be finite and between 0 and 300.")


__all__ = ["RedisCacheConfig"]
