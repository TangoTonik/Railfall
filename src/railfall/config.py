"""Runtime configuration, read from environment variables."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    """Platform settings. Defaults target the local Docker Compose stack."""

    lake_endpoint_url: str = "http://localhost:9000"
    lake_bucket: str = "railfall-lake"
    lake_access_key: str = "railfall"
    lake_secret_key: str = field(default="railfall-secret", repr=False)
    warehouse_dsn: str = field(
        default="postgresql://rail:rail@localhost:5432/warehouse", repr=False
    )
    http_timeout_s: float = 30.0
    http_max_retries: int = 3
    # Météo-France departments to ingest. Default: the three biggest rail hubs
    # (Paris, Lyon, Marseille); the full list is ~100 files per period.
    meteo_departments: tuple[str, ...] = ("75", "69", "13")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Build settings from environment variables, falling back to defaults."""
        env = os.environ if env is None else env
        defaults = cls()

        def _float(name: str, default: float) -> float:
            raw = env.get(name)
            return default if raw is None else float(raw)

        def _int(name: str, default: int) -> int:
            raw = env.get(name)
            value = default if raw is None else int(raw)
            if value < 0:
                raise ValueError(f"{name} must be >= 0, got {value}")
            return value

        def _list(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
            raw = env.get(name)
            if raw is None:
                return default
            items = tuple(dict.fromkeys(i.strip() for i in raw.split(",") if i.strip()))
            if not items:
                raise ValueError(f"{name} must list at least one value")
            return items

        return cls(
            lake_endpoint_url=env.get("LAKE_ENDPOINT_URL", defaults.lake_endpoint_url),
            lake_bucket=env.get("LAKE_BUCKET", defaults.lake_bucket),
            lake_access_key=env.get(
                "LAKE_ACCESS_KEY", env.get("MINIO_ROOT_USER", defaults.lake_access_key)
            ),
            lake_secret_key=env.get(
                "LAKE_SECRET_KEY", env.get("MINIO_ROOT_PASSWORD", defaults.lake_secret_key)
            ),
            warehouse_dsn=env.get("WAREHOUSE_DSN", defaults.warehouse_dsn),
            http_timeout_s=_float("HTTP_TIMEOUT_S", defaults.http_timeout_s),
            http_max_retries=_int("HTTP_MAX_RETRIES", defaults.http_max_retries),
            meteo_departments=_list("METEO_DEPARTMENTS", defaults.meteo_departments),
        )
