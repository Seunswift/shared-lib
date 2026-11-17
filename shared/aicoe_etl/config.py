"""Connection settings for Trino (source) and ClickHouse (target)."""
import os
from dataclasses import dataclass, field
from typing import Optional, Union


def _parse_verify(value) -> Union[bool, str]:
    """'true'/'false' -> bool ; any other text = path to a CA bundle."""
    if isinstance(value, bool):
        return value
    v = str(value).strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off", ""):
        return False
    return str(value)


@dataclass
class TrinoConfig:
    host: str
    user: str
    port: int = 443
    password: Optional[str] = None      # None -> no authentication header
    catalog: str = "hive"
    schema: str = "default"
    http_scheme: str = "https"
    verify: Union[bool, str] = False    # True/False or path to a CA bundle
    extra: dict = field(default_factory=dict)  # any other kwargs for trino.dbapi.connect

    @classmethod
    def from_env(cls, **overrides) -> "TrinoConfig":
        """Reads TRINO_HOST, TRINO_USER, TRINO_PORT, TRINO_PASSWORD, TRINO_CATALOG,
        TRINO_SCHEMA, TRINO_HTTP_SCHEME, TRINO_VERIFY. Keyword overrides win."""
        e = os.environ
        values = {
            "host": e.get("TRINO_HOST"),
            "user": e.get("TRINO_USER"),
            "port": int(e.get("TRINO_PORT", 443)),
            "password": e.get("TRINO_PASSWORD") or None,
            "catalog": e.get("TRINO_CATALOG", "hive"),
            "schema": e.get("TRINO_SCHEMA", "default"),
            "http_scheme": e.get("TRINO_HTTP_SCHEME", "https"),
            "verify": _parse_verify(e.get("TRINO_VERIFY", "false")),
        }
        values.update({k: v for k, v in overrides.items() if v is not None})
        if not values["host"] or not values["user"]:
            raise ValueError("TRINO_HOST and TRINO_USER are required.")
        return cls(**values)


@dataclass
class ClickHouseConfig:
    host: str
    port: int = 8123                    # HTTP interface (clickhouse-connect uses HTTP)
    user: str = "default"
    password: str = ""
    database: str = "default"
    secure: bool = False
    extra: dict = field(default_factory=dict)  # any other kwargs for clickhouse_connect.get_client

    @classmethod
    def from_env(cls, **overrides) -> "ClickHouseConfig":
        """Reads CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE, CH_SECURE."""
        e = os.environ
        values = {
            "host": e.get("CH_HOST"),
            "port": int(e.get("CH_PORT", 8123)),
            "user": e.get("CH_USER", "default"),
            "password": e.get("CH_PASSWORD", ""),
            "database": e.get("CH_DATABASE", "default"),
            "secure": e.get("CH_SECURE", "false").strip().lower() in ("1", "true", "yes", "on"),
        }
        values.update({k: v for k, v in overrides.items() if v is not None})
        if not values["host"]:
            raise ValueError("CH_HOST is required.")
        return cls(**values)
