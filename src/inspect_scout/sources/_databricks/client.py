"""Databricks SQL connector client helpers."""

import os
from dataclasses import dataclass

from inspect_scout._view.databricks_auth import get_forwarded_access_token


@dataclass(frozen=True)
class DatabricksConnectionConfig:
    """Connection settings for Databricks SQL."""

    server_hostname: str
    http_path: str
    access_token: str


def resolve_databricks_config(
    server_hostname: str | None,
    http_path: str | None,
    access_token: str | None,
) -> DatabricksConnectionConfig:
    """Resolve Databricks connection settings from args/environment/context.

    Token resolution order:
    1. Explicit ``access_token`` argument
    2. Forwarded Databricks OBO user token from request headers
    3. ``DATABRICKS_TOKEN`` environment variable
    """
    resolved_server_hostname = server_hostname or os.environ.get(
        "DATABRICKS_SERVER_HOSTNAME"
    )
    resolved_http_path = http_path or os.environ.get("DATABRICKS_HTTP_PATH")
    resolved_access_token = (
        access_token
        or get_forwarded_access_token()
        or os.environ.get("DATABRICKS_TOKEN")
    )

    if not resolved_server_hostname:
        raise ValueError(
            "Databricks server hostname is required. Provide server_hostname or set "
            "DATABRICKS_SERVER_HOSTNAME."
        )
    if not resolved_http_path:
        raise ValueError(
            "Databricks HTTP path is required. Provide http_path or set "
            "DATABRICKS_HTTP_PATH."
        )
    if not resolved_access_token:
        raise ValueError(
            "Databricks access token is required. Provide access_token, run under "
            "Databricks forwarded user authorization, or set DATABRICKS_TOKEN."
        )

    return DatabricksConnectionConfig(
        server_hostname=resolved_server_hostname,
        http_path=resolved_http_path,
        access_token=resolved_access_token,
    )


def connect_databricks_sql(config: DatabricksConnectionConfig):
    """Create a Databricks SQL connector connection."""
    try:
        from databricks import sql
    except ImportError as e:
        raise ImportError(
            "databricks-sql-connector is required for Databricks import. "
            "Install it with: pip install databricks-sql-connector"
        ) from e

    return sql.connect(
        server_hostname=config.server_hostname,
        http_path=config.http_path,
        access_token=config.access_token,
    )
