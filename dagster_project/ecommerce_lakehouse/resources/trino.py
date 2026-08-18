"""Trino resource: SQL access to the lakehouse query engine."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import trino.dbapi
from dagster import ConfigurableResource
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(REPO_ROOT / ".env")


class TrinoResource(ConfigurableResource):
    """Dagster resource for running SQL against the local Trino coordinator.

    Defaults come from the repo-root .env (TRINO_HOST / TRINO_PORT /
    TRINO_USER / TRINO_CATALOG) and can be overridden in the Dagster UI.
    """

    host: str = os.environ.get("TRINO_HOST", "localhost")
    port: int = int(os.environ.get("TRINO_PORT", "8080"))
    user: str = os.environ.get("TRINO_USER", "admin")
    catalog: str = os.environ.get("TRINO_CATALOG", "iceberg")

    def connect(self) -> "trino.dbapi.Connection":
        """Open a new Trino DBAPI connection."""
        return trino.dbapi.connect(host=self.host, port=self.port, user=self.user)

    def execute(self, sql: str) -> None:
        """Execute a statement (DDL/DML); no results expected."""
        cur = self.connect().cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def fetch(self, sql: str) -> list[tuple[Any, ...]]:
        """Execute a query and return all rows."""
        cur = self.connect().cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()
