from __future__ import annotations

from typing import Any

import duckdb
import pytest
from dotenv import load_dotenv

import target_duckdb
from target_duckdb.db_sync import DbSync

try:
    import tests.utils as test_utils
except ImportError:
    import utils as test_utils

METADATA_COLUMNS = [
    "_sdc_extracted_at",
    "_sdc_batched_at",
    "_sdc_deleted_at",
]


@pytest.fixture(scope="session", autouse=True)
def _load_env():
    load_dotenv()


@pytest.fixture(scope="session")
def target_schema() -> str:
    return "integration_test_schema"


@pytest.fixture(scope="class")
def config(target_schema: str) -> dict[str, Any]:
    cfg = test_utils.get_db_config()
    cfg["default_target_schema"] = target_schema
    return cfg


@pytest.fixture
def connection(config: dict[str, Any]) -> duckdb.DuckDBPyConnection:
    return target_duckdb.duckdb_connect(config)


@pytest.fixture
def instance(
    config: dict[str, Any],
    connection: duckdb.DuckDBPyConnection,
) -> DbSync:
    return DbSync(connection, config)


@pytest.fixture
def prepare(target_schema: str, instance: DbSync):
    instance.conn.query(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE")


def remove_metadata_columns_from_rows(rows: list[dict]) -> list[dict]:
    """Remove Singer metadata columns from a list of row dicts."""
    return [{k: v for k, v in row.items() if k not in METADATA_COLUMNS} for row in rows]


def assert_metadata_columns_exist(rows: list[dict[str, Any]]):
    """Assert that every row has all metadata columns."""
    assert all(md_c in r for r in rows for md_c in METADATA_COLUMNS)


def assert_metadata_columns_not_exist(rows: list[dict]):
    """Assert that no row has any metadata column."""
    assert not any(md_c in r for r in rows for md_c in METADATA_COLUMNS)
