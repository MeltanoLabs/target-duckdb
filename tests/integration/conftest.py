from __future__ import annotations

from pathlib import Path
from typing import Any, TypeAlias, Callable

import duckdb
import pytest
from dotenv import load_dotenv

import target_duckdb
from target_duckdb.db_sync import DbSync

FileLoader: TypeAlias = Callable[[str], list[str]]

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
    config: dict[str, Any] = {}

    # --------------------------------------------------------------------------
    # Default configuration settings for integration tests.
    # --------------------------------------------------------------------------
    # The following values needs to be defined in environment variables with
    # valid details to a local DuckDB file
    # --------------------------------------------------------------------------
    # DuckDB file path/schema
    config["path"] = "md:target_duckdb"
    config["default_target_schema"] = "integration_test_schema"

    # --------------------------------------------------------------------------
    # The following variables needs to be empty.
    # The tests cases will set them automatically whenever it's needed
    # --------------------------------------------------------------------------
    config["disable_table_cache"] = None
    config["schema_mapping"] = None
    config["add_metadata_columns"] = None
    config["hard_delete"] = None
    config["flush_all_streams"] = None
    config["default_target_schema"] = target_schema

    return config


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


@pytest.fixture
def get_test_tap_lines(shared_datadir: Path) -> FileLoader:
    """Read test tap lines from a resource file."""

    def _get_test_tap_lines(filename: str) -> list[str]:
        with open(shared_datadir / filename) as tap_stdout:
            return list(tap_stdout.readlines())

    return _get_test_tap_lines


def remove_metadata_columns_from_rows(rows: list[dict]) -> list[dict]:
    """Remove Singer metadata columns from a list of row dicts."""
    return [{k: v for k, v in row.items() if k not in METADATA_COLUMNS} for row in rows]


def assert_metadata_columns_exist(rows: list[dict[str, Any]]):
    """Assert that every row has all metadata columns."""
    assert all(md_c in r for r in rows for md_c in METADATA_COLUMNS)


def assert_metadata_columns_not_exist(rows: list[dict]):
    """Assert that no row has any metadata column."""
    assert not any(md_c in r for r in rows for md_c in METADATA_COLUMNS)
