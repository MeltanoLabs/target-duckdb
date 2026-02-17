from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
import target_duckdb

RESOURCES = Path(__file__).parent / "resources"


@pytest.fixture
def config():
    return {}


@pytest.fixture
def connection():
    conn = duckdb.connect()
    yield conn
    conn.close()


@patch("target_duckdb.flush_streams")
@patch("target_duckdb.DbSync")
def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(
    dbsync_mock,
    flush_streams_mock,
    config,
    connection,
):
    config["batch_size_rows"] = 20
    config["flush_all_streams"] = True

    lines = (RESOURCES / "logical-streams.json").read_text().splitlines(keepends=True)

    instance = dbsync_mock.return_value
    instance.create_schema_if_not_exists.return_value = None
    instance.sync_table.return_value = None

    flush_streams_mock.return_value = '{"currently_syncing": null}'

    target_duckdb.persist_lines(connection, config, lines)

    flush_streams_mock.assert_called_once()
