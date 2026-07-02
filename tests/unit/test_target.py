from __future__ import annotations

import gzip
import json
from pathlib import Path
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
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


def _schema_line(stream="mydb-mytable", key_properties=("id",)):
    return (
        json.dumps(
            {
                "type": "SCHEMA",
                "stream": stream,
                "schema": {
                    "type": "object",
                    "properties": {
                        "id": {"type": ["null", "integer"]},
                        "name": {"type": ["null", "string"]},
                    },
                },
                "key_properties": list(key_properties),
            }
        )
        + "\n"
    )


def _batch_line(stream, manifest, encoding):
    return (
        json.dumps(
            {
                "type": "BATCH",
                "stream": stream,
                "encoding": encoding,
                "manifest": manifest,
            }
        )
        + "\n"
    )


@patch("target_duckdb.flush_streams")
@patch("target_duckdb.DbSync")
def test_persist_lines_dispatches_arrow_batch_to_load_rows_from_arrow_files(
    dbsync_mock,
    flush_streams_mock,
    config,
    connection,
    tmp_path,
):
    stream = "mydb-mytable"
    instance = dbsync_mock.return_value
    instance.create_schema_if_not_exists.return_value = None
    instance.sync_table.return_value = None
    flush_streams_mock.return_value = None

    arrow_path = tmp_path / "batch.arrow"
    batch = pa.RecordBatch.from_pylist([{"id": 1, "name": "a"}])
    with ipc.new_file(str(arrow_path), batch.schema) as writer:
        writer.write_batch(batch)

    lines = [
        _schema_line(stream),
        _batch_line(stream, [f"file://{arrow_path}"], {"format": "arrow"}),
    ]

    target_duckdb.persist_lines(connection, config, lines)

    instance.load_rows_from_arrow_files.assert_called_once_with([str(arrow_path)])
    flush_streams_mock.assert_not_called()


@patch("target_duckdb.flush_streams")
@patch("target_duckdb.DbSync")
def test_persist_lines_dispatches_jsonl_gz_batch_to_load_rows_from_json_files(
    dbsync_mock,
    flush_streams_mock,
    config,
    connection,
    tmp_path,
):
    stream = "mydb-mytable"
    instance = dbsync_mock.return_value
    instance.create_schema_if_not_exists.return_value = None
    instance.sync_table.return_value = None
    flush_streams_mock.return_value = None

    batch_path = tmp_path / "batch.jsonl.gz"
    with gzip.open(batch_path, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"id": 1, "name": "a"}) + "\n")
        f.write(json.dumps({"id": 2, "name": "b"}) + "\n")

    lines = [
        _schema_line(stream),
        _batch_line(
            stream, [f"file://{batch_path}"], {"format": "jsonl", "compression": "gzip"}
        ),
    ]

    target_duckdb.persist_lines(connection, config, lines)

    instance.load_rows_from_json_files.assert_called_once_with(
        [str(batch_path)], compression="gzip"
    )
    flush_streams_mock.assert_not_called()
    instance.load_rows_from_arrow_files.assert_not_called()


@patch("target_duckdb.flush_streams")
@patch("target_duckdb.DbSync")
def test_persist_lines_unsupported_batch_encoding_raises(
    dbsync_mock,
    flush_streams_mock,
    config,
    connection,
):
    stream = "mydb-mytable"
    instance = dbsync_mock.return_value
    instance.create_schema_if_not_exists.return_value = None
    instance.sync_table.return_value = None

    lines = [
        _schema_line(stream),
        _batch_line(stream, ["file:///tmp/whatever.parquet"], {"format": "parquet"}),
    ]

    with pytest.raises(Exception, match="Unsupported BATCH encoding format"):
        target_duckdb.persist_lines(connection, config, lines)
