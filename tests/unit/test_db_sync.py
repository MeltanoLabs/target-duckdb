from __future__ import annotations

import gzip
import json
import os

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

import target_duckdb
from target_duckdb.db_sync import DbSync


def test_config_validation():
    """Test configuration validator"""
    validator = target_duckdb.validate_config
    empty_config = {}
    minimal_config = {
        "filepath": "dummy-value",
        "default_target_schema": "dummy-value",
    }

    # Empty configuration should fail
    assert len(validator(empty_config)) > 0

    # Minimal configuration should pass
    assert len(validator(minimal_config)) == 0

    # Configuration without schema references
    config_with_no_schema = minimal_config.copy()
    config_with_no_schema.pop("default_target_schema")
    assert len(validator(config_with_no_schema)) > 0

    # Configuration with schema mapping
    config_with_schema_mapping = minimal_config.copy()
    config_with_schema_mapping.pop("default_target_schema")
    config_with_schema_mapping["schema_mapping"] = {
        "dummy_stream": {"target_schema": "dummy_schema"}
    }
    assert len(validator(config_with_schema_mapping)) == 0


def test_column_type_mapping():
    """Test JSON type to DuckDB column type mappings"""
    mapper = target_duckdb.db_sync.column_type

    # Incoming JSON schema types
    json_str = {"type": ["string"]}
    json_str_or_null = {"type": ["string", "null"]}
    json_dt = {"type": ["string"], "format": "date-time"}
    json_dt_or_null = {"type": ["string", "null"], "format": "date-time"}
    json_t = {"type": ["string"], "format": "time"}
    json_d = {"type": ["string"], "format": "date"}
    json_t_or_null = {"type": ["string", "null"], "format": "time"}
    json_num = {"type": ["number"]}
    json_smallint = {"type": ["integer"], "maximum": 32767, "minimum": -32768}
    json_int = {"type": ["integer"], "maximum": 2147483647, "minimum": -2147483648}
    json_bigint = {
        "type": ["integer"],
        "maximum": 9223372036854775807,
        "minimum": -9223372036854775808,
    }
    json_nobound_int = {"type": ["integer"]}
    json_int_or_str = {"type": ["integer", "string"]}
    json_bool = {"type": ["boolean"]}
    json_obj = {"type": ["object"]}
    json_arr = {"type": ["array"]}

    # Mapping from JSON schema types to DuckDB column types
    assert mapper(json_str) == "varchar"
    assert mapper(json_str_or_null) == "varchar"
    assert mapper(json_dt) == "timestamp"
    assert mapper(json_dt_or_null) == "timestamp"
    assert mapper(json_t) == "time"
    assert mapper(json_d) == "date"
    assert mapper(json_t_or_null) == "time"
    assert mapper(json_num) == "double"
    assert mapper(json_smallint) == "smallint"
    assert mapper(json_int) == "integer"
    assert mapper(json_bigint) == "bigint"
    assert mapper(json_nobound_int) == "hugeint"
    assert mapper(json_int_or_str) == "varchar"
    assert mapper(json_bool) == "boolean"
    assert mapper(json_obj) == "json"
    assert mapper(json_arr) == "json"


def test_stream_name_to_dict():
    """Test identifying catalog, schema and table names from fully qualified stream and table names"""
    # Singer stream name format (Default '-' separator)
    assert target_duckdb.db_sync.stream_name_to_dict("my_table") == {
        "catalog_name": None,
        "schema_name": None,
        "table_name": "my_table",
    }

    assert target_duckdb.db_sync.stream_name_to_dict("my_schema-my_table") == {
        "catalog_name": None,
        "schema_name": "my_schema",
        "table_name": "my_table",
    }

    assert target_duckdb.db_sync.stream_name_to_dict(
        "my_catalog-my_schema-my_table"
    ) == {
        "catalog_name": "my_catalog",
        "schema_name": "my_schema",
        "table_name": "my_table",
    }

    # Redshift table format (Custom '.' separator)
    assert target_duckdb.db_sync.stream_name_to_dict("my_table", separator=".") == {
        "catalog_name": None,
        "schema_name": None,
        "table_name": "my_table",
    }

    assert target_duckdb.db_sync.stream_name_to_dict(
        "my_schema.my_table", separator="."
    ) == {
        "catalog_name": None,
        "schema_name": "my_schema",
        "table_name": "my_table",
    }

    assert target_duckdb.db_sync.stream_name_to_dict(
        "my_catalog.my_schema.my_table", separator="."
    ) == {
        "catalog_name": "my_catalog",
        "schema_name": "my_schema",
        "table_name": "my_table",
    }


def test_flatten_schema():
    """Test flattening of SCHEMA messages"""
    flatten_schema = target_duckdb.db_sync.flatten_schema

    # Schema with no object properties should be empty dict
    schema_with_no_properties = {"type": "object"}
    assert flatten_schema(schema_with_no_properties) == {}

    not_nested_schema = {
        "type": "object",
        "properties": {
            "c_pk": {"type": ["null", "integer"]},
            "c_varchar": {"type": ["null", "string"]},
            "c_int": {"type": ["null", "integer"]},
        },
    }
    # NO FLATTENNING - Schema with simple properties should be a plain dictionary
    assert flatten_schema(not_nested_schema) == not_nested_schema["properties"]

    nested_schema_with_no_properties = {
        "type": "object",
        "properties": {
            "c_pk": {"type": ["null", "integer"]},
            "c_varchar": {"type": ["null", "string"]},
            "c_int": {"type": ["null", "integer"]},
            "c_obj": {"type": ["null", "object"]},
        },
    }
    # NO FLATTENNING - Schema with object type property but without further properties should be a plain dictionary
    assert (
        flatten_schema(nested_schema_with_no_properties)
        == nested_schema_with_no_properties["properties"]
    )

    nested_schema_with_properties = {
        "type": "object",
        "properties": {
            "c_pk": {"type": ["null", "integer"]},
            "c_varchar": {"type": ["null", "string"]},
            "c_int": {"type": ["null", "integer"]},
            "c_obj": {
                "type": ["null", "object"],
                "properties": {
                    "nested_prop1": {"type": ["null", "string"]},
                    "nested_prop2": {"type": ["null", "string"]},
                    "nested_prop3": {
                        "type": ["null", "object"],
                        "properties": {
                            "multi_nested_prop1": {"type": ["null", "string"]},
                            "multi_nested_prop2": {"type": ["null", "string"]},
                        },
                    },
                },
            },
        },
    }
    # NO FLATTENNING - No flattening (default)
    assert (
        flatten_schema(nested_schema_with_properties)
        == nested_schema_with_properties["properties"]
    )

    # NO FLATTENNING - max_level: 0
    assert (
        flatten_schema(nested_schema_with_properties, max_level=0)
        == nested_schema_with_properties["properties"]
    )

    # FLATTENNING - max_level: 1
    assert flatten_schema(nested_schema_with_properties, max_level=1) == {
        "c_pk": {"type": ["null", "integer"]},
        "c_varchar": {"type": ["null", "string"]},
        "c_int": {"type": ["null", "integer"]},
        "c_obj__nested_prop1": {"type": ["null", "string"]},
        "c_obj__nested_prop2": {"type": ["null", "string"]},
        "c_obj__nested_prop3": {
            "type": ["null", "object"],
            "properties": {
                "multi_nested_prop1": {"type": ["null", "string"]},
                "multi_nested_prop2": {"type": ["null", "string"]},
            },
        },
    }

    # FLATTENNING - max_level: 10
    assert flatten_schema(nested_schema_with_properties, max_level=10) == {
        "c_pk": {"type": ["null", "integer"]},
        "c_varchar": {"type": ["null", "string"]},
        "c_int": {"type": ["null", "integer"]},
        "c_obj__nested_prop1": {"type": ["null", "string"]},
        "c_obj__nested_prop2": {"type": ["null", "string"]},
        "c_obj__nested_prop3__multi_nested_prop1": {"type": ["null", "string"]},
        "c_obj__nested_prop3__multi_nested_prop2": {"type": ["null", "string"]},
    }


def test_flatten_record():
    """Test flattening of RECORD messages"""
    flatten_record = target_duckdb.db_sync.flatten_record

    empty_record = {}
    # Empty record should be empty dict
    assert flatten_record(empty_record) == {}

    not_nested_record = {"c_pk": 1, "c_varchar": "1", "c_int": 1}
    # NO FLATTENNING - Record with simple properties should be a plain dictionary
    assert flatten_record(not_nested_record) == not_nested_record

    nested_record = {
        "c_pk": 1,
        "c_varchar": "1",
        "c_int": 1,
        "c_obj": {
            "nested_prop1": "value_1",
            "nested_prop2": "value_2",
            "nested_prop3": {
                "multi_nested_prop1": "multi_value_1",
                "multi_nested_prop2": "multi_value_2",
            },
        },
    }

    # NO FLATTENNING - No flattening (default)
    assert flatten_record(nested_record) == {
        "c_pk": 1,
        "c_varchar": "1",
        "c_int": 1,
        "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}',
    }

    # NO FLATTENNING - max_level: 0
    assert flatten_record(nested_record, max_level=0) == {
        "c_pk": 1,
        "c_varchar": "1",
        "c_int": 1,
        "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}',
    }

    # SEMI FLATTENNING - max_level: 1
    assert flatten_record(nested_record, max_level=1) == {
        "c_pk": 1,
        "c_varchar": "1",
        "c_int": 1,
        "c_obj__nested_prop1": "value_1",
        "c_obj__nested_prop2": "value_2",
        "c_obj__nested_prop3": '{"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}',
    }

    # FLATTENNING - max_level: 10
    assert flatten_record(nested_record, max_level=10) == {
        "c_pk": 1,
        "c_varchar": "1",
        "c_int": 1,
        "c_obj__nested_prop1": "value_1",
        "c_obj__nested_prop2": "value_2",
        "c_obj__nested_prop3__multi_nested_prop1": "multi_value_1",
        "c_obj__nested_prop3__multi_nested_prop2": "multi_value_2",
    }


def test_flatten_record_with_flatten_schema():
    flatten_record = target_duckdb.db_sync.flatten_record

    flatten_schema = {"id": {"type": ["object", "array", "null"]}}

    test_cases = [
        (True, {"id": 1, "data": "xyz"}, {"id": "1", "data": "xyz"}),
        (False, {"id": 1, "data": "xyz"}, {"id": 1, "data": "xyz"}),
    ]

    for should_use_flatten_schema, record, expected_output in test_cases:
        output = flatten_record(
            record, flatten_schema if should_use_flatten_schema else None
        )
        assert output == expected_output


def _write_arrow_ipc_file(tmp_path, rows, name="batch.arrow"):
    batch = pa.RecordBatch.from_pylist(rows)
    path = str(tmp_path / name)
    with ipc.new_file(path, batch.schema) as writer:
        writer.write_batch(batch)
    return path


def _schema_message(stream, key_properties=("id",)):
    return {
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


@pytest.fixture
def local_connection():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


class TestLoadRowsFromArrowFiles:
    """Exercises DbSync.load_rows_from_arrow_files against a real (local, in-memory)
    DuckDB connection and a real Arrow IPC file -- this needs network access the first
    time, to fetch DuckDB's `arrow` community extension (cached locally afterwards)."""

    def _make_db_sync(
        self, local_connection, stream="mydb-mytable", key_properties=("id",)
    ):
        config = {"default_target_schema": "public"}
        db_sync = DbSync(
            local_connection, config, _schema_message(stream, key_properties)
        )
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()
        return db_sync

    def test_loads_rows_from_a_single_arrow_file(self, tmp_path, local_connection):
        db_sync = self._make_db_sync(local_connection)
        path = _write_arrow_ipc_file(
            tmp_path, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        )

        db_sync.load_rows_from_arrow_files([path])

        rows = db_sync.query('SELECT * FROM "public"."mytable" ORDER BY id')
        assert [(r["id"], r["name"]) for r in rows] == [(1, "a"), (2, "b")]

    def test_loads_rows_from_multiple_arrow_files(self, tmp_path, local_connection):
        db_sync = self._make_db_sync(local_connection)
        path1 = _write_arrow_ipc_file(
            tmp_path, [{"id": 1, "name": "a"}], name="b1.arrow"
        )
        path2 = _write_arrow_ipc_file(
            tmp_path, [{"id": 2, "name": "b"}], name="b2.arrow"
        )

        db_sync.load_rows_from_arrow_files([path1, path2])

        rows = db_sync.query('SELECT * FROM "public"."mytable" ORDER BY id')
        assert [(r["id"], r["name"]) for r in rows] == [(1, "a"), (2, "b")]

    def test_upserts_on_primary_key(self, tmp_path, local_connection):
        db_sync = self._make_db_sync(local_connection)
        path1 = _write_arrow_ipc_file(
            tmp_path, [{"id": 1, "name": "original"}], name="b1.arrow"
        )
        db_sync.load_rows_from_arrow_files([path1])

        path2 = _write_arrow_ipc_file(
            tmp_path,
            [{"id": 1, "name": "updated"}, {"id": 2, "name": "new"}],
            name="b2.arrow",
        )
        db_sync.load_rows_from_arrow_files([path2])

        rows = db_sync.query('SELECT * FROM "public"."mytable" ORDER BY id')
        assert [(r["id"], r["name"]) for r in rows] == [(1, "updated"), (2, "new")]

    def test_extra_target_columns_absent_from_arrow_file_become_null(
        self, tmp_path, local_connection
    ):
        # simulates add_metadata_columns: the target table has _sdc_* columns the Arrow
        # file doesn't carry.
        schema_msg = _schema_message("mydb-withmeta")
        schema_msg["schema"]["properties"]["_sdc_extracted_at"] = {
            "type": ["null", "string"]
        }
        config = {"default_target_schema": "public"}
        db_sync = DbSync(local_connection, config, schema_msg)
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()

        path = _write_arrow_ipc_file(tmp_path, [{"id": 1, "name": "a"}])
        db_sync.load_rows_from_arrow_files([path])

        rows = db_sync.query('SELECT * FROM "public"."withmeta"')
        assert rows == [{"id": 1, "name": "a", "_sdc_extracted_at": None}]

    def test_deletes_processed_files_after_successful_load(
        self, tmp_path, local_connection
    ):
        db_sync = self._make_db_sync(local_connection)
        path1 = _write_arrow_ipc_file(
            tmp_path, [{"id": 1, "name": "a"}], name="b1.arrow"
        )
        path2 = _write_arrow_ipc_file(
            tmp_path, [{"id": 2, "name": "b"}], name="b2.arrow"
        )

        db_sync.load_rows_from_arrow_files([path1, path2])

        assert not os.path.exists(path1)
        assert not os.path.exists(path2)

    def test_does_not_delete_files_when_flattening_guard_raises(
        self, tmp_path, local_connection
    ):
        config = {"default_target_schema": "public", "data_flattening_max_level": 1}
        db_sync = DbSync(
            local_connection, config, _nested_schema_message("mydb-nested-keep")
        )
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()

        path = _write_arrow_ipc_file(tmp_path, [{"id": 1, "name": "a"}])
        with pytest.raises(Exception, match="data_flattening_max_level"):
            db_sync.load_rows_from_arrow_files([path])

        assert os.path.exists(path)


def _write_jsonl_file(tmp_path, rows, name="batch.jsonl", compress=False):
    opener = gzip.open if compress else open
    path = tmp_path / name
    with opener(path, "wt", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")
    return str(path)


class TestLoadRowsFromJsonFiles:
    """Exercises DbSync.load_rows_from_json_files against a real (local, in-memory)
    DuckDB connection and real newline-delimited JSON files, both plain and
    gzip-compressed."""

    def _make_db_sync(
        self, local_connection, stream="mydb-mytable", key_properties=("id",)
    ):
        config = {"default_target_schema": "public"}
        db_sync = DbSync(
            local_connection, config, _schema_message(stream, key_properties)
        )
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()
        return db_sync

    def test_loads_rows_from_a_plain_jsonl_file(self, tmp_path, local_connection):
        db_sync = self._make_db_sync(local_connection)
        path = _write_jsonl_file(
            tmp_path, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        )

        db_sync.load_rows_from_json_files([path])

        rows = db_sync.query('SELECT * FROM "public"."mytable" ORDER BY id')
        assert [(r["id"], r["name"]) for r in rows] == [(1, "a"), (2, "b")]

    def test_loads_rows_from_a_gzip_compressed_jsonl_file(
        self, tmp_path, local_connection
    ):
        db_sync = self._make_db_sync(local_connection)
        path = _write_jsonl_file(
            tmp_path,
            [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
            name="batch.jsonl.gz",
            compress=True,
        )

        db_sync.load_rows_from_json_files([path], compression="gzip")

        rows = db_sync.query('SELECT * FROM "public"."mytable" ORDER BY id')
        assert [(r["id"], r["name"]) for r in rows] == [(1, "a"), (2, "b")]

    def test_loads_rows_from_multiple_jsonl_files(self, tmp_path, local_connection):
        db_sync = self._make_db_sync(local_connection)
        path1 = _write_jsonl_file(tmp_path, [{"id": 1, "name": "a"}], name="b1.jsonl")
        path2 = _write_jsonl_file(tmp_path, [{"id": 2, "name": "b"}], name="b2.jsonl")

        db_sync.load_rows_from_json_files([path1, path2])

        rows = db_sync.query('SELECT * FROM "public"."mytable" ORDER BY id')
        assert [(r["id"], r["name"]) for r in rows] == [(1, "a"), (2, "b")]

    def test_upserts_on_primary_key(self, tmp_path, local_connection):
        db_sync = self._make_db_sync(local_connection)
        path1 = _write_jsonl_file(
            tmp_path, [{"id": 1, "name": "original"}], name="b1.jsonl"
        )
        db_sync.load_rows_from_json_files([path1])

        path2 = _write_jsonl_file(
            tmp_path,
            [{"id": 1, "name": "updated"}, {"id": 2, "name": "new"}],
            name="b2.jsonl",
        )
        db_sync.load_rows_from_json_files([path2])

        rows = db_sync.query('SELECT * FROM "public"."mytable" ORDER BY id')
        assert [(r["id"], r["name"]) for r in rows] == [(1, "updated"), (2, "new")]

    def test_extra_target_columns_absent_from_json_file_become_null(
        self, tmp_path, local_connection
    ):
        schema_msg = _schema_message("mydb-withmetajson")
        schema_msg["schema"]["properties"]["_sdc_extracted_at"] = {
            "type": ["null", "string"]
        }
        config = {"default_target_schema": "public"}
        db_sync = DbSync(local_connection, config, schema_msg)
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()

        path = _write_jsonl_file(tmp_path, [{"id": 1, "name": "a"}])
        db_sync.load_rows_from_json_files([path])

        rows = db_sync.query('SELECT * FROM "public"."withmetajson"')
        assert rows == [{"id": 1, "name": "a", "_sdc_extracted_at": None}]

    def test_nested_object_and_array_values_land_as_json(
        self, tmp_path, local_connection
    ):
        schema_msg = _schema_message("mydb-nested")
        schema_msg["schema"]["properties"]["meta"] = {"type": ["null", "object"]}
        schema_msg["schema"]["properties"]["tags"] = {"type": ["null", "array"]}
        config = {"default_target_schema": "public"}
        db_sync = DbSync(local_connection, config, schema_msg)
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()

        path = _write_jsonl_file(
            tmp_path, [{"id": 1, "meta": {"x": 1}, "tags": ["a", "b"]}]
        )
        db_sync.load_rows_from_json_files([path])

        rows = db_sync.query('SELECT * FROM "public"."nested"')
        assert rows == [{"id": 1, "meta": '{"x":1}', "name": None, "tags": '["a","b"]'}]

    def test_deletes_processed_files_after_successful_load(
        self, tmp_path, local_connection
    ):
        db_sync = self._make_db_sync(local_connection)
        path1 = _write_jsonl_file(tmp_path, [{"id": 1, "name": "a"}], name="b1.jsonl")
        path2 = _write_jsonl_file(tmp_path, [{"id": 2, "name": "b"}], name="b2.jsonl")

        db_sync.load_rows_from_json_files([path1, path2])

        assert not os.path.exists(path1)
        assert not os.path.exists(path2)

    def test_does_not_delete_files_when_flattening_guard_raises(
        self, tmp_path, local_connection
    ):
        config = {"default_target_schema": "public", "data_flattening_max_level": 1}
        db_sync = DbSync(
            local_connection, config, _nested_schema_message("mydb-nested-keep-json")
        )
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()

        path = _write_jsonl_file(tmp_path, [{"id": 1, "c_obj": {"nested_prop1": "x"}}])
        with pytest.raises(Exception, match="data_flattening_max_level"):
            db_sync.load_rows_from_json_files([path])

        assert os.path.exists(path)


def _nested_schema_message(stream, key_properties=("id",)):
    return {
        "stream": stream,
        "schema": {
            "type": "object",
            "properties": {
                "id": {"type": ["null", "integer"]},
                "c_obj": {
                    "type": ["null", "object"],
                    "properties": {
                        "nested_prop1": {"type": ["null", "string"]},
                    },
                },
            },
        },
        "key_properties": list(key_properties),
    }


class TestBatchFlatteningGuard:
    """data_flattening_max_level > 0 actually flattening a stream's schema is incompatible
    with the native BY-NAME BATCH loading paths (the source file has the original nested
    column, not the flattened name) -- this should raise a clear error rather than either
    silently dropping the nested value or surfacing DuckDB's own confusing BinderException.
    """

    def test_arrow_raises_when_flattening_applies(self, tmp_path, local_connection):
        config = {"default_target_schema": "public", "data_flattening_max_level": 1}
        db_sync = DbSync(
            local_connection, config, _nested_schema_message("mydb-nested-arrow")
        )
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()

        path = _write_arrow_ipc_file(tmp_path, [{"id": 1, "name": "a"}])
        with pytest.raises(Exception, match="data_flattening_max_level"):
            db_sync.load_rows_from_arrow_files([path])

    def test_json_raises_when_flattening_applies(self, tmp_path, local_connection):
        config = {"default_target_schema": "public", "data_flattening_max_level": 1}
        db_sync = DbSync(
            local_connection, config, _nested_schema_message("mydb-nested-json")
        )
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()

        path = _write_jsonl_file(tmp_path, [{"id": 1, "c_obj": {"nested_prop1": "x"}}])
        with pytest.raises(Exception, match="data_flattening_max_level"):
            db_sync.load_rows_from_json_files([path])

    def test_does_not_raise_when_max_level_is_zero_despite_nested_schema(
        self, tmp_path, local_connection
    ):
        # default max_level=0 means flatten_schema never actually renames anything, even
        # though the schema itself has a nested object -- should load fine as a JSON column.
        config = {"default_target_schema": "public"}
        db_sync = DbSync(
            local_connection, config, _nested_schema_message("mydb-unflattened")
        )
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()

        path = _write_jsonl_file(tmp_path, [{"id": 1, "c_obj": {"nested_prop1": "x"}}])
        db_sync.load_rows_from_json_files([path])  # should not raise

        rows = db_sync.query('SELECT * FROM "public"."unflattened"')
        assert rows == [{"id": 1, "c_obj": '{"nested_prop1":"x"}'}]

    def test_does_not_raise_when_max_level_set_but_schema_has_no_nested_properties(
        self, tmp_path, local_connection
    ):
        # data_flattening_max_level > 0 with a flat schema is harmless: flatten_schema
        # produces the same column names as the raw schema either way.
        config = {"default_target_schema": "public", "data_flattening_max_level": 1}
        db_sync = DbSync(local_connection, config, _schema_message("mydb-flatalready"))
        db_sync.create_schema_if_not_exists()
        db_sync.sync_table()

        path = _write_jsonl_file(tmp_path, [{"id": 1, "name": "a"}])
        db_sync.load_rows_from_json_files([path])  # should not raise

        rows = db_sync.query('SELECT * FROM "public"."flatalready"')
        assert rows == [{"id": 1, "name": "a"}]
