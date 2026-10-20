Unreleased
----------

- RECORD messages are now loaded through temporary JSONL files and DuckDB's native `read_json`, instead of CSV files. This fixes type and quoting problems and adds no new dependency
- Nested objects/arrays are written as native JSON values, so they are no longer double-encoded in `JSON` columns
- Removes the `delimiter` and `quotechar` settings, which only applied to CSV temp files
- Tests run against DuckDB 1.5.5 for MotherDuck integration tests

0.11.1 (2026-07-28)
-------------------

- Raises custom exceptions instead of generic ones
- Pins DuckDB to a version supported by MotherDuck in tests

0.11.0 (2026-07-09)
-------------------

- Emits `record_count` metrics

0.10.0 (2026-07-02)
-------------------

- Adds support for Singer `BATCH` messages in `jsonl` and `arrow` formats, loaded directly with DuckDB (`read_json`/`read_arrow`)

0.9.0 (2026-02-16)
------------------

- Fixes the first record of each batch being silently dropped due to CSV header auto-detection
- Migrates packaging to `uv` and uses dynamic versioning

0.8.1 (2025-02-16)
------------------

- Add `date` column type

0.3.0 (2022-04-11)
------------------

- Targets DuckDB 0.3.3
- Supports JSON types for arrays/objects
- Simplifies the project's dependencies

0.2.0 (2022-04-08)
------------------

- Switches to INSERT based loading instead of using COPY CSV operations

0.1.0 (2022-04-07)
------------------

- First basically working version
- Targets DuckDB 0.3.2
- Known limitations:
    - object/array types are treated as varchars
    - primary keys on target tables are disabled
