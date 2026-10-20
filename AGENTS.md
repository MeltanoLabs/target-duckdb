# AGENTS.md

`target-duckdb` is a Singer target that loads data into DuckDB / MotherDuck.

## Layout

- `target_duckdb/__init__.py`: CLI entry point and Singer message handling (SCHEMA, RECORD, BATCH, STATE).
- `target_duckdb/db_sync.py`: SQL generation, schema/column mapping and loading (`DbSync`).
- `tests/unit`: fast tests using in-memory DuckDB.
- `tests/integration`: need a MotherDuck token (`motherduck_token`) and network access.

## Commands

Uses [uv](https://docs.astral.sh/uv/); Python >= 3.10.

```shell
uv run pytest tests/unit          # unit tests
uv run pytest tests/integration   # integration tests (MotherDuck)
uv run ruff check target_duckdb tests
uv run ruff format target_duckdb tests
uv run mypy target_duckdb tests
```

## Conventions

- Run lint, format and unit tests before finishing a change.
- Match the surrounding code style; keep comments sparse and explain why, not what.
- Add a test for behavior changes and an entry under `Unreleased` in `CHANGELOG.md`.
- Update `README.md` when settings change.
- Use conventional commit prefixes (`feat:`, `fix:`, `refactor:`, `test:`, `chore:`, `ci:`).
- Don't commit unless asked.
- Pull requests require an LLM use disclosure (see `.github/PULL_REQUEST_TEMPLATE.md`). Fill it in honestly, naming the tool and how it was used.
