ruff:
	uv run ruff check target_duckdb/ tests/

unit_test:
	uv run pytest tests/unit -v

integration_test:
	uv run pytest tests/integration -v
