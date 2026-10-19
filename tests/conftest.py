from __future__ import annotations

import duckdb
import pytest


def pytest_report_header(config: pytest.Config) -> str:
    return f"duckdb: {duckdb.__version__}"
