"""Emit Singer-style METRIC log lines for consumption by log-scraping benchmark harnesses.

This target isn't built on singer-sdk (see __init__.py's module docstring for why), so it has
none of the SDK's built-in `Metrics` machinery. This is a minimal, standalone equivalent: a
single `METRIC: {...}` line per call, in the same `{"type", "metric", "value", "tags"}` shape
singer-sdk itself emits, so downstream consumers (e.g. a harness parsing plugin logs) don't need
target-duckdb-specific handling.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import logging


def emit_metric(
    logger: logging.Logger,
    metric_type: str,
    metric_name: str,
    value: float,
    tags: dict[str, Any] | None = None,
) -> None:
    """Log a `METRIC: {...}` line at INFO level.

    Args:
        logger: A stdlib Logger (see logger.get_logger).
        metric_type: "counter" or "timer" (matches singer-sdk's Metric.type values).
        metric_name: The metric's name, e.g. "record_count".
        value: The metric's value.
        tags: Extra context to attach, e.g. {"stream": stream}.
    """
    logger.info(
        "METRIC: %s",
        json.dumps(
            {
                "type": metric_type,
                "metric": metric_name,
                "value": value,
                "tags": tags or {},
            }
        ),
    )
