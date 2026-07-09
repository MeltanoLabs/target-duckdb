from __future__ import annotations

import json
import logging

from target_duckdb.metrics import emit_metric


def test_emit_metric_logs_a_metric_line_at_info_level(caplog):
    logger = logging.getLogger("test-emit-metric")

    with caplog.at_level(logging.INFO, logger="test-emit-metric"):
        emit_metric(logger, "counter", "record_count", 42, {"stream": "mydb-mytable"})

    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.INFO
    assert record.getMessage().startswith("METRIC: ")

    payload = json.loads(record.getMessage().removeprefix("METRIC: "))
    assert payload == {
        "type": "counter",
        "metric": "record_count",
        "value": 42,
        "tags": {"stream": "mydb-mytable"},
    }


def test_emit_metric_defaults_tags_to_empty_dict(caplog):
    logger = logging.getLogger("test-emit-metric-2")

    with caplog.at_level(logging.INFO, logger="test-emit-metric-2"):
        emit_metric(logger, "timer", "job_duration", 1.5)

    payload = json.loads(caplog.records[0].getMessage().removeprefix("METRIC: "))
    assert payload["tags"] == {}
