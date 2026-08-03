"""The path-tagged logger every actor is handed."""

import logging

import pytest

from tapio.actor import ActorPath
from tapio.logging import actor_logger, runtime_logger


@pytest.fixture
def logger_path() -> ActorPath:
    return ActorPath.root("sys").child("user").child("worker", uid=3)


def test_the_path_is_both_a_prefix_and_a_field(
    logger_path: ActorPath, caplog: pytest.LogCaptureFixture
):
    log = actor_logger(logger_path)

    with caplog.at_level(logging.INFO, logger="tapio.actor"):
        log.info("started with %d workers", 4)

    record = caplog.records[-1]
    assert record.getMessage() == f"{logger_path}: started with 4 workers"
    assert record.actor_path == str(logger_path)


def test_a_caller_keeps_its_own_structured_fields(
    logger_path: ActorPath, caplog: pytest.LogCaptureFixture
):
    # LoggerAdapter replaces `extra` wholesale by default, which would drop
    # whatever the caller attached.
    log = actor_logger(logger_path)

    with caplog.at_level(logging.INFO, logger="tapio.actor"):
        log.info("handled", extra={"request_id": "abc"})

    record = caplog.records[-1]
    assert record.request_id == "abc"
    assert record.actor_path == str(logger_path)


def test_runtime_records_live_under_the_same_root():
    assert runtime_logger("runtime").name == "tapio.runtime"
    assert actor_logger(ActorPath.root("sys")).logger.name == "tapio.actor"
