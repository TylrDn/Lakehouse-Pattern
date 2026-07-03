"""Unit tests for the environment preflight and logger."""

from __future__ import annotations

import logging

import pytest

from lakehouse import env


def test_logger_is_idempotent():
    log1 = env.get_logger("test-idempotent")
    log2 = env.get_logger("test-idempotent")
    assert log1 is log2
    # Only ever one handler even after many calls.
    for _ in range(5):
        env.get_logger("test-idempotent")
    assert len(log1.handlers) == 1


def test_logger_emits(capsys):
    # get_logger sets propagate=False and installs its own StreamHandler on
    # sys.stdout, so we assert against captured stdout rather than caplog
    # (which relies on record propagation to the root logger).
    log = env.get_logger("test-emits")
    log.setLevel(logging.INFO)
    log.info("hello %d", 42)
    for h in log.handlers:
        h.flush()
    captured = capsys.readouterr()
    assert "hello 42" in captured.out


@pytest.mark.parametrize(
    "text,expected",
    [
        ('openjdk version "17.0.7" 2023-04-18\n', 17),
        ('java version "1.8.0_402"\n', 8),
        ('openjdk version "21.0.1" 2023-10-17\n', 21),
        ("no version here", None),
    ],
)
def test_java_major_parser(text, expected):
    assert env._java_major_from_version(text) == expected


def test_check_prerequisites_passes_on_current_env():
    # If pytest can run at all here, Java 17+ is set up.
    env.check_prerequisites()


def test_check_prerequisites_raises_when_too_old(monkeypatch):
    monkeypatch.setattr(env, "_detect_java_major", lambda: 11)
    with pytest.raises(env.PrerequisiteError, match="Java 17"):
        env.check_prerequisites()


def test_check_prerequisites_raises_when_absent(monkeypatch):
    monkeypatch.setattr(env, "_detect_java_major", lambda: None)
    with pytest.raises(env.PrerequisiteError, match="not found"):
        env.check_prerequisites()
