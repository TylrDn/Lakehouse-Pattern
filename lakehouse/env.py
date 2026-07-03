"""Environment preflight + shared logging.

Two responsibilities:

1. ``check_prerequisites()`` — fail *early* and with an actionable message when
   the local environment is missing something PySpark needs (Java 17+,
   ``JAVA_HOME`` sane). Without this the first thing users see is a 40-line
   Py4J stacktrace, which is a terrible onboarding experience.

2. ``get_logger()`` — return a module logger configured with a single-line
   format so pipeline output is grep-friendly and the same across every
   job (``[2026-07-03T14:12:00Z INFO silver-clean] ...``).

Both are safe to call multiple times and are cheap.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

_MIN_JAVA_MAJOR = 17
_LOG_FORMAT = "[%(asctime)s %(levelname)s %(name)s] %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger. Idempotent — safe to call from every module."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LAKEHOUSE_LOG_LEVEL", "INFO"))
        logger.propagate = False
    return logger


def _java_major_from_version(text: str) -> Optional[int]:
    """Extract the Java major version from `java -version` stderr.

    Handles both ``1.8.0_...`` (legacy) and ``17.0.7`` (modern).
    """
    for line in text.splitlines():
        # Look for the first quoted version token.
        if '"' not in line:
            continue
        token = line.split('"', 2)[1]
        parts = token.split(".")
        if parts[0] == "1" and len(parts) > 1:  # e.g. 1.8.0
            try:
                return int(parts[1])
            except ValueError:
                return None
        try:
            return int(parts[0])
        except ValueError:
            return None
    return None


def _detect_java_major() -> Optional[int]:
    """Return the installed Java major version, or None if Java is not on PATH."""
    java = shutil.which("java")
    if java is None:
        java_home = os.environ.get("JAVA_HOME")
        if java_home:
            candidate = Path(java_home) / "bin" / "java"
            if candidate.exists():
                java = str(candidate)
        if java is None:
            return None
    try:
        proc = subprocess.run(
            [java, "-version"], capture_output=True, text=True, timeout=5
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    # `java -version` writes to stderr.
    return _java_major_from_version(proc.stderr or proc.stdout)


class PrerequisiteError(RuntimeError):
    """Raised when the environment can't run PySpark + Delta."""


def check_prerequisites(min_java: int = _MIN_JAVA_MAJOR) -> None:
    """Fail fast with an actionable message if Java is missing or too old.

    Called from :func:`lakehouse.spark.get_spark` before we build a
    SparkSession, so users see::

        PrerequisiteError: Java 17+ is required for PySpark 3.5.
          Detected: Java 11
          Install: brew install openjdk@17  (macOS)
                   sudo apt install openjdk-17-jre  (Debian/Ubuntu)
          Then export JAVA_HOME=<the JDK 17 install path>.

    instead of a Py4J stacktrace.
    """
    major = _detect_java_major()
    if major is None:
        raise PrerequisiteError(
            "Java was not found on PATH and JAVA_HOME is unset.\n"
            "PySpark 3.5 requires a JDK 17+ runtime.\n"
            "  macOS:          brew install openjdk@17\n"
            "  Debian/Ubuntu:  sudo apt install openjdk-17-jre\n"
            "Then set JAVA_HOME to the JDK 17 install path."
        )
    if major < min_java:
        raise PrerequisiteError(
            f"Java {min_java}+ is required for PySpark 3.5.\n"
            f"  Detected: Java {major}\n"
            f"  Install:  brew install openjdk@17  (macOS)\n"
            f"            sudo apt install openjdk-17-jre  (Debian/Ubuntu)\n"
            f"Then export JAVA_HOME to the JDK 17 install path."
        )


if __name__ == "__main__":
    check_prerequisites()
    log = get_logger("env")
    log.info("Prerequisites OK.")
