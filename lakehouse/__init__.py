"""Shared helpers for the Lakehouse-Pattern demo.

This package holds cross-cutting utilities (Spark bootstrap, path constants,
schema definitions) that every layer imports. Keeping them in one place means
every job — batch, streaming, orchestration, ML — gets identical Delta / Spark
configuration and there is a single source of truth for table locations.
"""
