"""AI Functions analog — Databricks' ai_query, ai_classify, ai_extract as UDFs.

Databricks exposes a family of AI SQL functions:

    SELECT ai_query('databricks-meta-llama-3-1-70b-instruct',
                    'Summarize: ' || review) FROM reviews;

They are, mechanically, UDFs that call the workspace's Model Serving
endpoint. This module registers Spark SQL UDFs with the same names and
signatures so an existing SQL query ports directly. The transport is a
pluggable ``Backend`` — HTTPS to any OpenAI-compatible endpoint (Ollama,
vLLM, LM Studio, OpenAI, Together, …) using the secret store for the API
key.

Registered UDFs
---------------
* ``ai_query(model, prompt) → STRING``
* ``ai_classify(text, ARRAY<STRING> labels) → STRING``
* ``ai_extract(text, ARRAY<STRING> attributes) → MAP<STRING,STRING>``
* ``ai_similarity(a, b) → DOUBLE`` (cosine similarity via MiniLM)
"""

from __future__ import annotations

import json
import os
import urllib.request
from functools import lru_cache

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    DoubleType,
    MapType,
    StringType,
)

from governance.local_uc.secrets import get as get_secret
from lakehouse.env import get_logger

_log = get_logger("ml.ai_functions")


@lru_cache
def _api_key() -> str:
    try:
        return get_secret("ai", "openai_api_key")
    except KeyError:
        return os.environ.get("OPENAI_API_KEY", "")


def _chat(model: str, prompt: str) -> str:
    endpoint = os.environ.get("AI_ENDPOINT", "https://api.openai.com/v1/chat/completions")
    body = json.dumps(
        {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
        }
    ).encode()
    req = urllib.request.Request(
        endpoint,
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {_api_key()}",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
        payload = json.loads(resp.read())
    return payload["choices"][0]["message"]["content"]


@udf(StringType())
def ai_query(model: str, prompt: str) -> str | None:
    if not prompt:
        return None
    return _chat(model or "gpt-4o-mini", prompt)


@udf(StringType())
def ai_classify(text: str, labels: list[str]) -> str | None:
    if not text or not labels:
        return None
    p = (
        "Classify the following text into exactly one of these labels: "
        + ", ".join(labels)
        + f"\n\nText: {text}\n\nRespond with only the label."
    )
    return _chat("gpt-4o-mini", p).strip()


@udf(MapType(StringType(), StringType()))
def ai_extract(text: str, attributes: list[str]) -> dict[str, str] | None:
    if not text or not attributes:
        return None
    p = (
        "Extract the following attributes as JSON: "
        + json.dumps(attributes)
        + f"\n\nText: {text}\n\nRespond with a single JSON object."
    )
    raw = _chat("gpt-4o-mini", p)
    try:
        parsed = json.loads(raw)
        return {k: str(v) for k, v in parsed.items()}
    except json.JSONDecodeError:
        _log.warning("ai_extract non-JSON response: %s", raw[:120])
        return None


@lru_cache
def _embedder():
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


@udf(DoubleType())
def ai_similarity(a: str, b: str) -> float | None:
    if not a or not b:
        return None
    import numpy as np

    e = _embedder().encode([a, b])
    return float(np.dot(e[0], e[1]) / (np.linalg.norm(e[0]) * np.linalg.norm(e[1])))


def register_all(spark: SparkSession) -> None:
    spark.udf.register("ai_query", ai_query)
    spark.udf.register("ai_classify", ai_classify)
    spark.udf.register("ai_extract", ai_extract)
    spark.udf.register("ai_similarity", ai_similarity)
    _log.info("AI SQL functions registered: ai_query, ai_classify, ai_extract, ai_similarity")
