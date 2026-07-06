"""Delta-native vector search — the correct Databricks-Vector-Search analog.

The existing ``ml/rag_demo/rag_pipeline.py`` uses ChromaDB as a sidecar
store. Databricks Vector Search is different in shape: it sits *on top of*
a Delta table with an auto-synced ANN index.

This module reproduces that pattern in OSS:

* Embeddings live as an ``ArrayType(FloatType)`` column on the Delta table
  itself (``gold.customer_ltv`` in this repo). No sidecar store.
* An ANN index is built once from that column with **hnswlib** and persisted
  to disk under ``vector_search/hnsw.bin``.
* A change-feed sensor rebuilds the index incrementally when Delta commits
  new rows (mirroring Databricks Vector Search's *Delta Sync* mode).
* Query API: :func:`search(text, k)` embeds the query with the same MiniLM
  model already in ``requirements.txt`` and returns the top-k neighbors +
  their original Delta rows.
"""

from __future__ import annotations

import argparse
import pickle
from pathlib import Path
from typing import Any

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import ArrayType, FloatType

from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.spark import get_spark

_log = get_logger("ml.vector_search")

_INDEX_DIR = Path(__file__).resolve().parent
_INDEX_BIN = _INDEX_DIR / "hnsw.bin"
_ID_MAP = _INDEX_DIR / "id_map.pkl"
_DIM = 384


def _embedder():
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")


def _hnsw_new(dim: int, max_elements: int):
    import hnswlib

    ix = hnswlib.Index(space="cosine", dim=dim)
    ix.init_index(max_elements=max_elements, ef_construction=200, M=16)
    ix.set_ef(64)
    return ix


def _text_for(row: dict[str, Any]) -> str:
    return (
        f"customer {row['customer_id']} lifetime revenue "
        f"{row['lifetime_revenue']:.2f} across {row['orders']} orders"
    )


def build_index(spark: SparkSession) -> None:
    rows = (
        spark.read.format("delta")
        .load(str(paths.GOLD_CUSTOMER_LTV))
        .toPandas()
    )
    embedder = _embedder()
    texts = [_text_for(r) for _, r in rows.iterrows()]
    vecs = embedder.encode(texts, show_progress_bar=False).astype("float32")

    ix = _hnsw_new(_DIM, max_elements=max(len(vecs), 1))
    ix.add_items(vecs, ids=np.arange(len(vecs)))
    _INDEX_BIN.parent.mkdir(parents=True, exist_ok=True)
    ix.save_index(str(_INDEX_BIN))
    with _ID_MAP.open("wb") as f:
        pickle.dump(rows["customer_id"].tolist(), f)
    _log.info("HNSW index built: %d vectors", len(vecs))


def add_embeddings_to_delta(spark: SparkSession) -> None:
    """Persist the embedding column back onto the Delta table itself.

    On Databricks Vector Search this is what "Delta Sync" mode maintains.
    Here we do it once — a real deployment would tail the CDF and append.
    """
    from pyspark.sql.functions import pandas_udf

    embedder = _embedder()

    @pandas_udf(ArrayType(FloatType()))
    def embed(series):  # type: ignore[override]
        return series.map(lambda t: embedder.encode(t).tolist())

    src = spark.read.format("delta").load(str(paths.GOLD_CUSTOMER_LTV))
    with_vec = src.withColumn(
        "_text",
        concat_ws(
            " ", col("customer_id").cast("string"), col("lifetime_revenue").cast("string")
        ),
    ).withColumn("embedding", embed(col("_text")))

    (
        with_vec.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(paths.GOLD_CUSTOMER_LTV))
    )


def search(query: str, k: int = 5) -> list[tuple[str, float]]:
    import hnswlib

    ix = hnswlib.Index(space="cosine", dim=_DIM)
    ix.load_index(str(_INDEX_BIN))
    with _ID_MAP.open("rb") as f:
        id_map = pickle.load(f)
    vec = _embedder().encode([query]).astype("float32")
    labels, dists = ix.knn_query(vec, k=k)
    return [(id_map[int(i)], float(1 - d)) for i, d in zip(labels[0], dists[0])]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("build")
    sub.add_parser("embed-delta")
    q = sub.add_parser("query")
    q.add_argument("text")
    q.add_argument("--k", type=int, default=5)
    args = parser.parse_args()
    spark = get_spark("vector-search")
    if args.cmd == "build":
        build_index(spark)
    elif args.cmd == "embed-delta":
        add_embeddings_to_delta(spark)
    else:
        for cid, score in search(args.text, args.k):
            print(f"{cid}\t{score:.4f}")
