"""Small OSS RAG demo grounded in Gold-layer facts.

We show the RAG *pattern* end-to-end without pulling in a hosted LLM:

1. Convert Gold rows (per-customer LTV summaries) into "documents".
2. Embed them locally with ``sentence-transformers``.
3. Store vectors in ``chromadb`` (persistent local vector DB).
4. Retrieve the top-k documents for a natural-language question.
5. Render an answer with a template — ``no LLM required``. When plugging in
   an actual LLM (OpenAI, Anthropic, Databricks Model Serving, etc.), only
   the ``compose_answer`` function needs to change.

Databricks-native equivalent
----------------------------
The exact same code runs on Databricks. For production you would swap:
* ``chromadb`` -> **Databricks Vector Search** (managed, syncs from a Delta
  table via a Delta Vector Sync index).
* Local sentence-transformer -> **Databricks Foundation Model APIs** or a
  workspace-hosted embedding endpoint.
* ``compose_answer`` -> **Model Serving** endpoint for the generation model.

The pattern (Delta -> embed -> index -> retrieve -> ground) is identical.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import chromadb
from sentence_transformers import SentenceTransformer

from lakehouse.paths import GOLD_CUSTOMER_LTV
from lakehouse.spark import get_spark

_STORE_DIR = Path(__file__).parent / "chroma_store"
_COLLECTION = "customer_ltv"
_EMBED_MODEL = "all-MiniLM-L6-v2"  # small, fast, CPU-friendly


def _documents_from_gold() -> list[dict]:
    """Materialize Gold rows as short natural-language documents."""
    spark = get_spark("rag-index-build")
    pdf = spark.read.format("delta").load(str(GOLD_CUSTOMER_LTV)).toPandas()
    docs: list[dict] = []
    for _, row in pdf.iterrows():
        text = (
            f"Customer {row['customer_id']} has placed {int(row['orders'])} orders "
            f"totaling {row['lifetime_revenue']:.2f} in revenue. "
            f"First seen {row['first_seen']}; last seen {row['last_seen']}."
        )
        docs.append(
            {
                "id": str(row["customer_id"]),
                "text": text,
                "metadata": {
                    "customer_id": str(row["customer_id"]),
                    "orders": int(row["orders"]),
                    "lifetime_revenue": float(row["lifetime_revenue"]),
                },
            }
        )
    return docs


def build_index() -> None:
    """(Re)build the local Chroma index from Gold."""
    _STORE_DIR.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(_STORE_DIR))
    # Reset the collection each build so demos are deterministic.
    try:
        client.delete_collection(_COLLECTION)
    except Exception:
        pass
    coll = client.create_collection(_COLLECTION)

    model = SentenceTransformer(_EMBED_MODEL)
    docs = _documents_from_gold()
    if not docs:
        raise RuntimeError("No Gold customer_ltv rows found — run the pipeline first.")

    texts = [d["text"] for d in docs]
    embeddings = model.encode(texts, show_progress_bar=False).tolist()
    coll.add(
        ids=[d["id"] for d in docs],
        documents=texts,
        metadatas=[d["metadata"] for d in docs],
        embeddings=embeddings,
    )
    print(f"Indexed {len(docs)} customer documents into {_STORE_DIR}")


def retrieve(question: str, k: int = 3) -> list[dict]:
    """Retrieve top-k grounding docs for a question."""
    client = chromadb.PersistentClient(path=str(_STORE_DIR))
    coll = client.get_collection(_COLLECTION)
    model = SentenceTransformer(_EMBED_MODEL)
    q_emb = model.encode([question]).tolist()
    result = coll.query(query_embeddings=q_emb, n_results=k)
    hits = []
    for i in range(len(result["ids"][0])):
        hits.append(
            {
                "id": result["ids"][0][i],
                "text": result["documents"][0][i],
                "metadata": result["metadatas"][0][i],
                "distance": result["distances"][0][i],
            }
        )
    return hits


def compose_answer(question: str, hits: list[dict]) -> str:
    """Deterministic template renderer (swap for an LLM call in production)."""
    if not hits:
        return "I couldn't find any matching customers in Gold."
    lines = [f"Question: {question}", "", "Grounding context:"]
    for h in hits:
        lines.append(f"  - {h['text']}  (distance={h['distance']:.3f})")
    top = hits[0]
    lines += [
        "",
        f"Answer (grounded): customer {top['metadata']['customer_id']} is the closest "
        f"match with {top['metadata']['orders']} orders and "
        f"{top['metadata']['lifetime_revenue']:.2f} lifetime revenue.",
    ]
    return "\n".join(lines)


def ask(question: str, k: int = 3) -> str:
    hits = retrieve(question, k=k)
    return compose_answer(question, hits)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rebuild", action="store_true", help="Rebuild the vector index")
    parser.add_argument(
        "--question",
        default="Who is our most valuable customer?",
        help="Natural-language question to answer.",
    )
    args = parser.parse_args()
    if args.rebuild or not _STORE_DIR.exists():
        build_index()
    print(ask(args.question))


if __name__ == "__main__":
    main()
