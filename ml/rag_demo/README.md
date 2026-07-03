# RAG Demo

A minimal, fully offline retrieval-augmented pipeline grounded in the Gold
`customer_ltv` mart. It exists to prove the pattern, not to be a production
RAG system.

## Run

```bash
make gold          # ensure Gold is populated
python -m ml.rag_demo.rag_pipeline --rebuild --question "Who is our best customer?"
```

## Stack

| Concern | OSS choice | Databricks-native swap |
| --- | --- | --- |
| Vector store | `chromadb` (local, persistent) | Databricks Vector Search + Delta Sync index |
| Embeddings | `sentence-transformers/all-MiniLM-L6-v2` | Foundation Model APIs / Model Serving endpoint |
| Generation | Template renderer | Model Serving endpoint (Llama-3, DBRX, etc.) |

The retrieval + grounding logic is identical across the two — only the
concrete providers change.
