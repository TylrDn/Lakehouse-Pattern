# Contributing

Contributions welcome. This is a portfolio / reference project — the goal is
readability + defensible design choices, not feature velocity.

## Local setup

```bash
git clone https://github.com/TylrDn/Lakehouse-Pattern.git
cd Lakehouse-Pattern
python -m venv .venv && source .venv/bin/activate
make setup
make pipeline    # download data + run bronze -> silver -> gold
make test
```

## Ground rules

- Every PR must keep CI green (`make ci`).
- New PySpark code needs a unit test that runs against a tiny in-memory
  DataFrame. Do not gate CI on downloading real data.
- Prefer explicit schemas over `inferSchema` at every layer.
- Every non-trivial design choice belongs in a comment or in
  `docs/architecture.md` — reviewers should never have to guess *why*.
- Never commit generated data. `data/bronze/`, `data/silver/`, `data/gold/`,
  `mlruns/`, and `chroma_store/` are all gitignored.

## Commit style

- Small, logical commits. One concern per commit.
- Subject line: imperative mood, ≤ 72 chars.
- Body: what + why, not how (the diff shows how).

Example: `feat(silver): add MERGE upsert for idempotent re-ingest`
