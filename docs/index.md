# Lakehouse-Pattern

> End-to-end Delta Lake reference implementation — Bronze/Silver/Gold,
> streaming ingest, declarative expectations, MLflow tracking + registry,
> OSS RAG, and a Streamlit gold explorer.

[![ci](https://github.com/TylrDn/Lakehouse-Pattern/actions/workflows/ci.yml/badge.svg)](https://github.com/TylrDn/Lakehouse-Pattern/actions/workflows/ci.yml)
[![release](https://img.shields.io/github/v/release/TylrDn/Lakehouse-Pattern?display_name=tag&sort=semver)](https://github.com/TylrDn/Lakehouse-Pattern/releases)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/LICENSE)

<p align="center">
  <img src="images/streamlit-gold-explorer.png" alt="Streamlit gold-layer explorer" width="820"/>
</p>

## What this repo is

A reproducible Bronze → Silver → Gold pipeline that ends in an interactive
Streamlit dashboard, a registered MLflow model, and a working RAG assistant
— all runnable locally with a single `make dag` after `make preflight`.

Every Databricks concept in the [concept coverage matrix](concept-coverage.md)
is either implemented end-to-end, or has a runnable OSS analog with a
documented mapping to the Databricks feature it stands in for.

## Where to start

- **[Architecture](architecture.md)** — the medallion narrative, tradeoffs,
  and design decisions.
- **[Data lineage](lineage.md)** — table-level Mermaid diagram plus
  column-level provenance for the gold marts.
- **[Concept coverage](concept-coverage.md)** — every Databricks concept
  demonstrated by this repo, with links to the exact file that implements it.
- **[Troubleshooting](troubleshooting.md)** — the top-10 setup pitfalls.
- **[Contributing](contributing.md)** — how to submit PRs.
- **[Changelog](changelog.md)** — release history.

## Quickstart

```bash
git clone https://github.com/TylrDn/Lakehouse-Pattern.git
cd Lakehouse-Pattern
python -m venv .venv && source .venv/bin/activate
make setup
make preflight   # verify Java 17+ is available
make dag         # runs the full Bronze→Silver→Gold + ML pipeline
make serve       # http://localhost:8501
```

Java 17 is required for PySpark 3.5. `make preflight` will tell you if
anything is missing.
