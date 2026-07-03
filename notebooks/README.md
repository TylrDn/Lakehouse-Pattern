# Notebooks

Scrollable, narrative-view companions to the CLI pipeline. Every notebook
uses the same Python modules that `make dag` invokes — nothing here is
mocked or reimplemented, so what you see is exactly what CI runs.

## Running

From the repo root:

```bash
python -m venv .venv && source .venv/bin/activate
make setup
pip install jupyterlab   # only needed once, and only for notebooks
jupyter lab
```

Then open a notebook from this folder. The notebooks add the repo root
to `sys.path` in their first cell, so `from lakehouse import …` works
regardless of where Jupyter was launched.

## Notebooks

| Notebook | What it shows |
| --- | --- |
| [`01_lakehouse_walkthrough.ipynb`](01_lakehouse_walkthrough.ipynb) | Bronze → Silver → Gold end-to-end with live table outputs, time-travel demo, and an optional MLflow training cell |

## Why the notebooks aren't executed in CI

Notebook execution requires a live SparkSession and downloads the
delta-spark JARs on first run. CI runs the same code paths via `pytest`
and `make preflight`, which is faster and more diff-friendly than
diffing executed notebook JSON.

If you want to *view* executed outputs without running the notebook,
they render inline on GitHub after you commit an executed copy (or
convert with `jupyter nbconvert --to markdown`).
