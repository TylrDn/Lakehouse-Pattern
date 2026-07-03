# Sample Data

`download.py` regenerates a small, deterministic retail-transactions dataset.
Run it via `make data` or `python -m data.sample_raw.download`.

## Why synthetic?

* Reproducible in CI without a network dependency.
* Lets us inject exactly the data-quality problems (duplicates, negative
  quantities, malformed timestamps, whitespace, nulls) that the Silver layer
  is supposed to clean — so the medallion architecture's value is *visible*.

## Swapping in a real dataset

Two production-realistic options are documented in `download.py`:

1. UCI **Online Retail II** — real ~1M-row retail transactions.
2. NYC **TLC Taxi Trips** — larger, streams well via Auto Loader.

To swap: implement a downloader that writes CSV/JSON into `data/sample_raw/`
matching the Bronze schema in `lakehouse/schemas.py`. Nothing downstream
needs to change.

Nothing in this directory is committed except this README, `download.py`, and
a `.gitkeep`; generated data is `.gitignore`d.
