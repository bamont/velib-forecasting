# Vélib' forecasting

Data collection and (soon) forecasting of bike availability at the ~1,400 stations of the
Vélib' Métropole network in Paris.

There is no public history of Vélib' availability: the official open data only exposes the
current state, refreshed every minute. This repository builds its own archive by taking a
snapshot every 5 minutes with a GitHub Actions workflow.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

pytest                                # offline tests, simulated payloads
python -m velib.ingestion.collect collect
```

The last command fetches one live snapshot and writes it to `data/raw/velib/`. **Run it once
locally before enabling the workflow**: the parser was written from the GBFS 1.0 specification
and has only been tested on simulated payloads. If the feed format differs, the command stops
with an explicit error instead of writing bad data.

## Enabling the automated collection

1. Push this repository to GitHub. **It must be public**: a private repository would exceed the
   free 2,000 Actions minutes per month (288 runs per day).
2. Open the *Actions* tab and run the `collect-velib` workflow manually (*Run workflow*).
3. Check that a release named `velib-YYYY-MM` appears with a `station_status_YYYY-MM-DD.parquet`
   file. From then on, the workflow runs every 5 minutes.

## Where the data lives

Files are published as **GitHub Release assets** rather than committed, so the git history stays small.

| Release tag | Asset | Content |
|---|---|---|
| `velib-YYYY-MM` | `station_status_YYYY-MM-DD.parquet` | all snapshots of the UTC day |
| `velib-YYYY-MM` | `station_information_YYYY-MM-DD.parquet` | static station attributes (name, coordinates, capacity) |

Expect roughly 2 MB per day (under 1 GB per year, estimated on simulated data).

To download everything locally (requires the [GitHub CLI](https://cli.github.com/), authenticated):

```bash
python -m velib.ingestion.collect sync          # writes to data/raw/velib/
```

```python
from pathlib import Path
from velib.ingestion.collect import read_status

df = read_status(Path("data/raw/velib"))
```

## Schema of `station_status`

All timestamps are **UTC**.

| Column | Type | Description |
|---|---|---|
| `fetched_at` | timestamp | when the snapshot was received by the collector |
| `feed_updated_at` | timestamp | freshness declared by the feed (null if absent) |
| `station_id` | int64 | station identifier |
| `station_code` | string | public station code |
| `bikes` | Int16 | bikes available (all types) |
| `mechanical`, `ebikes` | Int16 | breakdown by bike type (null if absent) |
| `docks` | Int16 | free docks |
| `is_installed`, `is_renting`, `is_returning` | boolean | station state flags |
| `last_reported` | timestamp | last report sent by the station (null if it never reported) |

## Things to know

- **Scheduled runs are often delayed** by GitHub, sometimes by several minutes. The sampling
  grid is therefore irregular: use `fetched_at` and resample onto a regular grid with a missing-value mask.
- **GitHub disables scheduled workflows after 60 days without repository activity** on public
  repositories. Publishing a release does not count as activity: check the *Actions* tab now and
  then, or push a small commit.
- **Failure behavior**: the collector exits with an error (red run) if the feed format changes or
  if a remote daily file cannot be read back, rather than overwriting it.
- **Seasonality**: data collected from autumn onward will not cover a full year for a long time.
  State this limitation when reporting forecasting results.

## Project layout

```
velib-forecasting/
├── .github/workflows/collect.yml      # scheduled collection (every 5 minutes)
├── src/velib/ingestion/collect.py     # fetch, parse, store
├── tests/test_collect.py              # offline tests
├── pyproject.toml                     # package metadata and pytest configuration
├── requirements-collect.txt           # minimal dependencies for the workflow
└── README.md
```

## Data source and license

Data: Vélib' Métropole / Smovengo, GBFS open data feed. Check the license on the dataset page of
[data.gouv.fr](https://www.data.gouv.fr/datasets/velib-velos-et-bornes-disponibilite-temps-reel)
before redistributing the collected files, and credit the source.
