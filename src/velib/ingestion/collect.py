"""Collect Vélib' Métropole availability snapshots (GBFS 1.0, no API key required).

Each run:
  1. fetches station_status.json (one snapshot, ~1,400 stations);
  2. appends it to the Parquet file of the current UTC day: station_status_YYYY-MM-DD.parquet;
  3. once per day, also stores station_information_YYYY-MM-DD.parquet (static attributes).

With --upload, files are stored in GitHub Releases (one release per month, tag
velib-YYYY-MM). This keeps the git history small and needs no extra secret.

All timestamps are UTC:
  fetched_at       when WE received the response
  feed_updated_at  freshness declared by the feed (root field), when present
  last_reported    last report sent by the station itself

Usage:
  python -m velib.ingestion.collect collect            # local run, useful for a first test
  python -m velib.ingestion.collect collect --upload   # inside GitHub Actions
  python -m velib.ingestion.collect sync               # download everything locally
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pandas as pd

log = logging.getLogger("velib")

# The feed has been served from two hosts over time; they are tried in order.
BASE_URLS = [
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole",
    "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole",
]
TS_DTYPE = "datetime64[us, UTC]"
MIN_EXPECTED_STATIONS = 100  # sanity threshold: the network has ~1,400 stations


# --------------------------------------------------------------------------- #
# Network
# --------------------------------------------------------------------------- #
def fetch_json(filename: str, retries: int = 3) -> dict:
    """Fetch a GBFS file, retrying with exponential backoff and falling back to the second host."""
    last_exc: Exception | None = None
    for base in BASE_URLS:
        for attempt in range(retries):
            try:
                req = urllib.request.Request(
                    f"{base}/{filename}", headers={"User-Agent": "velib-forecasting/0.1"})
                with urllib.request.urlopen(req, timeout=30) as response:
                    return json.load(response)
            except Exception as exc:  # network error, invalid JSON, 5xx...
                last_exc = exc
                log.warning("%s/%s attempt %d failed: %s", base, filename, attempt + 1, exc)
                time.sleep(2 ** attempt)
    raise RuntimeError(f"could not fetch {filename}") from last_exc


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #
def _epoch_to_utc(s: pd.Series) -> pd.Series:
    values = pd.to_numeric(s, errors="coerce")
    values = values.where(values > 0)  # 0 or negative means "never reported"
    return pd.to_datetime(values, unit="s", utc=True, errors="coerce").astype(TS_DTYPE)


def _to_bool(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int8").astype("boolean")


def parse_status(payload: dict, fetched_at: pd.Timestamp) -> pd.DataFrame:
    """Turn a station_status.json payload into a typed DataFrame.

    Raises ValueError when the structure looks different from what we expect, so that a
    format change fails loudly instead of silently producing empty or garbage files.
    """
    try:
        stations = payload["data"]["stations"]
    except (KeyError, TypeError) as exc:
        raise ValueError(f"unexpected structure (root keys: {list(payload)[:6]})") from exc

    rows = []
    for s in stations:
        bike_types: dict = {}
        for entry in s.get("num_bikes_available_types") or []:
            bike_types.update(entry)
        rows.append({
            "station_id": s.get("station_id"),
            "station_code": s.get("stationCode"),
            "bikes": s.get("num_bikes_available"),
            "mechanical": bike_types.get("mechanical"),
            "ebikes": bike_types.get("ebike"),
            "docks": s.get("num_docks_available"),
            "is_installed": s.get("is_installed"),
            "is_renting": s.get("is_renting"),
            "is_returning": s.get("is_returning"),
            "last_reported": s.get("last_reported"),
        })
    df = pd.DataFrame(rows)
    if df.empty or "station_id" not in df:
        raise ValueError("no station in the response")

    out = pd.DataFrame({
        "fetched_at": pd.Series(fetched_at, index=df.index).astype(TS_DTYPE),
        "feed_updated_at": _epoch_to_utc(pd.Series(payload.get("lastUpdatedOther"), index=df.index)),
        "station_id": pd.to_numeric(df["station_id"], errors="coerce"),
        "station_code": df["station_code"].astype("string"),
        "bikes": pd.to_numeric(df["bikes"], errors="coerce").round().astype("Int16"),
        "mechanical": pd.to_numeric(df["mechanical"], errors="coerce").round().astype("Int16"),
        "ebikes": pd.to_numeric(df["ebikes"], errors="coerce").round().astype("Int16"),
        "docks": pd.to_numeric(df["docks"], errors="coerce").round().astype("Int16"),
        "is_installed": _to_bool(df["is_installed"]),
        "is_renting": _to_bool(df["is_renting"]),
        "is_returning": _to_bool(df["is_returning"]),
        "last_reported": _epoch_to_utc(df["last_reported"]),
    })
    out = out.dropna(subset=["station_id"]).astype({"station_id": "int64"})

    # Guard rails: a changed format must not silently produce unusable files.
    if len(out) < MIN_EXPECTED_STATIONS:
        raise ValueError(f"only {len(out)} stations in the response: suspicious feed")
    if out["bikes"].isna().mean() > 0.5 or out["docks"].isna().mean() > 0.5:
        raise ValueError("more than 50% missing values in bikes/docks: did the format change?")
    return out


def parse_information(payload: dict, fetched_at: pd.Timestamp) -> pd.DataFrame:
    """Turn a station_information.json payload into a DataFrame of static station attributes."""
    try:
        stations = payload["data"]["stations"]
    except (KeyError, TypeError) as exc:
        raise ValueError("station_information: unexpected structure") from exc
    df = pd.DataFrame(stations)
    wanted = {"station_id": "station_id", "stationCode": "station_code", "name": "name",
              "lat": "lat", "lon": "lon", "capacity": "capacity"}
    df = df[[c for c in wanted if c in df.columns]].rename(columns=wanted)
    df["station_id"] = pd.to_numeric(df["station_id"], errors="coerce")
    df = df.dropna(subset=["station_id"]).astype({"station_id": "int64"})
    df["fetched_at"] = pd.Series(fetched_at, index=df.index).astype(TS_DTYPE)
    return df


# --------------------------------------------------------------------------- #
# Storage: local files + GitHub Releases (through the gh CLI)
# --------------------------------------------------------------------------- #
def append_snapshot(path: Path, new: pd.DataFrame) -> int:
    """Append `new` to the day's Parquet file (deduplicated). Returns the resulting row count."""
    if path.exists():
        new = pd.concat([pd.read_parquet(path), new], ignore_index=True)
    new = (new.drop_duplicates(["station_id", "fetched_at"])
              .sort_values(["station_id", "fetched_at"], ignore_index=True))
    tmp = path.with_suffix(".tmp")
    new.to_parquet(tmp, index=False, compression="zstd")
    tmp.replace(path)
    return len(new)


def _gh(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=check)


def ensure_release(tag: str) -> None:
    if _gh("release", "view", tag, check=False).returncode != 0:
        _gh("release", "create", tag,
            "--title", f"Vélib' snapshots {tag.removeprefix('velib-')}",
            "--notes", "GBFS station_status snapshots, UTC. One Parquet file per day.",
            check=False)  # losing a race against a concurrent run is not an error


def release_assets(tag: str) -> set[str]:
    result = _gh("release", "view", tag, "--json", "assets", "-q", ".assets[].name")
    return set(result.stdout.split())


def release_download(tag: str, name: str, dest: Path) -> bool:
    result = _gh("release", "download", tag, "--pattern", name, "--dir", str(dest),
                 "--clobber", check=False)
    return result.returncode == 0 and (dest / name).exists()


def release_upload(tag: str, path: Path, retries: int = 3) -> None:
    for attempt in range(retries):
        result = _gh("release", "upload", tag, str(path), "--clobber", check=False)
        if result.returncode == 0:
            return
        log.warning("upload attempt %d failed: %s", attempt + 1, result.stderr.strip())
        time.sleep(2 ** attempt)
    raise RuntimeError(f"could not upload {path.name}")


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #
def collect_snapshot(out_dir: Path, upload: bool) -> None:
    """Fetch one snapshot and append it to the current day's file."""
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = fetch_json("station_status.json")
    fetched_at = pd.Timestamp.now(tz="UTC")
    snapshot = parse_status(payload, fetched_at)

    age = (snapshot["fetched_at"] - snapshot["feed_updated_at"]).dt.total_seconds().iloc[0]
    log.info("%d stations | median bikes %s | feed age: %s s",
             len(snapshot), snapshot["bikes"].median(), "?" if pd.isna(age) else int(age))

    day, tag = f"{fetched_at:%Y-%m-%d}", f"velib-{fetched_at:%Y-%m}"
    name = f"station_status_{day}.parquet"
    path = out_dir / name
    info_name = f"station_information_{day}.parquet"
    assets: set[str] = set()

    if upload:
        ensure_release(tag)
        assets = release_assets(tag)
        path.unlink(missing_ok=True)
        if name in assets and not release_download(tag, name, out_dir):
            # Never overwrite a remote file we could not read back: the day would be lost.
            raise RuntimeError(f"{name} exists in the release but could not be downloaded")

    rows = append_snapshot(path, snapshot)
    log.info("%s: %d rows", name, rows)
    if upload:
        release_upload(tag, path)

    info_path = out_dir / info_name
    if (upload and info_name not in assets) or (not upload and not info_path.exists()):
        info = parse_information(fetch_json("station_information.json"), fetched_at)
        info.to_parquet(info_path, index=False, compression="zstd")
        if upload:
            release_upload(tag, info_path)
        log.info("%s: %d stations", info_name, len(info))


def sync_releases(out_dir: Path) -> None:
    """Download every file of every velib-* release (requires an authenticated gh CLI)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    tags = _gh("release", "list", "--limit", "200", "--json", "tagName",
               "-q", ".[].tagName").stdout.split()
    for tag in sorted(t for t in tags if t.startswith("velib-")):
        _gh("release", "download", tag, "--pattern", "*.parquet", "--dir", str(out_dir), "--clobber")
        log.info("synced %s", tag)


def read_status(directory: Path) -> pd.DataFrame:
    """Load all daily station_status files of a directory into a single DataFrame."""
    files = sorted(directory.glob("station_status_*.parquet"))
    if not files:
        raise FileNotFoundError(f"no station_status_*.parquet file in {directory}")
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["collect", "sync"])
    parser.add_argument("--out", type=Path, default=Path("data/raw/velib"),
                        help="local output directory (default: data/raw/velib)")
    parser.add_argument("--upload", action="store_true",
                        help="store files in GitHub Releases instead of only locally")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.command == "collect":
            collect_snapshot(args.out, args.upload)
        else:
            sync_releases(args.out)
    except Exception as exc:
        log.error("%s", exc)
        sys.exit(1)  # red run in GitHub Actions: failures must be visible


if __name__ == "__main__":
    main()
