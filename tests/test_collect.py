"""Tests for the Vélib' collector, using simulated GBFS payloads.

CAVEAT: the simulated format below comes from the GBFS 1.0 specification and from what is known
of the Vélib' feed (num_bikes_available_types, stationCode, lastUpdatedOther...). It has not been
compared against the live feed: run `python -m velib.ingestion.collect collect` once locally
to confirm it.
"""
import pandas as pd
import pytest

from velib.ingestion import collect as vc

T0 = pd.Timestamp("2026-10-15 10:00:07", tz="UTC")


def status_payload(n=1400, updated=1_791_000_000):
    stations = []
    for i in range(n):
        stations.append({
            "station_id": 213_688_169 + i, "stationCode": f"{16000 + i}",
            "num_bikes_available": 7, "numBikesAvailable": 7,
            "num_bikes_available_types": [{"mechanical": 3}, {"ebike": 4}],
            "num_docks_available": 24, "numDocksAvailable": 24,
            "is_installed": 1, "is_returning": 1, "is_renting": 1,
            "last_reported": updated - 120,
        })
    return {"lastUpdatedOther": updated, "ttl": 3600, "data": {"stations": stations}}


def info_payload(n=1400):
    return {"data": {"stations": [
        {"station_id": 213_688_169 + i, "stationCode": f"{16000 + i}", "name": f"Station {i}",
         "lat": 48.85, "lon": 2.35, "capacity": 31} for i in range(n)]}}


def test_parse_status_values_and_types():
    df = vc.parse_status(status_payload(), T0)
    row = df.iloc[0]
    assert (row.bikes, row.mechanical, row.ebikes, row.docks) == (7, 3, 4, 24)
    assert bool(row.is_renting) and bool(row.is_installed)
    assert df["station_id"].dtype == "int64"
    assert str(df["fetched_at"].dtype) == vc.TS_DTYPE
    assert (df["fetched_at"] - df["feed_updated_at"]).dt.total_seconds().iloc[0] > 0
    assert (df["feed_updated_at"] - df["last_reported"]).dt.total_seconds().iloc[0] == 120


def test_never_reported_station_becomes_nat():
    payload = status_payload()
    payload["data"]["stations"][0]["last_reported"] = 0
    assert vc.parse_status(payload, T0)["last_reported"].isna().iloc[0]


def test_missing_optional_fields_are_tolerated():
    payload = status_payload()
    for station in payload["data"]["stations"]:
        del station["num_bikes_available_types"], station["stationCode"]
    del payload["lastUpdatedOther"]
    df = vc.parse_status(payload, T0)
    assert df["mechanical"].isna().all() and df["feed_updated_at"].isna().all()


@pytest.mark.parametrize("payload", [
    {"unexpected": 1},
    {"data": {"stations": []}},
    status_payload(n=10),
    {"data": {"stations": [{"station_id": i, "foo": 1} for i in range(500)]}},  # renamed fields
])
def test_format_change_fails_loudly(payload):
    with pytest.raises(ValueError):
        vc.parse_status(payload, T0)


def test_parse_information():
    df = vc.parse_information(info_payload(), T0)
    assert list(df.columns) == ["station_id", "station_code", "name", "lat", "lon",
                                "capacity", "fetched_at"]
    assert len(df) == 1400


def test_append_is_idempotent_and_schema_stable(tmp_path):
    path = tmp_path / "station_status_2026-10-15.parquet"
    first = vc.parse_status(status_payload(), T0)
    second = vc.parse_status(status_payload(), T0 + pd.Timedelta(minutes=5))
    assert vc.append_snapshot(path, first) == 1400
    assert vc.append_snapshot(path, first) == 1400   # same snapshot replayed: no duplicates
    assert vc.append_snapshot(path, second) == 2800
    df = pd.read_parquet(path)
    assert df.dtypes.astype(str).to_dict() == first.dtypes.astype(str).to_dict()
    assert not df.duplicated(["station_id", "fetched_at"]).any()


def test_collect_local_end_to_end(tmp_path, monkeypatch):
    calls = []

    def fake_fetch(filename, retries=3):
        calls.append(filename)
        return status_payload() if filename == "station_status.json" else info_payload()

    monkeypatch.setattr(vc, "fetch_json", fake_fetch)
    vc.collect_snapshot(tmp_path, upload=False)
    vc.collect_snapshot(tmp_path, upload=False)
    files = sorted(p.name for p in tmp_path.glob("*.parquet"))
    assert len(files) == 2 and any(f.startswith("station_information_") for f in files)
    assert calls.count("station_information.json") == 1   # static info fetched once per day
    status = vc.read_status(tmp_path)
    assert status["station_id"].nunique() == 1400 and len(status) >= 1400


def test_refuses_to_overwrite_unreadable_remote_file(tmp_path, monkeypatch):
    today = f"station_status_{pd.Timestamp.now(tz='UTC'):%Y-%m-%d}.parquet"
    uploaded = []
    monkeypatch.setattr(vc, "fetch_json", lambda filename, retries=3: status_payload())
    monkeypatch.setattr(vc, "ensure_release", lambda tag: None)
    monkeypatch.setattr(vc, "release_assets", lambda tag: {today})
    monkeypatch.setattr(vc, "release_download", lambda tag, name, dest: False)
    monkeypatch.setattr(vc, "release_upload", lambda tag, path, retries=3: uploaded.append(path))
    with pytest.raises(RuntimeError):
        vc.collect_snapshot(tmp_path, upload=True)
    assert uploaded == []   # nothing was sent: the remote file is not overwritten


def test_cli_help_runs(capsys):
    with pytest.raises(SystemExit) as exc:
        vc.main(["--help"])
    assert exc.value.code == 0
    assert "collect" in capsys.readouterr().out
