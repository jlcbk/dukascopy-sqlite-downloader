from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import lzma
import sqlite3
import struct
import sys
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

SCRIPT_PATH = Path(__file__).parents[1] / "download_dukascopy_sqlite.py"
SPEC = importlib.util.spec_from_file_location("dukascopy_sqlite_downloader", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
downloader = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = downloader
SPEC.loader.exec_module(downloader)


def bi5_payload(hour_number: int = 0) -> bytes:
    base = 110_000 + hour_number
    raw = b"".join(
        (
            struct.pack(">iiiff", 0, base + 2, base, 1.0, 2.0),
            struct.pack(">iiiff", 1_000, base + 4, base + 1, 3.0, 4.0),
        )
    )
    return lzma.compress(raw, format=lzma.FORMAT_ALONE)


def store_complete_database(database_dir: Path, hours: list) -> Path:
    symbol = "EURUSD"
    working = database_dir / ".work" / f"{symbol}.sqlite.part"
    connection = downloader.open_database(working, symbol)
    try:
        for number, hour in enumerate(hours):
            payload = bi5_payload(number)
            result = downloader.FetchResult(
                hour=hour,
                status="ok",
                url=downloader.source_url(symbol, hour),
                http_status=200,
                payload=payload,
                payload_sha256=hashlib.sha256(payload).hexdigest(),
                tick_stats=downloader.validate_payload(payload, symbol),
            )
            downloader.store_fetch_result(connection, result)
        connection.commit()
    finally:
        downloader.close_database(connection)
    return downloader.publish_database(database_dir, symbol)


def test_payload_validation_and_fetch_contract() -> None:
    payload = bi5_payload()
    stats = downloader.validate_payload(payload, "EURUSD")
    assert stats.tick_count == 2
    assert stats.first_offset_ms == 0
    assert stats.last_offset_ms == 1_000
    hour = downloader.parse_utc("2025-01-06T00:00:00Z")
    assert downloader.source_url("EURUSD", hour).endswith(
        "/EURUSD/2025/00/06/00h_ticks.bi5"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = downloader.fetch_hour(client, "EURUSD", hour, retries=0)
    assert result.status == "ok"
    assert result.payload_sha256 == hashlib.sha256(payload).hexdigest()


def test_publish_manifest_verify_and_aggregate(tmp_path) -> None:
    database_dir = tmp_path / "databases"
    hours = downloader.requested_hours(
        downloader.parse_utc("2025-01-06T00:00:00Z"),
        downloader.parse_utc("2025-01-06T08:00:00Z"),
    )
    final_path = store_complete_database(database_dir, hours)
    assert final_path.name == "EURUSD.sqlite"
    assert not (database_dir / ".work" / "EURUSD.sqlite.part").exists()
    assert not (database_dir / "EURUSD.sqlite-wal").exists()
    assert not (database_dir / "EURUSD.sqlite-shm").exists()
    assert (database_dir / "EURUSD.sqlite.sha256").exists()
    assert (database_dir / "EURUSD.sqlite.json").exists()

    assert downloader.manifest_command(
        SimpleNamespace(database_dir=str(database_dir), symbols="EURUSD", output=None)
    ) == 0
    assert downloader.verify_command(
        SimpleNamespace(database_dir=str(database_dir), manifest=None, deep=True)
    ) == 0

    output_dir = tmp_path / "bars"
    arguments = SimpleNamespace(
        database_dir=str(database_dir),
        output_dir=str(output_dir),
        symbols="EURUSD",
        start="2025-01-06T00:00:00Z",
        end="2025-01-06T08:00:00Z",
        interval="4h",
        allow_incomplete=False,
    )
    assert downloader.aggregate_command(arguments) == 0
    with (output_dir / "EURUSD.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    assert all(int(row["tick_count"]) == 8 for row in rows)
    manifest = json.loads(
        (output_dir / "_data_manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["symbols"]["EURUSD"]["source"]["manifest_complete"]


def test_aggregate_refuses_missing_requested_hour(tmp_path) -> None:
    database_dir = tmp_path / "databases"
    hours = downloader.requested_hours(
        downloader.parse_utc("2025-01-06T00:00:00Z"),
        downloader.parse_utc("2025-01-06T08:00:00Z"),
    )
    final_path = store_complete_database(database_dir, hours)
    with sqlite3.connect(final_path) as connection:
        connection.execute(
            "DELETE FROM hours WHERE hour_utc = ?",
            (downloader.epoch_hour(hours[2]),),
        )
    with pytest.raises(ValueError, match="requested hours are absent"):
        downloader.aggregate_symbol(
            final_path,
            tmp_path / "EURUSD.csv",
            "EURUSD",
            hours[0],
            hours[-1] + downloader.timedelta(hours=1),
            "4h",
            False,
        )


def test_download_resumes_and_only_publishes_complete_database(
    tmp_path, monkeypatch
) -> None:
    calls: list[str] = []

    class FakeResponse:
        status_code = 200

        def __init__(self, url: str) -> None:
            self.content = bi5_payload(int(url.rsplit("/", 1)[-1][:2]))

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def get(self, url: str):
            calls.append(url)
            return FakeResponse(url)

    monkeypatch.setattr(
        downloader,
        "build_http_client",
        lambda proxy, use_env_proxy, timeout: FakeClient(),
    )
    arguments = SimpleNamespace(
        symbols="EURUSD",
        start="2025-01-06T00:00:00Z",
        end="2025-01-06T08:00:00Z",
        database_dir=str(tmp_path),
        workers=2,
        retries=0,
        timeout=1.0,
        batch_size=4,
        proxy=None,
        use_env_proxy=False,
        refresh_no_data=False,
    )
    assert downloader.download_command(arguments) == 0
    assert len(calls) == 8
    assert (tmp_path / "EURUSD.sqlite").exists()
    assert not (tmp_path / ".work" / "EURUSD.sqlite.part").exists()

    calls.clear()
    assert downloader.download_command(arguments) == 0
    assert calls == []


def test_cli_defaults_to_direct() -> None:
    args = downloader.build_parser().parse_args(["download"])
    assert args.proxy is None
    assert not args.use_env_proxy
    downloader.validate_cli_arguments(args)
