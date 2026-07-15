#!/usr/bin/env python3
"""Standalone Dukascopy downloader and local aggregator.

VPS workflow:
  python download_dukascopy_sqlite.py download --database-dir dukascopy_sqlite
  python download_dukascopy_sqlite.py manifest --database-dir dukascopy_sqlite

Local workflow after copying the directory:
  python download_dukascopy_sqlite.py verify --database-dir dukascopy_sqlite
  python download_dukascopy_sqlite.py aggregate \
      --database-dir dukascopy_sqlite --output-dir data/dukascopy_bid_ask

Only ``download`` requires the third-party ``httpx`` package. All validation,
manifest and aggregation commands use the Python standard library.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import lzma
import math
import sqlite3
import struct
import sys
import time
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import httpx
except ImportError:  # Aggregate/verify remain usable without httpx.
    httpx = None  # type: ignore[assignment]

BASE_URL = "https://datafeed.dukascopy.com/datafeed"
PROGRAM_VERSION = "1.1.0"
PARSER_VERSION = "dukascopy-bi5-v1"
DATABASE_SCHEMA_VERSION = 1
TICK_RECORD = struct.Struct(">iiiff")

DEFAULT_SYMBOLS = (
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "USDCHF",
    "AUDUSD",
    "NZDUSD",
    "USDCAD",
    "EURGBP",
    "EURJPY",
    "GBPJPY",
    "AUDJPY",
    "CADJPY",
    "USDNOK",
    "USDSEK",
)

PRICE_DIVISORS = {
    "AUDCAD": 100_000,
    "AUDCHF": 100_000,
    "AUDJPY": 1_000,
    "AUDNZD": 100_000,
    "AUDUSD": 100_000,
    "CADCHF": 100_000,
    "CADJPY": 1_000,
    "CHFJPY": 1_000,
    "EURAUD": 100_000,
    "EURCAD": 100_000,
    "EURCHF": 100_000,
    "EURGBP": 100_000,
    "EURJPY": 1_000,
    "EURNZD": 100_000,
    "EURUSD": 100_000,
    "GBPAUD": 100_000,
    "GBPCAD": 100_000,
    "GBPCHF": 100_000,
    "GBPJPY": 1_000,
    "GBPNZD": 100_000,
    "GBPUSD": 100_000,
    "NZDCAD": 100_000,
    "NZDCHF": 100_000,
    "NZDJPY": 1_000,
    "NZDUSD": 100_000,
    "USDCAD": 100_000,
    "USDCHF": 100_000,
    "USDJPY": 1_000,
    "USDNOK": 100_000,
    "USDSEK": 100_000,
}

CSV_COLUMNS = (
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "bid_open",
    "bid_high",
    "bid_low",
    "bid_close",
    "ask_open",
    "ask_high",
    "ask_low",
    "ask_close",
    "tick_count",
)

DATABASE_SQL = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hours (
    hour_utc INTEGER PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('ok', 'no_data')),
    payload BLOB,
    payload_sha256 TEXT,
    compressed_bytes INTEGER NOT NULL,
    tick_count INTEGER NOT NULL,
    first_offset_ms INTEGER,
    last_offset_ms INTEGER,
    http_status INTEGER,
    retrieved_at TEXT NOT NULL,
    source_url TEXT NOT NULL,
    CHECK (
        (status = 'ok' AND payload IS NOT NULL AND payload_sha256 IS NOT NULL)
        OR (status = 'no_data' AND payload IS NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_hours_status ON hours(status);
"""


@dataclass(frozen=True)
class TickStats:
    tick_count: int
    first_offset_ms: int | None
    last_offset_ms: int | None


@dataclass(frozen=True)
class FetchResult:
    hour: datetime
    status: str
    url: str
    http_status: int | None = None
    payload: bytes | None = None
    payload_sha256: str | None = None
    tick_stats: TickStats | None = None
    error: str | None = None


def utc_now() -> datetime:
    return datetime.now(UTC)


def parse_utc(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso_hour(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def epoch_hour(value: datetime) -> int:
    return int(value.astimezone(UTC).timestamp())


def datetime_from_epoch(value: int) -> datetime:
    return datetime.fromtimestamp(value, tz=UTC)


def normalize_symbols(raw: str | Iterable[str]) -> list[str]:
    values = raw.split(",") if isinstance(raw, str) else list(raw)
    output: list[str] = []
    for value in values:
        symbol = value.upper().replace("/", "").replace("_", "").strip()
        if symbol not in PRICE_DIVISORS:
            raise ValueError(f"Unsupported symbol or missing price metadata: {symbol}")
        if symbol not in output:
            output.append(symbol)
    if not output:
        raise ValueError("At least one symbol is required")
    return output


def candidate_market_hour(value: datetime) -> bool:
    weekday = value.weekday()
    return weekday < 4 or (weekday == 4 and value.hour < 22) or (
        weekday == 6 and value.hour >= 21
    )


def requested_hours(start: datetime, end: datetime) -> list[datetime]:
    current = start.replace(minute=0, second=0, microsecond=0)
    output: list[datetime] = []
    while current < end:
        if candidate_market_hour(current):
            output.append(current)
        current += timedelta(hours=1)
    return output


def source_url(symbol: str, hour: datetime) -> str:
    month = hour.month - 1  # Dukascopy uses zero-based month directories.
    return (
        f"{BASE_URL}/{symbol}/{hour.year:04d}/{month:02d}/{hour.day:02d}/"
        f"{hour.hour:02d}h_ticks.bi5"
    )


def validate_payload(payload: bytes, symbol: str) -> TickStats:
    try:
        raw = lzma.decompress(payload)
    except lzma.LZMAError as error:
        raise ValueError("invalid LZMA payload") from error
    if not raw:
        return TickStats(0, None, None)
    if len(raw) % TICK_RECORD.size:
        raise ValueError("decompressed payload is not a multiple of 20 bytes")
    previous_offset = -1
    first_offset: int | None = None
    count = 0
    for offset, ask_integer, bid_integer, ask_volume, bid_volume in TICK_RECORD.iter_unpack(raw):
        if offset < 0 or offset >= 3_600_000 or offset < previous_offset:
            raise ValueError("invalid or unordered tick offset")
        if bid_integer <= 0 or ask_integer < bid_integer:
            raise ValueError("invalid or crossed bid/ask quote")
        if (
            not math.isfinite(ask_volume)
            or not math.isfinite(bid_volume)
            or ask_volume < 0
            or bid_volume < 0
        ):
            raise ValueError("invalid bid/ask quote volume")
        if first_offset is None:
            first_offset = offset
        previous_offset = offset
        count += 1
    return TickStats(count, first_offset, previous_offset)


def metadata_get(connection: sqlite3.Connection, key: str) -> str | None:
    row = connection.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
    return str(row[0]) if row is not None else None


def metadata_set(connection: sqlite3.Connection, key: str, value: object) -> None:
    connection.execute(
        "INSERT INTO metadata(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )


def open_database(path: Path, symbol: str) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=60)
    connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    connection.execute("PRAGMA busy_timeout = 60000")
    connection.executescript(DATABASE_SQL)
    contract = {
        "database_schema_version": DATABASE_SCHEMA_VERSION,
        "parser_version": PARSER_VERSION,
        "provider": "dukascopy",
        "base_url": BASE_URL,
        "symbol": symbol,
        "price_divisor": PRICE_DIVISORS[symbol],
    }
    for key, expected in contract.items():
        existing = metadata_get(connection, key)
        if existing is not None and existing != str(expected):
            connection.close()
            raise ValueError(
                f"{path}: metadata {key}={existing!r} does not match {expected!r}"
            )
        metadata_set(connection, key, expected)
    if metadata_get(connection, "created_at") is None:
        metadata_set(connection, "created_at", utc_now().isoformat())
    connection.commit()
    return connection


def close_database(connection: sqlite3.Connection) -> None:
    connection.commit()
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    connection.execute("PRAGMA journal_mode = DELETE")
    connection.commit()
    connection.close()


def sidecar_path(database_path: Path, suffix: str) -> Path:
    return Path(str(database_path) + suffix)


def database_contains_all_hours(
    database_path: Path, symbol: str, hours: list[datetime]
) -> bool:
    if not database_path.exists() or not hours:
        return False
    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    try:
        if metadata_get(connection, "symbol") != symbol:
            return False
        expected = {epoch_hour(hour) for hour in hours}
        stored = {
            int(row[0])
            for row in connection.execute(
                "SELECT hour_utc FROM hours WHERE hour_utc >= ? AND hour_utc < ?",
                (min(expected), max(expected) + 3600),
            )
        }
        return expected.issubset(stored)
    finally:
        connection.close()


def prepare_working_database(
    database_dir: Path, symbol: str, hours: list[datetime]
) -> Path | None:
    final_path = database_dir / f"{symbol}.sqlite"
    working_path = database_dir / ".work" / f"{symbol}.sqlite.part"
    working_path.parent.mkdir(parents=True, exist_ok=True)
    if final_path.exists() and working_path.exists():
        raise ValueError(
            f"Both final and working databases exist for {symbol}; inspect them before continuing"
        )
    if working_path.exists():
        return working_path
    if final_path.exists():
        if database_contains_all_hours(final_path, symbol, hours):
            return None
        final_path.replace(working_path)
        sidecar_path(final_path, ".sha256").unlink(missing_ok=True)
        sidecar_path(final_path, ".json").unlink(missing_ok=True)
        return working_path
    return working_path


def fetch_hour(
    client: Any,
    symbol: str,
    hour: datetime,
    retries: int,
) -> FetchResult:
    url = source_url(symbol, hour)
    last_error: str | None = None
    for attempt in range(retries + 1):
        try:
            response = client.get(url)
            status = int(response.status_code)
            if status == 404 or (status == 200 and not response.content):
                return FetchResult(hour, "no_data", url, http_status=status)
            if status == 200:
                payload = bytes(response.content)
                try:
                    stats = validate_payload(payload, symbol)
                except ValueError as error:
                    last_error = f"payload validation: {error}"
                else:
                    if stats.tick_count == 0:
                        return FetchResult(hour, "no_data", url, http_status=status)
                    return FetchResult(
                        hour,
                        "ok",
                        url,
                        http_status=status,
                        payload=payload,
                        payload_sha256=hashlib.sha256(payload).hexdigest(),
                        tick_stats=stats,
                    )
            elif status not in {408, 425, 429, 500, 502, 503, 504}:
                return FetchResult(
                    hour, "failed", url, http_status=status, error=f"HTTP {status}"
                )
            else:
                last_error = f"HTTP {status}"
        except Exception as error:  # Network errors are retried and never persisted.
            last_error = f"{type(error).__name__}: {error}"
        if attempt < retries:
            time.sleep(min(2**attempt, 8))
    return FetchResult(hour, "failed", url, error=last_error or "unknown error")


def store_fetch_result(connection: sqlite3.Connection, result: FetchResult) -> None:
    stats = result.tick_stats or TickStats(0, None, None)
    connection.execute(
        """
        INSERT INTO hours(
            hour_utc, status, payload, payload_sha256, compressed_bytes,
            tick_count, first_offset_ms, last_offset_ms, http_status,
            retrieved_at, source_url
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(hour_utc) DO UPDATE SET
            status=excluded.status,
            payload=excluded.payload,
            payload_sha256=excluded.payload_sha256,
            compressed_bytes=excluded.compressed_bytes,
            tick_count=excluded.tick_count,
            first_offset_ms=excluded.first_offset_ms,
            last_offset_ms=excluded.last_offset_ms,
            http_status=excluded.http_status,
            retrieved_at=excluded.retrieved_at,
            source_url=excluded.source_url
        """,
        (
            epoch_hour(result.hour),
            result.status,
            result.payload,
            result.payload_sha256,
            len(result.payload or b""),
            stats.tick_count,
            stats.first_offset_ms,
            stats.last_offset_ms,
            result.http_status,
            utc_now().isoformat(),
            result.url,
        ),
    )


def database_status_counts(
    connection: sqlite3.Connection, start_epoch: int, end_epoch: int
) -> dict[str, int]:
    counts = {"ok": 0, "no_data": 0}
    for status, count in connection.execute(
        "SELECT status, COUNT(*) FROM hours "
        "WHERE hour_utc >= ? AND hour_utc < ? GROUP BY status",
        (start_epoch, end_epoch),
    ):
        counts[str(status)] = int(count)
    return counts


def format_duration(seconds: float) -> str:
    if not math.isfinite(seconds):
        return "unknown"
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def download_symbol(
    client: Any,
    database_dir: Path,
    symbol: str,
    hours: list[datetime],
    workers: int,
    retries: int,
    batch_size: int,
    refresh_no_data: bool,
) -> int:
    database_path = prepare_working_database(database_dir, symbol, hours)
    if database_path is None:
        print(f"[{symbol}] already published and complete", flush=True)
        return 0
    connection = open_database(database_path, symbol)
    start_epoch = epoch_hour(hours[0]) if hours else 0
    end_epoch = epoch_hour(hours[-1] + timedelta(hours=1)) if hours else 0
    try:
        if refresh_no_data and hours:
            connection.execute(
                "DELETE FROM hours WHERE status='no_data' AND hour_utc >= ? AND hour_utc < ?",
                (start_epoch, end_epoch),
            )
            connection.commit()
        existing = {
            int(row[0])
            for row in connection.execute(
                "SELECT hour_utc FROM hours WHERE hour_utc >= ? AND hour_utc < ?",
                (start_epoch, end_epoch),
            )
        }
        pending = [hour for hour in hours if epoch_hour(hour) not in existing]
        counts = database_status_counts(connection, start_epoch, end_epoch)
        print(
            f"[{symbol}] expected={len(hours):,} existing={len(existing):,} "
            f"ok={counts['ok']:,} no_data={counts['no_data']:,} pending={len(pending):,}",
            flush=True,
        )
        if not pending:
            return 0
        started = time.monotonic()
        processed = 0
        failures = 0
        recent_cutoff = utc_now() - timedelta(days=7)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for offset in range(0, len(pending), batch_size):
                batch = pending[offset : offset + batch_size]
                futures = [
                    executor.submit(fetch_hour, client, symbol, hour, retries)
                    for hour in batch
                ]
                stored = 0
                for future in as_completed(futures):
                    result = future.result()
                    processed += 1
                    if result.status == "failed":
                        failures += 1
                        if failures <= 10:
                            print(
                                f"[{symbol}] failed {iso_hour(result.hour)}: {result.error}",
                                file=sys.stderr,
                                flush=True,
                            )
                        continue
                    if result.status == "no_data" and result.hour >= recent_cutoff:
                        # Recent 404/empty responses may be publication lag; retry next run.
                        failures += 1
                        continue
                    store_fetch_result(connection, result)
                    stored += 1
                connection.commit()
                elapsed = max(time.monotonic() - started, 1e-9)
                rate = processed / elapsed
                remaining = len(pending) - processed
                eta = remaining / rate if rate > 0 else float("inf")
                print(
                    f"[{symbol}] {processed:,}/{len(pending):,} "
                    f"stored={stored:,} failed={failures:,} rate={rate:.2f}/s "
                    f"eta={format_duration(eta)}",
                    flush=True,
                )
        counts = database_status_counts(connection, start_epoch, end_epoch)
        completed = counts["ok"] + counts["no_data"]
        missing = len(hours) - completed
        metadata_set(connection, "requested_start", iso_hour(hours[0]))
        metadata_set(
            connection,
            "requested_end_exclusive",
            iso_hour(hours[-1] + timedelta(hours=1)),
        )
        metadata_set(connection, "expected_hours", len(hours))
        metadata_set(connection, "completed_hours", completed)
        metadata_set(connection, "ok_hours", counts["ok"])
        metadata_set(connection, "no_data_hours", counts["no_data"])
        metadata_set(connection, "missing_hours", missing)
        metadata_set(connection, "updated_at", utc_now().isoformat())
        connection.commit()
        print(
            f"[{symbol}] complete={completed:,}/{len(hours):,}; missing={missing:,}; "
            f"database={database_path}",
            flush=True,
        )
        return missing
    finally:
        close_database(connection)


def publish_database(database_dir: Path, symbol: str) -> Path:
    working_path = database_dir / ".work" / f"{symbol}.sqlite.part"
    final_path = database_dir / f"{symbol}.sqlite"
    if working_path.exists():
        if final_path.exists():
            raise ValueError(f"Refusing to replace existing published database: {final_path}")
        working_path.replace(final_path)
    if not final_path.exists():
        raise FileNotFoundError(final_path)
    hash_path = sidecar_path(final_path, ".sha256")
    info_path = sidecar_path(final_path, ".json")
    if hash_path.exists() and info_path.exists():
        return final_path
    summary = read_database_summary(final_path)
    if summary["integrity"] != "ok":
        raise ValueError(f"{final_path}: SQLite quick_check failed")
    print(
        f"[{symbol}] finalizing SHA-256 ({final_path.stat().st_size / 1e9:.2f} GB)…",
        flush=True,
    )
    digest = sha256_file(final_path)
    hash_temporary = hash_path.with_suffix(hash_path.suffix + ".tmp")
    hash_temporary.write_text(f"{digest}  {final_path.name}\n", encoding="utf-8")
    hash_temporary.replace(hash_path)
    atomic_json_write(
        info_path,
        {
            "schema_version": 1,
            "program_version": PROGRAM_VERSION,
            "created_at": utc_now().isoformat(),
            "symbol": symbol,
            "file": final_path.name,
            "bytes": final_path.stat().st_size,
            "sha256": digest,
            **summary,
        },
    )
    print(f"[{symbol}] published atomically: {final_path}", flush=True)
    return final_path


def build_http_client(proxy: str | None, use_env_proxy: bool, timeout: float) -> Any:
    if httpx is None:
        raise RuntimeError(
            "download requires httpx; install it with: python -m pip install 'httpx>=0.27,<1'"
        )
    kwargs: dict[str, object] = {
        "timeout": timeout,
        "follow_redirects": True,
        "limits": httpx.Limits(max_connections=32, max_keepalive_connections=16),
        "headers": {"User-Agent": f"dukascopy-sqlite-downloader/{PROGRAM_VERSION}"},
    }
    if proxy:
        kwargs["proxy"] = proxy
        kwargs["trust_env"] = False
    else:
        kwargs["trust_env"] = use_env_proxy
    return httpx.Client(**kwargs)


def download_command(args: argparse.Namespace) -> int:
    symbols = normalize_symbols(args.symbols)
    start = parse_utc(args.start)
    requested_end = parse_utc(args.end)
    complete_watermark = utc_now().replace(minute=0, second=0, microsecond=0)
    end = min(requested_end, complete_watermark)
    if start >= end:
        raise ValueError("start must be earlier than the complete-hour end watermark")
    hours = requested_hours(start, end)
    if not hours:
        raise ValueError("requested range contains no candidate FX market hours")
    database_dir = Path(args.database_dir)
    database_dir.mkdir(parents=True, exist_ok=True)
    print(
        f"symbols={len(symbols)} hours_per_symbol={len(hours):,} "
        f"total_requests_max={len(symbols) * len(hours):,} database_dir={database_dir}",
        flush=True,
    )
    total_missing = 0
    with build_http_client(args.proxy, args.use_env_proxy, args.timeout) as client:
        for symbol in symbols:
            missing = download_symbol(
                client,
                database_dir,
                symbol,
                hours,
                args.workers,
                args.retries,
                args.batch_size,
                args.refresh_no_data,
            )
            total_missing += missing
            if missing == 0:
                publish_database(database_dir, symbol)
    if total_missing:
        print(
            f"Download finished with {total_missing:,} missing hours; rerun the same command "
            "to resume. Do not aggregate a formal dataset yet.",
            file=sys.stderr,
        )
        return 1
    print("Download complete. Run the manifest command before transferring databases.")
    return 0


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def read_database_summary(path: Path) -> dict[str, object]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        integrity = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        metadata = {
            str(key): str(value)
            for key, value in connection.execute("SELECT key, value FROM metadata")
        }
        counts = {"ok": 0, "no_data": 0}
        for status, count in connection.execute(
            "SELECT status, COUNT(*) FROM hours GROUP BY status"
        ):
            counts[str(status)] = int(count)
        bounds = connection.execute("SELECT MIN(hour_utc), MAX(hour_utc) FROM hours").fetchone()
        return {
            "integrity": integrity,
            "metadata": metadata,
            "counts": counts,
            "first_hour": (
                iso_hour(datetime_from_epoch(int(bounds[0])))
                if bounds[0] is not None
                else None
            ),
            "last_hour": (
                iso_hour(datetime_from_epoch(int(bounds[1])))
                if bounds[1] is not None
                else None
            ),
        }
    finally:
        connection.close()


def atomic_json_write(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    temporary.replace(path)


def manifest_command(args: argparse.Namespace) -> int:
    database_dir = Path(args.database_dir)
    symbols = normalize_symbols(args.symbols)
    output = Path(args.output) if args.output else database_dir / "_sqlite_manifest.json"
    databases: dict[str, object] = {}
    for symbol in symbols:
        path = database_dir / f"{symbol}.sqlite"
        if not path.exists():
            raise FileNotFoundError(path)
        print(f"Hashing {path} ({path.stat().st_size / 1e9:.2f} GB)…", flush=True)
        summary = read_database_summary(path)
        if summary["integrity"] != "ok":
            raise ValueError(f"{path}: SQLite quick_check failed: {summary['integrity']}")
        databases[symbol] = {
            "file": path.name,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
            **summary,
        }
    atomic_json_write(
        output,
        {
            "schema_version": 1,
            "program_version": PROGRAM_VERSION,
            "created_at": utc_now().isoformat(),
            "parser_version": PARSER_VERSION,
            "databases": databases,
        },
    )
    print(f"Transfer manifest written to {output}")
    return 0


def verify_database_payloads(path: Path, symbol: str) -> int:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    checked = 0
    try:
        for hour_epoch, payload, expected_hash in connection.execute(
            "SELECT hour_utc, payload, payload_sha256 FROM hours "
            "WHERE status='ok' ORDER BY hour_utc"
        ):
            raw_payload = bytes(payload)
            if hashlib.sha256(raw_payload).hexdigest() != expected_hash:
                raise ValueError(
                    f"{path}: payload SHA mismatch at {iso_hour(datetime_from_epoch(hour_epoch))}"
                )
            validate_payload(raw_payload, symbol)
            checked += 1
            if checked % 10_000 == 0:
                print(f"[{symbol}] deep-verified {checked:,} payloads", flush=True)
        return checked
    finally:
        connection.close()


def verify_command(args: argparse.Namespace) -> int:
    database_dir = Path(args.database_dir)
    manifest_path = Path(args.manifest) if args.manifest else database_dir / "_sqlite_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for symbol, expected in manifest["databases"].items():
        path = database_dir / expected["file"]
        if not path.exists():
            raise FileNotFoundError(path)
        actual_size = path.stat().st_size
        if actual_size != int(expected["bytes"]):
            raise ValueError(f"{path}: size mismatch ({actual_size} != {expected['bytes']})")
        print(f"Verifying SHA-256 {path}…", flush=True)
        actual_hash = sha256_file(path)
        if actual_hash != expected["sha256"]:
            raise ValueError(f"{path}: database SHA-256 mismatch")
        summary = read_database_summary(path)
        if summary["integrity"] != "ok":
            raise ValueError(f"{path}: SQLite quick_check failed")
        if args.deep:
            checked = verify_database_payloads(path, symbol)
            print(f"[{symbol}] deep verification complete: {checked:,} payloads")
    print("All transferred databases match the manifest.")
    return 0


def decode_hour_bar(payload: bytes, symbol: str, hour: datetime) -> dict[str, object]:
    stats = validate_payload(payload, symbol)
    if stats.tick_count == 0:
        raise ValueError(f"{symbol} {iso_hour(hour)}: empty payload cannot form a bar")
    raw = lzma.decompress(payload)
    divisor = PRICE_DIVISORS[symbol]
    first_bid = first_ask = first_mid = None
    last_bid = last_ask = last_mid = None
    bid_high = ask_high = mid_high = float("-inf")
    bid_low = ask_low = mid_low = float("inf")
    volume = 0.0
    for _, ask_integer, bid_integer, ask_volume, bid_volume in TICK_RECORD.iter_unpack(raw):
        ask = ask_integer / divisor
        bid = bid_integer / divisor
        mid = (ask + bid) / 2
        if first_bid is None:
            first_bid, first_ask, first_mid = bid, ask, mid
        last_bid, last_ask, last_mid = bid, ask, mid
        bid_high, bid_low = max(bid_high, bid), min(bid_low, bid)
        ask_high, ask_low = max(ask_high, ask), min(ask_low, ask)
        mid_high, mid_low = max(mid_high, mid), min(mid_low, mid)
        volume += float(ask_volume) + float(bid_volume)
    return {
        "timestamp": hour,
        "open": first_mid,
        "high": mid_high,
        "low": mid_low,
        "close": last_mid,
        "volume": volume,
        "bid_open": first_bid,
        "bid_high": bid_high,
        "bid_low": bid_low,
        "bid_close": last_bid,
        "ask_open": first_ask,
        "ask_high": ask_high,
        "ask_low": ask_low,
        "ask_close": last_ask,
        "tick_count": stats.tick_count,
    }


def bucket_start(hour: datetime, interval: str) -> datetime:
    if interval == "1h":
        return hour
    return hour.replace(hour=(hour.hour // 4) * 4, minute=0, second=0, microsecond=0)


def combine_bars(bars: list[dict[str, object]], interval: str) -> dict[str, object] | None:
    expected = 1 if interval == "1h" else 4
    if len(bars) != expected:
        return None
    start = bars[0]["timestamp"]
    if not isinstance(start, datetime):
        raise TypeError("bar timestamp is not a datetime")
    for index, bar in enumerate(bars):
        if bar["timestamp"] != start + timedelta(hours=index):
            return None
    return {
        "timestamp": iso_hour(start),
        "open": bars[0]["open"],
        "high": max(float(bar["high"]) for bar in bars),
        "low": min(float(bar["low"]) for bar in bars),
        "close": bars[-1]["close"],
        "volume": sum(float(bar["volume"]) for bar in bars),
        "bid_open": bars[0]["bid_open"],
        "bid_high": max(float(bar["bid_high"]) for bar in bars),
        "bid_low": min(float(bar["bid_low"]) for bar in bars),
        "bid_close": bars[-1]["bid_close"],
        "ask_open": bars[0]["ask_open"],
        "ask_high": max(float(bar["ask_high"]) for bar in bars),
        "ask_low": min(float(bar["ask_low"]) for bar in bars),
        "ask_close": bars[-1]["ask_close"],
        "tick_count": sum(int(bar["tick_count"]) for bar in bars),
    }


def aggregate_symbol(
    database_path: Path,
    output_path: Path,
    symbol: str,
    start: datetime,
    end: datetime,
    interval: str,
    allow_incomplete: bool,
) -> dict[str, object]:
    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    expected = requested_hours(start, end)
    expected_epochs = {epoch_hour(hour) for hour in expected}
    try:
        metadata = {
            str(key): str(value)
            for key, value in connection.execute("SELECT key, value FROM metadata")
        }
        if metadata.get("symbol") != symbol or metadata.get("parser_version") != PARSER_VERSION:
            raise ValueError(f"{database_path}: incompatible symbol/parser metadata")
        status_rows = {
            int(hour): str(status)
            for hour, status in connection.execute(
                "SELECT hour_utc, status FROM hours WHERE hour_utc >= ? AND hour_utc < ?",
                (epoch_hour(start), epoch_hour(end)),
            )
            if int(hour) in expected_epochs
        }
        missing = sorted(expected_epochs - set(status_rows))
        if missing and not allow_incomplete:
            first_missing = iso_hour(datetime_from_epoch(missing[0]))
            raise ValueError(
                f"{symbol}: {len(missing):,} requested hours are absent; first={first_missing}. "
                "Rerun download or pass --allow-incomplete for diagnostic output only."
            )
        ok_hours = sum(status == "ok" for status in status_rows.values())
        no_data_hours = sum(status == "no_data" for status in status_rows.values())
        temporary = output_path.with_suffix(output_path.suffix + ".tmp")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        row_count = 0
        first_timestamp: str | None = None
        last_timestamp: str | None = None
        spread_sum = 0.0
        total_ticks = 0
        active_bucket: datetime | None = None
        bucket_bars: list[dict[str, object]] = []
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
            writer.writeheader()

            def write_bucket() -> None:
                nonlocal row_count, first_timestamp, last_timestamp, spread_sum, total_ticks
                combined = combine_bars(bucket_bars, interval)
                if combined is None:
                    return
                writer.writerow(combined)
                timestamp = str(combined["timestamp"])
                first_timestamp = first_timestamp or timestamp
                last_timestamp = timestamp
                spread_sum += float(combined["ask_close"]) - float(combined["bid_close"])
                total_ticks += int(combined["tick_count"])
                row_count += 1

            cursor = connection.execute(
                "SELECT hour_utc, payload, payload_sha256 FROM hours "
                "WHERE status='ok' AND hour_utc >= ? AND hour_utc < ? ORDER BY hour_utc",
                (epoch_hour(start), epoch_hour(end)),
            )
            for hour_epoch, payload, expected_hash in cursor:
                hour = datetime_from_epoch(int(hour_epoch))
                if int(hour_epoch) not in expected_epochs:
                    continue
                raw_payload = bytes(payload)
                if hashlib.sha256(raw_payload).hexdigest() != expected_hash:
                    raise ValueError(f"{symbol} {iso_hour(hour)}: payload SHA-256 mismatch")
                bar = decode_hour_bar(raw_payload, symbol, hour)
                target_bucket = bucket_start(hour, interval)
                if active_bucket is None:
                    active_bucket = target_bucket
                if target_bucket != active_bucket:
                    write_bucket()
                    bucket_bars = []
                    active_bucket = target_bucket
                bucket_bars.append(bar)
            if bucket_bars:
                write_bucket()
        if row_count < 2:
            temporary.unlink(missing_ok=True)
            raise ValueError(f"{symbol}: aggregation produced fewer than two complete bars")
        temporary.replace(output_path)
        csv_hash = sha256_file(output_path)
        return {
            "csv_sha256": csv_hash,
            "rows": row_count,
            "start": first_timestamp,
            "end": last_timestamp,
            "dropped_invalid_ohlc": 0,
            "price_mode": "bid_ask",
            "mean_spread_price": spread_sum / row_count,
            "historical_swap": {"long": False, "short": False},
            "source": {
                "provider": "dukascopy",
                "requested_hours": len(expected),
                "available_hours": ok_hours,
                "no_data_hours": no_data_hours,
                "failed_hours": len(missing),
                "downloaded_hours": ok_hours,
                "cached_hours": 0,
                "tick_count": total_ticks,
                "hour_coverage": ok_hours / len(expected) if expected else 0.0,
                "base_url": BASE_URL,
                "price_divisor": PRICE_DIVISORS[symbol],
                "volume_semantics": "sum_bid_ask_quote_size",
                "parser_version": PARSER_VERSION,
                "manifest_complete": not missing,
            },
        }
    finally:
        connection.close()


def aggregate_command(args: argparse.Namespace) -> int:
    symbols = normalize_symbols(args.symbols)
    start = parse_utc(args.start)
    end = parse_utc(args.end)
    if start >= end:
        raise ValueError("start must be earlier than end")
    database_dir = Path(args.database_dir)
    output_dir = Path(args.output_dir)
    manifest_symbols: dict[str, object] = {}
    for symbol in symbols:
        database_path = database_dir / f"{symbol}.sqlite"
        if not database_path.exists():
            raise FileNotFoundError(database_path)
        print(f"[{symbol}] aggregating {args.interval} bars…", flush=True)
        manifest_symbols[symbol] = aggregate_symbol(
            database_path,
            output_dir / f"{symbol}.csv",
            symbol,
            start,
            end,
            args.interval,
            args.allow_incomplete,
        )
        print(
            f"[{symbol}] rows={manifest_symbols[symbol]['rows']:,} "
            f"output={output_dir / f'{symbol}.csv'}",
            flush=True,
        )
    atomic_json_write(
        output_dir / "_data_manifest.json",
        {"schema_version": 2, "symbols": manifest_symbols},
    )
    print(f"Aggregation complete: {output_dir}")
    return 0


def add_range_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    parser.add_argument("--start", default="2016-01-01")
    parser.add_argument("--end", default="2026-01-01", help="Exclusive UTC end")
    parser.add_argument("--database-dir", default="dukascopy_sqlite")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download validated Dukascopy ticks into per-symbol SQLite databases"
    )
    parser.add_argument("--version", action="version", version=PROGRAM_VERSION)
    subparsers = parser.add_subparsers(dest="command", required=True)

    download = subparsers.add_parser("download", help="Download/resume raw hourly bi5 payloads")
    add_range_arguments(download)
    download.add_argument("--workers", type=int, default=2)
    download.add_argument("--retries", type=int, default=5)
    download.add_argument("--timeout", type=float, default=30.0)
    download.add_argument("--batch-size", type=int, default=64)
    download.add_argument("--proxy", help="Explicit proxy URL, e.g. http://127.0.0.1:7890")
    download.add_argument(
        "--use-env-proxy",
        action="store_true",
        help="Honor HTTP(S)_PROXY environment variables; default is direct",
    )
    download.add_argument(
        "--refresh-no-data", action="store_true", help="Retry previously confirmed empty hours"
    )
    download.set_defaults(handler=download_command)

    manifest = subparsers.add_parser(
        "manifest", help="Hash closed databases before transferring them"
    )
    manifest.add_argument("--symbols", default=",".join(DEFAULT_SYMBOLS))
    manifest.add_argument("--database-dir", default="dukascopy_sqlite")
    manifest.add_argument("--output")
    manifest.set_defaults(handler=manifest_command)

    verify = subparsers.add_parser(
        "verify", help="Verify transferred databases against _sqlite_manifest.json"
    )
    verify.add_argument("--database-dir", default="dukascopy_sqlite")
    verify.add_argument("--manifest")
    verify.add_argument("--deep", action="store_true", help="Decode every stored bi5 payload")
    verify.set_defaults(handler=verify_command)

    aggregate = subparsers.add_parser(
        "aggregate", help="Aggregate transferred raw databases into research CSV files"
    )
    add_range_arguments(aggregate)
    aggregate.add_argument("--output-dir", default="data/dukascopy_bid_ask")
    aggregate.add_argument("--interval", choices=("1h", "4h"), default="4h")
    aggregate.add_argument(
        "--allow-incomplete",
        action="store_true",
        help="Create diagnostic output despite absent requested hours; it cannot pass data audit",
    )
    aggregate.set_defaults(handler=aggregate_command)
    return parser


def validate_cli_arguments(args: argparse.Namespace) -> None:
    if getattr(args, "workers", 1) < 1 or getattr(args, "workers", 1) > 32:
        raise ValueError("workers must be between 1 and 32")
    if getattr(args, "retries", 0) < 0 or getattr(args, "retries", 0) > 20:
        raise ValueError("retries must be between 0 and 20")
    if getattr(args, "batch_size", 1) < 1 or getattr(args, "batch_size", 1) > 4096:
        raise ValueError("batch-size must be between 1 and 4096")
    if getattr(args, "timeout", 1.0) <= 0:
        raise ValueError("timeout must be positive")
    if getattr(args, "proxy", None) and getattr(args, "use_env_proxy", False):
        raise ValueError("--proxy and --use-env-proxy are mutually exclusive")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        validate_cli_arguments(args)
        return int(args.handler(args))
    except KeyboardInterrupt:
        print("Interrupted safely. Rerun the same command to resume.", file=sys.stderr)
        return 130
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
