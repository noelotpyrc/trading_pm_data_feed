#!/usr/bin/env python3
"""Local utilities — runs on your machine to manage VPS remotely via SSH.

Requires SSH config with the VPS host alias (see ~/.ssh/config).
Reads VPS_SSH_HOST and VPS_PROJECT_DIR from .env file.

Usage:
  python -m utils.local <command> [args]

Commands:
  pull-artifact [--date YYYY-MM-DD] [--out-dir DIR]  Pull a day's artifact bundle
  pull-db [--out PATH]                                Pull the SQLite DB
  query-db <sql>                                      Run SQL on VPS DB
  tail-log <name> [--lines N]                         Tail a log file on VPS
  list-artifacts                                      List available artifact dates
  db-stats                                            Print remote DB coverage stats
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def _load_env() -> dict[str, str]:
    """Load .env file from project root."""
    env = {}
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def _get_config() -> tuple[str, str]:
    """Return (ssh_host, project_dir) from env."""
    env = _load_env()
    host = os.environ.get("VPS_SSH_HOST") or env.get("VPS_SSH_HOST", "vps-madrid")
    project_dir = os.environ.get("VPS_PROJECT_DIR") or env.get("VPS_PROJECT_DIR", "/root/trading_pm_data_feed")
    return host, project_dir


def _ssh(cmd: str) -> str:
    """Run a command on VPS via SSH, return stdout."""
    host, _ = _get_config()
    result = subprocess.run(
        ["ssh", host, cmd],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"[SSH ERROR] {result.stderr.strip()}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def _rsync(remote_path: str, local_path: str, is_dir: bool = False) -> None:
    """Rsync a file or directory from VPS to local."""
    host, _ = _get_config()
    src = f"{host}:{remote_path}{'/' if is_dir else ''}"
    args = ["rsync", "-az", "--progress", src, local_path]
    subprocess.run(args, check=True)


def pull_artifact(date: str | None = None, out_dir: str = "data/artifacts") -> None:
    """Pull a day's artifact bundle from VPS."""
    _, project_dir = _get_config()
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    remote = f"{project_dir}/data/artifacts/{date}"
    local = Path(out_dir) / date
    local.mkdir(parents=True, exist_ok=True)
    print(f"Pulling artifact {date} → {local}")
    _rsync(remote, str(local), is_dir=True)
    # Verify
    expected = ["model.json", "z_pool.npy", "metadata.json"]
    missing = [f for f in expected if not (local / f).exists()]
    if missing:
        print(f"[WARN] Missing files: {missing}")
    else:
        print(f"[OK] All artifact files present in {local}")


def pull_db(out_path: str = "data/btcusdt_perp_1m.sqlite") -> None:
    """Pull the SQLite DB from VPS."""
    _, project_dir = _get_config()
    remote = f"{project_dir}/data/btcusdt_perp_1m.sqlite"
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    print(f"Pulling DB → {out_path}")
    _rsync(remote, out_path)
    print("[OK] DB pulled")


def query_db(sql: str) -> None:
    """Run a SQL query on the VPS SQLite DB and print results."""
    _, project_dir = _get_config()
    # Escape single quotes in SQL for shell
    escaped = sql.replace("'", "'\\''")
    cmd = (
        f"cd {project_dir} && .venv/bin/python -c \""
        f"import sqlite3; con = sqlite3.connect('data/btcusdt_perp_1m.sqlite'); "
        f"rows = con.execute('{escaped}').fetchall(); "
        f"[print(r) for r in rows]; con.close()\""
    )
    output = _ssh(cmd)
    print(output, end="")


def fetch_warmup_bars(n: int = 1800) -> str:
    """Fetch last N deduped OHLCV rows from VPS DB as CSV string over SSH.

    Returns raw CSV text (with header). Caller parses with pd.read_csv().
    Pipes the Python script via stdin to avoid shell escaping issues.
    """
    host, project_dir = _get_config()
    db_file = f"{project_dir}/data/btcusdt_perp_1m.sqlite"
    script = f"""
import sqlite3, csv, sys
con = sqlite3.connect("{db_file}")
rows = con.execute(
    "SELECT timestamp, open, high, low, close, volume, num_trades "
    "FROM ohlcv_btcusdt_1m "
    "WHERE id IN ("
    "  SELECT id FROM ("
    "    SELECT id, ROW_NUMBER() OVER ("
    "      PARTITION BY timestamp ORDER BY ingested_at DESC, id DESC"
    "    ) AS rn FROM ohlcv_btcusdt_1m"
    "  ) WHERE rn = 1"
    ") "
    "ORDER BY timestamp DESC LIMIT {n}"
).fetchall()
w = csv.writer(sys.stdout)
w.writerow(["timestamp","open","high","low","close","volume","num_trades"])
for r in reversed(rows):
    w.writerow(r)
con.close()
"""
    result = subprocess.run(
        ["ssh", host, f"cd {project_dir} && .venv/bin/python"],
        input=script, capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(f"SSH fetch_warmup_bars failed: {result.stderr.strip()}")
    return result.stdout


def tail_log(name: str, lines: int = 50) -> None:
    """Tail a log file on VPS."""
    _, project_dir = _get_config()
    if not name.endswith(".log"):
        name += ".log"
    cmd = f"tail -n {lines} {project_dir}/data/{name}"
    output = _ssh(cmd)
    print(output, end="")


def list_artifacts() -> None:
    """List available artifact dates on VPS."""
    _, project_dir = _get_config()
    cmd = f"ls -1 {project_dir}/data/artifacts/ 2>/dev/null || echo '(none)'"
    output = _ssh(cmd)
    print(output, end="")


def db_stats() -> None:
    """Print remote DB coverage stats."""
    _, project_dir = _get_config()
    cmd = (
        f"cd {project_dir} && .venv/bin/python -c \""
        f"from cex_data_feed.pipeline_1m.sqlite_db import coverage_stats; "
        f"s = coverage_stats('data/btcusdt_perp_1m.sqlite'); "
        f"print(f'min={{s[0]}}  max={{s[1]}}  rows={{s[2]:,}}') if s else print('empty')\""
    )
    output = _ssh(cmd)
    print(output, end="")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Local VPS management via SSH")
    sub = parser.add_subparsers(dest="command")

    p_art = sub.add_parser("pull-artifact", help="Pull a day's artifact bundle")
    p_art.add_argument("--date", default=None, help="YYYY-MM-DD (default: today UTC)")
    p_art.add_argument("--out-dir", default="data/artifacts")

    p_db = sub.add_parser("pull-db", help="Pull the SQLite DB")
    p_db.add_argument("--out", default="data/btcusdt_perp_1m.sqlite")

    p_query = sub.add_parser("query-db", help="Run SQL on VPS DB")
    p_query.add_argument("sql", help="SQL query to run")

    p_tail = sub.add_parser("tail-log", help="Tail a log file on VPS")
    p_tail.add_argument("name", help="Log name (e.g. accumulate_1m)")
    p_tail.add_argument("--lines", type=int, default=50)

    sub.add_parser("list-artifacts", help="List available artifact dates")
    sub.add_parser("db-stats", help="Print remote DB coverage stats")

    args = parser.parse_args(argv)

    if args.command == "pull-artifact":
        pull_artifact(date=args.date, out_dir=args.out_dir)
    elif args.command == "pull-db":
        pull_db(out_path=args.out)
    elif args.command == "query-db":
        query_db(args.sql)
    elif args.command == "tail-log":
        tail_log(args.name, lines=args.lines)
    elif args.command == "list-artifacts":
        list_artifacts()
    elif args.command == "db-stats":
        db_stats()
    else:
        parser.print_help()
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
